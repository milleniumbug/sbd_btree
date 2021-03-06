package milleniumbug.sbd_02_isam;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.RandomAccess;
import java.util.logging.Level;
import java.util.logging.Logger;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import milleniumbug.sbd_02_isam.generic_caches.Cache;
import milleniumbug.sbd_02_isam.generic_caches.MapSynchronizer;
import milleniumbug.sbd_02_isam.generic_caches.NoopCache;
import milleniumbug.sbd_02_isam.program_specific_caches.FilePageSynchronizer;
import milleniumbug.sbd_02_isam.program_specific_caches.IndexCache;
import milleniumbug.sbd_02_isam.program_specific_caches.SeqFileCache;

public class ISAM implements AutoCloseable {

    final static private long sentinel_min = Long.MIN_VALUE;
    final static private long sentinel_max = Long.MAX_VALUE;

    @EqualsAndHashCode(exclude = {"thisPage", "deleted", "nextPointer", "value"})
    @ToString(exclude = {"thisPage"})
    public static class SeqFileRecord implements Comparable<SeqFileRecord> {

        final static public int recordsPerPage = 86;
        final public long thisPage;
        public boolean deleted = false;
        final public long key;
        public String value;
        public long nextPointer = sentinel_max;

        public SeqFileRecord(long this_page, long key, String value, long nextPointer) {
            this.thisPage = this_page;
            this.key = key;
            this.value = value;
            this.nextPointer = nextPointer;
        }

        @Override
        public int compareTo(SeqFileRecord other) {
            return Long.compare(this.key, other.key);
        }
    }

    @EqualsAndHashCode(exclude = {"thisPage", "page"})
    @ToString(exclude = {"thisPage"})
    public static class IndexRecord implements Comparable<IndexRecord> {

        final static public int recordsPerPage = 256;
        final public long thisPage;
        final public long key;
        public long page;

        public IndexRecord(long this_page, long key, long page) {
            this.thisPage = this_page;
            this.page = page;
            this.key = key;
        }

        @Override
        public int compareTo(IndexRecord other) {
            return Long.compare(this.key, other.key);
        }
    }

    private class ISAMRawIterator implements Iterator<SeqFileRecord> {

        private SeqFileRecord currkey;

        @Override
        public boolean hasNext() {
            return currkey != null;
        }

        private void getKey(List<SeqFileRecord> lookup) {
            assert lookup instanceof RandomAccess;
            lookup.sort(Comparator.naturalOrder());
            for (int i = 0; i < lookup.size(); ++i) {
                SeqFileRecord x = lookup.get(i);
                if (x.key > currkey.key) {
                    currkey = x;
                    return;
                }
            }
            assert false;
        }

        @Override
        public SeqFileRecord next() {
            if (!isSentinel(currkey.nextPointer)) {
                List<SeqFileRecord> page = data.lookup(currkey.nextPointer);
                SeqFileRecord prevkey = currkey;
                getKey(page);
                return prevkey;
            } else {
                SeqFileRecord prevkey = currkey;
                currkey = null;
                return prevkey;
            }
        }

        ISAMRawIterator(long key) {
            IndexRecord ir = binarySearchIndex(key);
            List<SeqFileRecord> lookup = data.lookup(ir.page);
            this.currkey = lookup.stream()
                    .sorted(Comparator.reverseOrder())
                    .filter(x -> x.key <= key)
                    .findFirst()
                    .get();
        }

        ISAMRawIterator() {
            this(sentinel_min);
        }
    }

    public class ISAMIterator implements Iterator<Map.Entry<Long, String>> {

        final ISAMRawIterator underlying;

        @Override
        public boolean hasNext() {
            return underlying.hasNext();
        }

        @Override
        public Map.Entry<Long, String> next() {
            SeqFileRecord n = underlying.next();
            return new Map.Entry<Long, String>() {

                Long key = n.key;
                String value = n.value;

                @Override
                public Long getKey() {
                    return key;
                }

                @Override
                public String getValue() {
                    return value;
                }

                @Override
                public String setValue(String v) {
                    value = v;
                    return v;
                }

            };
        }

        ISAMIterator(ISAMRawIterator iterator) {
            underlying = iterator;
        }
    }

    private long write_count = 0;
    private long read_count = 0;

    private long index_end_pointer;
    private long overflow_area_start_pointer;
    private long overflow_area_end_pointer;

    private File metadata_file;
    private Cache<Long, List<IndexRecord>> index;
    private Cache<Long, List<SeqFileRecord>> data;

    private boolean isSentinel(long key) {
        return key == sentinel_max || key == sentinel_min;
    }

    private boolean needToReorganize() {
        long primary_area_size = overflow_area_start_pointer;
        long overflow_area_size = overflow_area_end_pointer - overflow_area_start_pointer;
        return overflow_area_size > Main.log2nlz(primary_area_size);
    }

    // range is [page_start, page_end]
    private IndexRecord binarySearchIndex(long key, long page_start, long page_end) {
        for (int c = 0;; c++) {
            final long page_middle = (page_start + page_end) / 2;
            List<IndexRecord> page = index.lookup(page_middle);
            final int binarySearchResult = Collections.binarySearch(page, new IndexRecord(0L, key, 0L));
            final boolean exists = binarySearchResult >= 0;
            final int insertion_point = exists ? binarySearchResult : -binarySearchResult - 1;
            if (exists) {
                return page.get(binarySearchResult);
            } else if (insertion_point == 0) {
                if (page_middle - 1 >= 0) {
                    List<IndexRecord> prevpage = index.lookup(page_middle - 1);
                    IndexRecord prevrecord = prevpage.get(prevpage.size() - 1);
                    if (prevrecord.key < key) {
                        return prevrecord;
                    }
                }
                page_end = page_middle - 1;
                continue;
            } else if (insertion_point == page.size()) {
                if (page_start == page_end) {
                    return page.get(insertion_point - 1);
                }
                page_start = page_middle + 1;
                continue;
            } else if (insertion_point > 0) {
                return page.get(insertion_point - 1);
            }
            throw new AssertionError();
        }
    }

    private IndexRecord binarySearchIndex(long key) {
        return binarySearchIndex(key, 0, index_end_pointer - 1);
    }

    private long addToOverflowArea(SeqFileRecord rec) {
        long where_to_put = overflow_area_end_pointer - 1;
        List<SeqFileRecord> page = data.lookup(where_to_put);
        if (page.size() < SeqFileRecord.recordsPerPage) {
            rec = new SeqFileRecord(where_to_put, rec.key, rec.value, rec.nextPointer);
            page.add(rec);
            data.replace(where_to_put, page);
        } else {
            where_to_put = overflow_area_end_pointer;
            rec = new SeqFileRecord(where_to_put, rec.key, rec.value, rec.nextPointer);
            page = new ArrayList<>(Collections.singletonList(rec));
            data.replace(where_to_put, page);
            overflow_area_end_pointer++;
        }
        return where_to_put;
    }

    private void replace(SeqFileRecord rec) {
        List<SeqFileRecord> lookup = data.lookup(rec.thisPage);
        int index = lookup.indexOf(rec);
        assert index != -1;
        lookup.set(index, rec);
        Collections.sort(lookup);
        data.replace(rec.thisPage, lookup);
    }

    private File getDataFile(File metadata_file) {
        return new File(metadata_file.getParent(), metadata_file.getName() + "_data.sbd2");
    }

    private File getIndexFile(File metadata_file) {
        return new File(metadata_file.getParent(), metadata_file.getName() + "_index.sbd2");
    }

    public Optional<String> find(long key) {
        if (isSentinel(key)) {
            return Optional.empty();
        }
        for (Iterator<SeqFileRecord> it = internalIterator(key); it.hasNext();) {
            SeqFileRecord r = it.next();
            if (r.key > key) {
                break;
            }
            if (r.key == key && !r.deleted) {
                return Optional.of(r.value);
            }
        }
        return Optional.empty();
    }

    private static String truncate(String s) {
        return s.substring(0, s.length() < 30 ? s.length() : 30);
    }

    public String insert(long key, String value) {
        if (isSentinel(key)) {
            throw new IllegalArgumentException("trying to add sentinel value");
        }
        SeqFileRecord before = null;
        SeqFileRecord after = null;
        for (Iterator<SeqFileRecord> it = internalIterator(key); it.hasNext();) {
            after = it.next();

            if (after.key > key) {
                break;
            }
            if (after.key == key) {
                // można robić modyfikację w miejscu
                after.value = truncate(value);
                after.deleted = false;
                replace(after);
                return value;
            }
            before = after;
        }
        assert before != null && after != null;
        SeqFileRecord new_record = new SeqFileRecord(0, key, truncate(value), before.nextPointer);
        long target_page = addToOverflowArea(new_record);
        before = new SeqFileRecord(before.thisPage, before.key, before.value, target_page);
        replace(before);

        if (needToReorganize()) {
            reorganize();
        }
        return value;
    }

    public void erase(long key) {
        if (isSentinel(key)) {
            throw new IllegalArgumentException("trying to remove sentinel value");
        }
        for (Iterator<SeqFileRecord> it = internalIterator(key); it.hasNext();) {
            SeqFileRecord r = it.next();
            if (r.key > key) {
                break;
            }
            if (r.key == key && !r.deleted) {
                r.deleted = true;
                replace(r);
            }
        }
    }

    private void reorganize(Cache<Long, List<IndexRecord>> newindex, Cache<Long, List<SeqFileRecord>> newdata) {
        List<SeqFileRecord> datapage = new ArrayList<>();
        List<IndexRecord> indexpage = new ArrayList<>();
        // mutable long
        final long[] nextdatapage = new long[]{0};
        abstract class Flush {

            public long ptr = 0;

            abstract void run();
        }
        Flush flush_index = new Flush() {
            @Override
            void run() {
                if (!indexpage.isEmpty()) {
                    newindex.replace(ptr, new ArrayList<>(indexpage));
                    indexpage.clear();
                    ptr++;
                }
            }
        };
        Flush flush_data = new Flush() {
            @Override
            void run() {
                if (!datapage.isEmpty()) {
                    // fix the last entry
                    SeqFileRecord lastentry = datapage.get(datapage.size() - 1);
                    lastentry = new SeqFileRecord(
                            lastentry.thisPage,
                            lastentry.key,
                            lastentry.value,
                            (lastentry.key != sentinel_max) ? nextdatapage[0] + 1 : sentinel_max);
                    datapage.set(datapage.size() - 1, lastentry);
                    // rest
                    newdata.replace(ptr, new ArrayList<>(datapage));

                    indexpage.add(new IndexRecord(flush_index.ptr, datapage.get(0).key, ptr));
                    assert indexpage.size() <= IndexRecord.recordsPerPage;
                    if (indexpage.size() == IndexRecord.recordsPerPage) {
                        flush_index.run();
                    }
                    datapage.clear();
                    ptr++;
                }
            }
        };
        for (Iterator<SeqFileRecord> it = internalIterator(sentinel_min);
                it.hasNext();) {
            SeqFileRecord r = it.next();
            if (r.deleted) {
                continue;
            }

            datapage.add(new SeqFileRecord(flush_data.ptr, r.key, r.value, flush_data.ptr));
            if (datapage.size() == SeqFileRecord.recordsPerPage) {
                flush_data.run();
                nextdatapage[0] = flush_data.ptr;
            }
        }
        flush_data.run();
        flush_index.run();
        newdata.sync();
        newindex.sync();

        write_count += writeCountRaw();
        read_count += readCountRaw();

        index = newindex;
        data = newdata;
        index_end_pointer = flush_index.ptr;
        overflow_area_start_pointer = flush_data.ptr;
        overflow_area_end_pointer = flush_data.ptr + 1;
    }

    public void reorganize() {
        try {
            //Cache<Long, List<IndexRecord>> newindex = new NoopCache<>(new MapSynchronizer<>(new HashMap<Long, List<IndexRecord>>()));
            //Cache<Long, List<SeqFileRecord>> newdata = new NoopCache<>(new MapSynchronizer<>(new HashMap<Long, List<SeqFileRecord>>()));
            File f = new File("a");
            getIndexFile(f).createNewFile();
            getDataFile(f).createNewFile();
            Cache<Long, List<IndexRecord>> newindex = new IndexCache(getIndexFile(f));
            Cache<Long, List<SeqFileRecord>> newdata = new SeqFileCache(getDataFile(f));
            reorganize(newindex, newdata);
            getDataFile(metadata_file).delete();
            getIndexFile(metadata_file).delete();
            getDataFile(f).renameTo(getDataFile(metadata_file));
            getIndexFile(f).renameTo(getIndexFile(metadata_file));
        } catch (IOException ex) {
            Logger.getLogger(ISAM.class.getName()).log(Level.SEVERE, null, ex);
            throw new RuntimeException(ex);
        }
    }

    public static ISAM create(File metadata_file) throws Exception {
        try (ISAM isam = new ISAM()) {
            isam.metadata_file = metadata_file;
            isam.index_end_pointer = 1;
            isam.overflow_area_start_pointer = 1;
            isam.overflow_area_end_pointer = 2;
            isam.index = new NoopCache<>(new MapSynchronizer<>(new HashMap<Long, List<IndexRecord>>() {
                {
                    put(0L, new ArrayList<>(Arrays.asList(
                            new IndexRecord(0, sentinel_min, 0)
                    )));
                }
            }));
            isam.data = new NoopCache<>(new MapSynchronizer<>(new HashMap<Long, List<SeqFileRecord>>() {
                {
                    put(0L, new ArrayList<>(Arrays.asList(
                            new SeqFileRecord(0, sentinel_min, "", 0),
                            new SeqFileRecord(0, sentinel_max, "", sentinel_max)
                    )));
                    put(1L, new ArrayList<>(Arrays.asList()));
                }
            }));
            isam.reorganize();
        }
        return new ISAM(metadata_file);
    }

    private ISAM() {
        this.metadata_file = null;
    }

    public ISAM(File metadata_file) {
        this.metadata_file = Objects.requireNonNull(metadata_file);
        index = new IndexCache(getIndexFile(metadata_file), 32);
        data = new SeqFileCache(getDataFile(metadata_file), 256);
        index_end_pointer = getIndexFile(metadata_file).length() / FilePageSynchronizer.page_size;
        overflow_area_end_pointer = getDataFile(metadata_file).length() / FilePageSynchronizer.page_size;
        final long index_entries_count = (index_end_pointer - 1) * IndexRecord.recordsPerPage
                + index.lookup(index_end_pointer - 1).size();
        overflow_area_start_pointer = index_entries_count;
    }

    public long writeCount() {
        return write_count + writeCountRaw();
    }

    private long writeCountRaw() {
        try {
            IndexCache i = (IndexCache) index;
            SeqFileCache d = (SeqFileCache) data;
            return i.writeCount() + d.writeCount();
        } catch (ClassCastException ex) {
            return 0;
        }
    }

    public long readCount() {
        return read_count + readCountRaw();
    }

    public long readCountRaw() {
        try {
            IndexCache i = (IndexCache) index;
            SeqFileCache d = (SeqFileCache) data;
            return i.readCount() + d.readCount();
        } catch (ClassCastException ex) {
            return 0;
        }
    }

    private ISAMRawIterator internalIterator(long key) {
        return new ISAMRawIterator(key);
    }

    @Override
    public void close() throws Exception {
        data.close();
        index.close();
    }

    public static ISAM testData1() {
        ISAM isam = new ISAM();
        isam.metadata_file = new File("asdf");
        isam.index_end_pointer = 1;
        isam.overflow_area_start_pointer = 2;
        isam.overflow_area_end_pointer = 3;
        isam.index = new NoopCache<>(new MapSynchronizer<>(new HashMap<Long, List<IndexRecord>>() {
            {
                put(0L, new ArrayList<>(Arrays.asList(
                        new IndexRecord(0, sentinel_min, 0),
                        new IndexRecord(0, 10, 1)
                )));
            }
        }));
        isam.data = new NoopCache<>(new MapSynchronizer<>(new HashMap<Long, List<SeqFileRecord>>() {
            {
                put(0L, new ArrayList<>(Arrays.asList(
                        new SeqFileRecord(0, sentinel_min, "a", 0),
                        new SeqFileRecord(0, 1, "b", 0),
                        new SeqFileRecord(0, 5, "c", 2),
                        new SeqFileRecord(0, 8, "e", 1)
                )));
                put(1L, new ArrayList<>(Arrays.asList(
                        new SeqFileRecord(1, 10, "f", 1),
                        new SeqFileRecord(1, 11, "g", 2),
                        new SeqFileRecord(1, 21, "j", 1),
                        new SeqFileRecord(1, sentinel_max, "k", sentinel_max)
                )));
                put(2L, new ArrayList<>(Arrays.asList(
                        new SeqFileRecord(2, 7, "d", 0),
                        new SeqFileRecord(2, 12, "h", 2),
                        new SeqFileRecord(2, 13, "i", 1)
                )));
            }
        }));
        return isam;
    }

    public static void main(String[] args) {
        ISAM isam = testData1();
        for (Iterator<SeqFileRecord> it = isam.internalIterator(sentinel_min); it.hasNext();) {
            SeqFileRecord rec = it.next();
            System.out.println(rec);
        }
        for (Iterator<SeqFileRecord> it = isam.internalIterator(10); it.hasNext();) {
            SeqFileRecord rec = it.next();
            System.out.println(rec);
        }
        for (Iterator<SeqFileRecord> it = isam.internalIterator(sentinel_max); it.hasNext();) {
            SeqFileRecord rec = it.next();
            System.out.println(rec);
        }
    }

    public void printOutRawISAM() {
        for (long i = 0; i < index_end_pointer; ++i) {
            System.out.println("Indeks strona " + i);
            index.lookup(i).stream().forEachOrdered(x -> System.out.println(x));
        }
        for (long i = 0; i < overflow_area_end_pointer; ++i) {
            System.out.println("Dane strona " + i + " " + (i >= overflow_area_start_pointer ? "OVERFLOW AREA" : ""));
            data.lookup(i).stream().forEachOrdered(x -> System.out.println(x));
        }
    }

    public ISAMIterator iterator() {
        ISAMRawIterator it = internalIterator(sentinel_min);
        it.next();
        return new ISAMIterator(it);
    }
}
