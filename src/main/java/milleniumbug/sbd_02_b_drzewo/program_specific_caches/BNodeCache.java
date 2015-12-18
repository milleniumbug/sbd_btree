package milleniumbug.sbd_02_b_drzewo.program_specific_caches;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import milleniumbug.sbd_02_b_drzewo.generic_caches.Cache;
import milleniumbug.sbd_02_b_drzewo.generic_caches.CacheSynchronizer;
import milleniumbug.sbd_02_b_drzewo.generic_caches.LruCache;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import milleniumbug.sbd_02_b_drzewo.BNode;

public class BNodeCache implements Cache<Long, BNode> {

    private final CounterSynchronizer<Long, byte[]> counter;
    private final Cache<Long, BNode> cache;

    public static class BNodeSynchronizer extends SerializerSynchronizer<BNode> {

        public BNodeSynchronizer(CacheSynchronizer<Long, byte[]> synchronizer) {
            super(synchronizer);
        }

        @Override
        public BNode deserialize(long pos, byte[] buffer) {
            if(pos == 0)
                return null;
            try {
                DataInputStream reader = new DataInputStream(new ByteArrayInputStream(buffer));
                List<BNode.BNodeElement> entries = new ArrayList<>();
                for (int i = 0; i < 2 * BNode.d; ++i) {
                    long left_child = reader.readLong();
                    long key = reader.readLong();
                    long value_pos = reader.readLong();
                    if (left_child != 0) {
                        if(key != 0)
                            entries.add(new BNode.BNodeElement(key, value_pos, left_child));
                        else
                            entries.add(new BNode.BNodeElement(BNode.not_a_key, value_pos, left_child));
                    }
                }
                return new BNode(entries, pos);
            } catch (IOException ex) {
                Logger.getLogger(BNodeCache.class.getName()).log(Level.SEVERE, null, ex);
                throw new RuntimeException(ex);
            }
        }

        @Override
        public byte[] serialize(long pos, BNode node) {
            if(pos == 0)
                throw new IndexOutOfBoundsException("nie ma wpisu zerowego");
            try {
                assert pos == node.getThisAddress();
                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                DataOutputStream writer = new DataOutputStream(bos);
                int count = 0;
                for (BNode.BNodeElement key : node.getEntries()) {
                    writer.writeLong(key.getLeftChild());
                    if (key.getKey() != BNode.not_a_key) {
                        writer.writeLong(key.getKey());
                        writer.writeLong(key.getValuePosition());
                        ++count;
                    }
                }
                for (int i = count; i < 2 * BNode.d; ++i) {
                    for (int j = 0; j < 3; ++j) {
                        writer.writeLong(0);
                    }
                }
                writer.flush();
                return Arrays.copyOf(bos.toByteArray(), FilePageSynchronizer.page_size);
            } catch (IOException ex) {
                Logger.getLogger(BNodeCache.class.getName()).log(Level.SEVERE, null, ex);
                throw new RuntimeException(ex);
            }
        }
    }

    public BNodeCache(File f) {
        try {
            counter = new CounterSynchronizer<>(new FilePageSynchronizer(new RandomAccessFile(f, "rw")));
            cache = new LruCache<>(1024, new BNodeSynchronizer(counter));
        } catch (FileNotFoundException ex) {
            Logger.getLogger(BNodeCache.class.getName()).log(Level.SEVERE, null, ex);
            throw new RuntimeException(ex);
        }
    }

    public long writeCount() {
        return counter.writeCount();
    }

    public long readCount() {
        return counter.readCount();
    }

    @Override
    public BNode lookup(Long key) {
        return cache.lookup(key);
    }

    @Override
    public BNode replace(Long key, BNode value) {
        return cache.replace(key, value);
    }

    @Override
    public void sync() {
        cache.sync();
    }

}
