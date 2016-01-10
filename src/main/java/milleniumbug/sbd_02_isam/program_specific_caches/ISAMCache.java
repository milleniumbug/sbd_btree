package milleniumbug.sbd_02_isam.program_specific_caches;

import milleniumbug.sbd_02_isam.generic_caches.Cache;
import milleniumbug.sbd_02_isam.generic_caches.LruCache;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import milleniumbug.sbd_02_isam.generic_caches.CacheSynchronizer;

class ISAMCache<T, Synchronizer> implements Cache<Long, T> {

    private final CounterSynchronizer<Long, byte[]> counter;
    private final Cache<Long, T> cache;

    public ISAMCache(File f, Function<CacheSynchronizer<Long, byte[]>, CacheSynchronizer<Long, T>> synchronizer_provider, int size) {
        try {
            counter = new CounterSynchronizer<>(new FilePageSynchronizer(new RandomAccessFile(f, "rw")));
            cache = new LruCache<>(size, synchronizer_provider.apply(counter));
        } catch (FileNotFoundException ex) {
            Logger.getLogger(ISAMCache.class.getName()).log(Level.SEVERE, null, ex);
            throw new RuntimeException(ex);
        }
    }

    public ISAMCache(File f, Function<CacheSynchronizer<Long, byte[]>, CacheSynchronizer<Long, T>> synchronizer_provider) {
        this(f, synchronizer_provider, 1024);
    }

    public long writeCount() {
        return counter.writeCount();
    }

    public long readCount() {
        return counter.readCount();
    }

    @Override
    public T lookup(Long key) {
        return cache.lookup(key);
    }

    @Override
    public T replace(Long key, T value) {
        return cache.replace(key, value);
    }

    @Override
    public void sync() {
        cache.sync();
    }

}
