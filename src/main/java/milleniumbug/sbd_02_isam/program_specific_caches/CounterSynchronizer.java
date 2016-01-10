package milleniumbug.sbd_02_isam.program_specific_caches;

import milleniumbug.sbd_02_isam.generic_caches.CacheSynchronizer;
import lombok.NonNull;

public class CounterSynchronizer<K, V> implements CacheSynchronizer<K, V> {
    
    private final CacheSynchronizer<K, V> synchronizer;
    private long writeCount_ = 0;
    private long readCount_ = 0;

    public CounterSynchronizer(CacheSynchronizer<K, V> synchronizer) {
        this.synchronizer = synchronizer;
    }

    public long writeCount() {
        return writeCount_;
    }

    public long readCount() {
        return readCount_;
    }

    @Override
    public V load(@NonNull K key) {
        ++readCount_;
        return synchronizer.load(key);
    }

    @Override
    public void flush(@NonNull K key, @NonNull V value) {
        ++writeCount_;
        synchronizer.flush(key, value);
    }

    @Override
    public void close() throws Exception {
        synchronizer.close();
    }
}
