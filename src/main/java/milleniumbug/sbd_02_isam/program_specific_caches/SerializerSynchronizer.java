package milleniumbug.sbd_02_isam.program_specific_caches;

import milleniumbug.sbd_02_isam.generic_caches.CacheSynchronizer;

public abstract class SerializerSynchronizer<V> implements CacheSynchronizer<Long,V> {

    private final CacheSynchronizer<Long, byte[]> synchronizer;

    public SerializerSynchronizer(CacheSynchronizer<Long, byte[]> synchronizer) {
        this.synchronizer = synchronizer;
    }

    public abstract V deserialize(long pos, byte[] buffer);
    public abstract byte[] serialize(long pos, V node);

    @Override
    public V load(Long key) {
        return deserialize(key, synchronizer.load(key));
    }

    @Override
    public void flush(Long key, V value) {
        synchronizer.flush(key, serialize(key, value));
    }
    
    @Override
    public void close() throws Exception
    {
        synchronizer.close();
    }
}
