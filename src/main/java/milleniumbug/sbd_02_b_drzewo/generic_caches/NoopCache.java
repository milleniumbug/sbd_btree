package milleniumbug.sbd_02_b_drzewo.generic_caches;

public class NoopCache<K,V> implements Cache<K,V> {
    CacheSynchronizer<K,V> behaviour;

    public NoopCache(CacheSynchronizer<K, V> behaviour) {
        this.behaviour = behaviour;
    }
    
    @Override
    public V lookup(K key) {
        return behaviour.load(key);
    }

    @Override
    public V replace(K key, V value) {
        behaviour.flush(key, value);
        return value;
    }

    @Override
    public void sync() {
        
    }

    @Override
    public void close() throws Exception {
        behaviour.close();
    }

}
