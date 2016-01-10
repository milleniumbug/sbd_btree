package milleniumbug.sbd_02_isam.generic_caches;

public interface CacheSynchronizer<K,V> extends AutoCloseable {
    public V load(K key);
    public void flush(K key, V value);
}
