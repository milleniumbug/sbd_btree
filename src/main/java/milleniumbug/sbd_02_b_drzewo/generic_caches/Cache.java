package milleniumbug.sbd_02_b_drzewo.generic_caches;

public interface Cache<K, V> extends AutoCloseable {
    public abstract V lookup(K key);
    public abstract V replace(K key, V value);
    public abstract void sync();
    
    
    @Override
    public default void close() throws Exception
    {
        sync();
    }
}
