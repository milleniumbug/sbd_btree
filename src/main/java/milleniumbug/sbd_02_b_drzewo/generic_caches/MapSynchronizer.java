package milleniumbug.sbd_02_b_drzewo.generic_caches;

import java.util.Map;

public class MapSynchronizer<K, V> implements CacheSynchronizer<K, V> {

    private final Map<K, V> map;

    public MapSynchronizer(Map<K, V> map) {
        this.map = map;
    }

    @Override
    public V load(K key) {
        return map.get(key);
    }

    @Override
    public void flush(K key, V value) {
        map.put(key, value);
    }

    @Override
    public void close() throws Exception {

    }

}
