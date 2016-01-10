package milleniumbug.sbd_02_isam.generic_caches;

import java.util.LinkedHashMap;
import java.util.Map;
import lombok.Getter;
import lombok.NonNull;

public class LruCache<K, V> implements Cache<K, V> {
    final private Map<K, V> map;
    @Getter final private CacheSynchronizer<K,V> behaviour;

    @Override
    public V lookup(@NonNull K key) {
        return map.computeIfAbsent(key, behaviour::load);
    }

    public LruCache(int maxEntryCount, CacheSynchronizer<K,V> behaviour) {
        this.behaviour = behaviour;
        this.map = new LinkedHashMap<K, V>() {
            @Override
            protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
                if(size() > maxEntryCount)
                {
                    behaviour.flush(eldest.getKey(), eldest.getValue());
                    return true;
                }
                return false;
            }
        };
    }

    @Override
    public void sync()
    {
        map.forEach(behaviour::flush);
        map.clear();
    }

    @Override
    public V replace(K key, V value) {
        return map.put(key, value);
    }
    
    @Override
    public void close() throws Exception
    {
        sync();
        behaviour.close();
    }
}
