package org.apache.ignite.springframework.boot.autoconfigure.caches;

import org.apache.ignite.client.ClientCache;
import org.jetbrains.annotations.NotNull;
import org.springframework.cache.Cache;
import org.springframework.cache.support.SimpleValueWrapper;

import java.util.concurrent.Callable;

/**
 * Spring Cache implementation for thin Ignite ClientCache
 */
public class IgniteClientStringCache implements Cache {

    private final ClientCache<Object, Object> delegate;

    public IgniteClientStringCache(@NotNull ClientCache<Object, Object> delegate) {
        this.delegate = delegate;
    }

    @NotNull
    @Override
    public String getName() {
        return delegate.getName();
    }

    @NotNull
    @Override
    public Object getNativeCache() {
        return delegate;
    }

    @Override
    public ValueWrapper get(@NotNull Object key) {
        return new SimpleValueWrapper(delegate.get(key));
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T get(@NotNull Object key, Class<T> type) {
        Object value = delegate.get(key);
        if (type.isInstance(value)) {
            return (T) value;
        } else {
            throw new IllegalStateException();
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T get(@NotNull Object key, @NotNull Callable<T> valueLoader) {
        Object value = delegate.get(key);
        if (value == null) {
            try {
                value = valueLoader.call();
            } catch (Exception e) {
                throw new ValueRetrievalException(key, valueLoader, e);
            }
            delegate.put(key, value);
        }
        return (T) value;
    }

    @Override
    public void put(@NotNull Object key, Object value) {
        delegate.put(key, value);
    }

    @Override
    public ValueWrapper putIfAbsent(@NotNull Object key, Object value) {
        return new SimpleValueWrapper(delegate.putIfAbsent(key, value));
    }

    @Override
    public void evict(@NotNull Object key) {
        delegate.remove(key);
    }

    @Override
    public void clear() {
        delegate.clear();
    }
}
