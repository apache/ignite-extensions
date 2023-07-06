package org.apache.ignite.spring.sessions.proxy;

import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.expiry.ExpiryPolicy;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.Query;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.spring.sessions.IgniteIndexedSessionRepository.IgniteSession;

/**
 * Represents {@link SessionProxy} implementation that uses {@link IgniteCache} to perform session operations.
 */
public class IgniteSessionProxy implements SessionProxy {
    /** Ignite cache instance. */
    private final IgniteCache<String, IgniteSession> cache;

    /**
     * @param cache Ignite cache instance.
     */
    public IgniteSessionProxy(IgniteCache<String, IgniteSession> cache) {
        this.cache = cache;
    }

    /** {@inheritDoc} */
    @Override public void registerCacheEntryListener(
        CacheEntryListenerConfiguration<String, IgniteSession> cacheEntryListenerConfiguration
    ) {
        cache.registerCacheEntryListener(cacheEntryListenerConfiguration);
    }

    /** {@inheritDoc} */
    @Override public void deregisterCacheEntryListener(
        CacheEntryListenerConfiguration<String, IgniteSession> cacheEntryListenerConfiguration
    ) {
        cache.deregisterCacheEntryListener(cacheEntryListenerConfiguration);
    }

    /** {@inheritDoc} */
    @Override public SessionProxy withExpiryPolicy(ExpiryPolicy expirePlc) {
        return new IgniteSessionProxy(cache.withExpiryPolicy(expirePlc));
    }

    /** {@inheritDoc} */
    @Override public IgniteSession get(String key) {
        return cache.get(key);
    }

    /** {@inheritDoc} */
    @Override public void put(String key, IgniteSession val) {
        cache.put(key, val);
    }

    /** {@inheritDoc} */
    @Override public boolean remove(String key) {
        return cache.remove(key);
    }

    /** {@inheritDoc} */
    @Override public boolean replace(String key, IgniteSession val) {
        return cache.replace(key, val);
    }

    /** {@inheritDoc} */
    @Override public <R> QueryCursor<R> query(Query<R> qry) {
        return cache.query(qry);
    }
}
