package org.apache.ignite.spring.sessions.proxy;

import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.expiry.ExpiryPolicy;
import org.apache.ignite.cache.query.Query;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.spring.sessions.IgniteIndexedSessionRepository.IgniteSession;

/**
 * Represents {@link SessionProxy} implementation that uses {@link ClientCache} to perform session operations.
 */
public class ClientSessionProxy implements SessionProxy {
    /** Client cache instance. */
    private final ClientCache<String, IgniteSession> cache;

    /**
     * @param cache Client cache instance.
     */
    public ClientSessionProxy(ClientCache<String, IgniteSession> cache) {
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
        return new ClientSessionProxy(cache.withExpirePolicy(expirePlc));
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
