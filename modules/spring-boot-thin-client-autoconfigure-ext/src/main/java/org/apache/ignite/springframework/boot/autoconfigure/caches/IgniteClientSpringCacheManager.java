package org.apache.ignite.springframework.boot.autoconfigure.caches;

import org.apache.ignite.client.IgniteClient;
import org.jetbrains.annotations.NotNull;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;

import java.util.Collection;

/**
 * Spring CacheManager for IgniteClient
 */
public class IgniteClientSpringCacheManager implements CacheManager {

    private final IgniteClient igniteClient;

    public IgniteClientSpringCacheManager(IgniteClient igniteClient) {
        this.igniteClient = igniteClient;
    }

    @Override
    public Cache getCache(@NotNull String name) {
        return new IgniteClientStringCache(
                igniteClient.getOrCreateCache(name)
        );
    }

    @NotNull
    @Override
    public Collection<String> getCacheNames() {
        return igniteClient.cacheNames();
    }
}
