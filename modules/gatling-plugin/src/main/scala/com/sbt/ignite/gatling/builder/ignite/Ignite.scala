/*
 * Copyright 2023 JSC SberTech
 */
package com.sbt.ignite.gatling.builder.ignite

import io.gatling.core.session.Expression

/**
 * DSL to create Ignite operations.
 */
trait Ignite {
    /**
     * Start constructing of the create cache action with the provided cache name.
     *
     * @tparam K Type of the cache key.
     * @tparam V Type of the cache value.
     * @param cacheName Cache name.
     * @return CreateCacheActionBuilderBase
     */
    def getOrCreateCache[K, V](cacheName: Expression[String]): GetOrCreateCacheActionBuilderBase[K, V] =
        GetOrCreateCacheActionBuilderBase(cacheName)

    /**
     * Start constructing of the start Ignite API action.
     *
     * @return StartClientActionBuilder
     */
    def startIgniteApi: StartClientActionBuilder = StartClientActionBuilder()

    /**
     * Start constructing of the close Ignite API action.
     *
     * @return CloseClientActionBuilder
     */
    def closeIgniteApi: CloseClientActionBuilder = CloseClientActionBuilder()
}
