/*
 * Copyright 2023 JSC SberTech
 */
package com.sbt.ignite.gatling.builder.cache

import scala.collection.SortedMap

import com.sbt.ignite.gatling.action.cache.CachePutAllAction
import io.gatling.core.action.Action
import io.gatling.core.action.builder.ActionBuilder
import io.gatling.core.session.Expression
import io.gatling.core.structure.ScenarioContext

/**
 * Cache putAll action builder.
 *
 * @tparam K Type of the cache key.
 * @tparam V Type of the cache value.
 * @param cacheName Cache name.
 * @param map Collection of cache entry keys and values.
 */
class CachePutAllActionBuilder[K, V](
    cacheName: Expression[String],
    map: Expression[SortedMap[K, V]]
) extends ActionBuilder
    with CacheActionCommonParameters {

    /**
     * Builds an action.
     *
     * @param ctx The scenario context.
     * @param next The action that will be chained with the Action build by this builder.
     * @return The resulting action.
     */
    override def build(ctx: ScenarioContext, next: Action): Action =
        new CachePutAllAction(requestName, cacheName, map, withKeepBinary, withAsync, next, ctx)
}
