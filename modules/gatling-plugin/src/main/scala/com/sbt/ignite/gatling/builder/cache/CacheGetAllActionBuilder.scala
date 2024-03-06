/*
 * Copyright 2023 JSC SberTech
 */
package com.sbt.ignite.gatling.builder.cache

import scala.collection.SortedSet

import com.sbt.ignite.gatling.action.cache.CacheGetAllAction
import io.gatling.core.action.Action
import io.gatling.core.action.builder.ActionBuilder
import io.gatling.core.session.Expression
import io.gatling.core.structure.ScenarioContext

/**
 * Cache getAll action builder.
 *
 * @tparam K Type of the cache key.
 * @tparam V Type of the cache value.
 * @param cacheName Cache name.
 * @param keys The collection of the cache entry keys.
 */
class CacheGetAllActionBuilder[K, V](
    cacheName: Expression[String],
    keys: Expression[SortedSet[K]]
) extends ActionBuilder
    with CacheActionCommonParameters
    with CheckParameters[K, V] {

    /**
     * Builds an action.
     *
     * @param ctx The scenario context.
     * @param next The action that will be chained with the Action build by this builder.
     * @return The resulting action.
     */
    override def build(ctx: ScenarioContext, next: Action): Action =
        new CacheGetAllAction(requestName, cacheName, keys, withKeepBinary, withAsync, checks, next, ctx)
}
