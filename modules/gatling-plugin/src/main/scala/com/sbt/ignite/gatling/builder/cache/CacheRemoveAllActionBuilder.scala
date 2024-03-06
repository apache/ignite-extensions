/*
 * Copyright 2023 JSC SberTech
 */
package com.sbt.ignite.gatling.builder.cache

import scala.collection.SortedSet

import com.sbt.ignite.gatling.action.cache.CacheRemoveAllAction
import io.gatling.core.action.Action
import io.gatling.core.action.builder.ActionBuilder
import io.gatling.core.session.Expression
import io.gatling.core.structure.ScenarioContext

/**
 * Cache entry remove action builder.
 *
 * @tparam K Type of the cache key.
 * @param cacheName Cache name.
 * @param keys Collection of cache entry keys.
 */
class CacheRemoveAllActionBuilder[K](
    cacheName: Expression[String],
    keys: Expression[SortedSet[K]]
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
        new CacheRemoveAllAction(requestName, cacheName, keys, withKeepBinary, withAsync, next, ctx)
}
