/*
 * Copyright 2023 JSC SberTech
 */
package com.sbt.ignite.gatling.action.cache

import scala.collection.SortedMap

import com.sbt.ignite.gatling.action.CacheAction
import io.gatling.core.action.Action
import io.gatling.core.session.Expression
import io.gatling.core.session.Session
import io.gatling.core.structure.ScenarioContext

/**
 * Action for the putAll Ignite operation.
 *
 * @tparam K Type of the cache key.
 * @tparam V Type of the cache value.
 * @param requestName Name of the request.
 * @param cacheName Name of cache.
 * @param map Collection of cache entry keys and values.
 * @param keepBinary True if it should operate with binary objects.
 * @param async True if async API should be used.
 * @param next Next action from chain to invoke upon this one completion.
 * @param ctx Scenario context.
 */
class CachePutAllAction[K, V](
    requestName: Expression[String],
    cacheName: Expression[String],
    map: Expression[SortedMap[K, V]],
    keepBinary: Boolean,
    async: Boolean,
    next: Action,
    ctx: ScenarioContext
) extends CacheAction[K, V]("putAll", requestName, ctx, next, cacheName, keepBinary, async) {

    override protected def execute(session: Session): Unit = withSessionCheck(session) {
        for {
            resolvedMap <- map(session)
            CacheActionParameters(resolvedRequestName, cacheApi, transactionApi) <- resolveCacheParameters(session)
        } yield {
            logger.debug(s"session user id: #${session.userId}, before $resolvedRequestName")

            val func = if (async) cacheApi.putAllAsync _ else cacheApi.putAll _

            call(func(resolvedMap), resolvedRequestName, session)
        }
    }
}
