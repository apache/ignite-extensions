/*
 * Copyright 2023 JSC SberTech
 */
package com.sbt.ignite.gatling.action.cache

import com.sbt.ignite.gatling.Predef.IgniteCheck
import com.sbt.ignite.gatling.action.CacheAction
import io.gatling.core.action.Action
import io.gatling.core.session.Expression
import io.gatling.core.session.Session
import io.gatling.core.structure.ScenarioContext
import org.apache.ignite.cache.CacheEntryProcessor

/**
 * Action for the invoke Ignite operation.
 *
 * @tparam K Type of the cache key.
 * @tparam V Type of the cache value.
 * @tparam T Type of the operation result.
 * @param requestName Name of the request.
 * @param cacheName Name of cache.
 * @param key Cache entry key.
 * @param entryProcessor Instance of CacheEntryProcessor.
 * @param arguments Additional arguments to pass to the entry processor.
 * @param keepBinary True if it should operate with binary objects.
 * @param async True if async API should be used.
 * @param checks Collection of checks to perform against the operation result.
 * @param next Next action from chain to invoke upon this one completion.
 * @param ctx Scenario context.
 */
class CacheInvokeAction[K, V, T](
    requestName: Expression[String],
    cacheName: Expression[String],
    key: Expression[K],
    entryProcessor: CacheEntryProcessor[K, V, T],
    arguments: Seq[Expression[Any]],
    keepBinary: Boolean,
    async: Boolean,
    checks: Seq[IgniteCheck[K, T]],
    next: Action,
    ctx: ScenarioContext
) extends CacheAction[K, V]("invoke", requestName, ctx, next, cacheName, keepBinary, async)
    with ArgumentsResolveSupport {

    override protected def execute(session: Session): Unit = withSessionCheck(session) {
        for {
            CacheActionParameters(resolvedRequestName, cacheApi, transactionApi) <- resolveCacheParameters(session)
            resolvedKey <- key(session)
            resolvedArguments <- resolveArguments(session, arguments)
        } yield {
            logger.debug(s"session user id: #${session.userId}, before $resolvedRequestName")

            val func = if (async) cacheApi.invokeAsync[T] _ else cacheApi.invoke[T] _

            call(func(resolvedKey, entryProcessor, resolvedArguments), resolvedRequestName, session, checks)
        }
    }
}
