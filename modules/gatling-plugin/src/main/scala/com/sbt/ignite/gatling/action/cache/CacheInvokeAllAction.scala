/*
 * Copyright 2023 JSC SberTech
 */
package com.sbt.ignite.gatling.action.cache

import javax.cache.processor.EntryProcessorResult

import scala.collection.SortedMap
import scala.collection.SortedSet

import com.sbt.ignite.gatling.Predef.IgniteCheck
import com.sbt.ignite.gatling.action.CacheAction
import com.sbt.ignite.gatling.api.CacheApi
import io.gatling.core.action.Action
import io.gatling.core.session.Expression
import io.gatling.core.session.Session
import io.gatling.core.structure.ScenarioContext
import org.apache.ignite.cache.CacheEntryProcessor

/**
 * Common functions for invokeAll actions of two flavours.
 *
 * @tparam K Type of the cache key.
 * @tparam V Type of the cache value.
 * @tparam T Type of the operation result.
 */
trait CacheInvokeAllAction[K, V, T] extends ArgumentsResolveSupport {
    this: CacheAction[K, V] =>

    /** Additional arguments to pass to the entry processor. */
    val arguments: Seq[Expression[Any]]

    /** Collection of checks to perform against the operation result. */
    val checks: Seq[IgniteCheck[K, EntryProcessorResult[T]]]

    /**
     * Method executed when the Action received a Session message.
     * @param session Session
     * @param resolvedRequestName Name of request.
     * @param cacheApi Instance of CacheApi.
     * @param resolvedMap Resolved map from cache entry key to entry processor instance.
     * @param resolvedArguments Resolved entry processor arguments.
     */
    def execute(
        session: Session,
        resolvedRequestName: String,
        cacheApi: CacheApi[K, V],
        resolvedMap: SortedMap[K, CacheEntryProcessor[K, V, T]],
        resolvedArguments: List[Any]
    ): Unit = {

        logger.debug(s"session user id: #${session.userId}, before $resolvedRequestName")

        val func = if (async) cacheApi.invokeAllAsync[T] _ else cacheApi.invokeAll[T] _

        call(func(resolvedMap, resolvedArguments), resolvedRequestName, session, checks)
    }
}

/**
 * Action for the invokeAll Ignite operation in case its own cache processor instance
 * was provided for each cache entry.
 *
 * @tparam K Type of the cache key.
 * @tparam V Type of the cache value.
 * @tparam T Type of the operation result.
 * @param requestName Name of the request.
 * @param cacheName Name of cache.
 * @param map Map from cache entry key to CacheEntryProcessor to invoke for this particular entry.
 * @param arguments Additional arguments to pass to the entry processor.
 * @param keepBinary True if it should operate with binary objects.
 * @param async True if async API should be used.
 * @param checks Collection of checks to perform against the operation result.
 * @param next Next action from chain to invoke upon this one completion.
 * @param ctx Scenario context.
 */
class CacheInvokeAllMapAction[K, V, T](
    requestName: Expression[String],
    cacheName: Expression[String],
    map: Expression[SortedMap[K, CacheEntryProcessor[K, V, T]]],
    val arguments: Seq[Expression[Any]],
    keepBinary: Boolean,
    async: Boolean,
    val checks: Seq[IgniteCheck[K, EntryProcessorResult[T]]],
    next: Action,
    ctx: ScenarioContext
) extends CacheAction[K, V]("invokeAll", requestName, ctx, next, cacheName, keepBinary, async)
    with CacheInvokeAllAction[K, V, T] {

    override protected def execute(session: Session): Unit = withSessionCheck(session) {
        for {
            CacheActionParameters(resolvedRequestName, cacheApi, _) <- resolveCacheParameters(session)
            resolvedMap <- map(session)
            resolvedArguments <- resolveArguments(session, arguments)
        } yield execute(session, resolvedRequestName, cacheApi, resolvedMap, resolvedArguments)
    }
}

/**
 * Action for the invokeAll Ignite operation in case a single cache processor should
 * be executed for all cache entries.
 *
 * @tparam K Type of the cache key.
 * @tparam V Type of the cache value.
 * @tparam T Type of the operation result.
 * @param requestName Name of the request.
 * @param cacheName Name of cache.
 * @param keys Collection of cache entry keys.
 * @param processor Instance of CacheEntryProcessor.
 * @param arguments Additional arguments to pass to the entry processor.
 * @param keepBinary True if it should operate with binary objects.
 * @param async True if async API should be used.
 * @param checks Collection of checks to perform against the operation result.
 * @param next Next action from chain to invoke upon this one completion.
 * @param ctx Scenario context.
 */
class CacheInvokeAllSingleProcessorAction[K, V, T](
    requestName: Expression[String],
    cacheName: Expression[String],
    keys: Expression[SortedSet[K]],
    processor: CacheEntryProcessor[K, V, T],
    val arguments: Seq[Expression[Any]],
    keepBinary: Boolean,
    async: Boolean,
    val checks: Seq[IgniteCheck[K, EntryProcessorResult[T]]],
    next: Action,
    ctx: ScenarioContext
) extends CacheAction[K, V]("invokeAll", requestName, ctx, next, cacheName, keepBinary, async)
    with CacheInvokeAllAction[K, V, T] {

    override protected def execute(session: Session): Unit = withSessionCheck(session) {
        for {
            CacheActionParameters(resolvedRequestName, cacheApi, _) <- resolveCacheParameters(session)
            resolvedKeys <- keys(session)
            resolvedArguments <- resolveArguments(session, arguments)
        } yield execute(session, resolvedRequestName, cacheApi, toSortedMap(resolvedKeys), resolvedArguments)
    }

    private def toSortedMap(resolvedKeys: SortedSet[K]): SortedMap[K, CacheEntryProcessor[K, V, T]] = {
        implicit val ordering = resolvedKeys.ordering
        SortedMap.from(resolvedKeys.unsorted.map(k => (k, processor)))
    }
}
