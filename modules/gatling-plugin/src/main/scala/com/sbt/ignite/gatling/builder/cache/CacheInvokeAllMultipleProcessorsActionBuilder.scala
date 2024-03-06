/*
 * Copyright 2023 JSC SberTech
 */
package com.sbt.ignite.gatling.builder.cache

import javax.cache.processor.EntryProcessorResult

import scala.collection.SortedMap

import com.sbt.ignite.gatling.action.cache.CacheInvokeAllMapAction
import io.gatling.core.action.Action
import io.gatling.core.action.builder.ActionBuilder
import io.gatling.core.session.Expression
import io.gatling.core.structure.ScenarioContext
import org.apache.ignite.cache.CacheEntryProcessor

/**
 * Base invokeAll action builder.
 *
 * @tparam K Type of the cache key.
 * @tparam V Type of the cache value.
 * @tparam T Type of the entry processor result.
 * @param cacheName Cache name.
 * @param map Map from cache entry key to CacheEntryProcessor to invoke for this particular entry.
 */
class CacheInvokeAllMultipleProcessorsActionBuilder[K, V, T](
    cacheName: Expression[String],
    map: Expression[SortedMap[K, CacheEntryProcessor[K, V, T]]]
) extends ActionBuilder
    with CacheActionCommonParameters
    with CheckParameters[K, EntryProcessorResult[T]] {

    /** Additional arguments to pass to the entry processor. */
    var arguments: Seq[Expression[Any]] = Seq.empty

    /**
     * Builds an action.
     *
     * @param ctx The scenario context.
     * @param next The action that will be chained with the Action build by this builder.
     * @return The resulting action.
     */
    override def build(ctx: ScenarioContext, next: Action): Action =
        new CacheInvokeAllMapAction[K, V, T](
            requestName,
            cacheName,
            map,
            arguments,
            withKeepBinary,
            withAsync,
            Seq.empty,
            next,
            ctx
        )

    /**
     * Specify additional arguments to pass to the entry processor.
     *
     * @param args Additional arguments to pass to the entry processor.
     * @return itself.
     */
    def args(args: Expression[Any]*): CacheInvokeAllMultipleProcessorsActionBuilder[K, V, T] = {
        arguments = args
        this
    }
}
