/*
 * Copyright 2023 JSC SberTech
 */
package com.sbt.ignite.gatling.builder.ignite

import com.sbt.ignite.gatling.action.ignite.GetOrCreateCacheAction
import io.gatling.core.action.Action
import io.gatling.core.action.builder.ActionBuilder
import io.gatling.core.session.EmptyStringExpressionSuccess
import io.gatling.core.session.Expression
import io.gatling.core.structure.ScenarioContext
import org.apache.ignite.cache.CacheAtomicityMode
import org.apache.ignite.cache.CacheMode
import org.apache.ignite.client.ClientCacheConfiguration
import org.apache.ignite.configuration.CacheConfiguration

/**
 * Base get or create cache action builder.
 *
 * Works in two modes. The simplified one allows to specify three basic parameters via the DSL (backups, atomicity and mode).
 * Other way the full fledged instances of ClientCacheConfiguration or CacheConfiguration may be passed via the `cfg`
 * method.
 *
 * @tparam K Type of the cache key.
 * @tparam V Type of the cache value.
 * @param cacheName Cache name.
 */
case class GetOrCreateCacheActionBuilderBase[K, V](cacheName: Expression[String]) extends ActionBuilder {
    /**
     * Specify number of backup copies.
     *
     * @param backups Number of backup copies.
     * @return Build step to specify other cache parameters via the DSL.
     */
    def backups(backups: Integer): GetOrCreateCacheActionBuilderSimpleConfigStep =
        GetOrCreateCacheActionBuilderSimpleConfigStep(cacheName, SimpleCacheConfiguration(backups = backups))

    /**
     * Specify atomicity.
     *
     * @param atomicity Atomicity.
     * @return Build step to specify other cache parameters via the DSL.
     */
    def atomicity(atomicity: CacheAtomicityMode): GetOrCreateCacheActionBuilderSimpleConfigStep =
        GetOrCreateCacheActionBuilderSimpleConfigStep(cacheName, SimpleCacheConfiguration(atomicity = atomicity))

    /**
     * Specify cache partitioning mode.
     *
     * @param cacheMode Cache partitioning mode.
     * @return Build step to specify other cache parameters via the DSL.
     */
    def mode(cacheMode: CacheMode): GetOrCreateCacheActionBuilderSimpleConfigStep =
        GetOrCreateCacheActionBuilderSimpleConfigStep(cacheName, SimpleCacheConfiguration(cacheMode = cacheMode))

    /**
     * Specify full cache configuration via the ClientCacheConfiguration instance
     * which is part of Ignite Client (thin) API.
     *
     * @param clientCacheCfg Client cache configuration.
     * @return Build step to specify other cache parameters via the DSL.
     */
    def cfg(clientCacheCfg: ClientCacheConfiguration): GetOrCreateCacheActionBuilder[K, V] =
        new GetOrCreateCacheActionBuilder(cacheName, ThinConfiguration(clientCacheCfg))

    /**
     * Specify full cache configuration via the CacheConfiguration instance
     * which is part of Ignite Node (thick) API.
     *
     * @param cacheCfg Cache configuration.
     * @return Build step to specify other cache parameters via the DSL.
     */
    def cfg(cacheCfg: CacheConfiguration[K, V]): GetOrCreateCacheActionBuilder[K, V] =
        new GetOrCreateCacheActionBuilder(cacheName, ThickConfiguration(cacheCfg))

    /**
     * Builds an action.
     *
     * @param ctx The scenario context.
     * @param next The action that will be chained with the Action build by this builder.
     * @return The resulting action.
     */
    override def build(ctx: ScenarioContext, next: Action): Action =
        new GetOrCreateCacheActionBuilder(cacheName, SimpleCacheConfiguration()).build(ctx, next)

    /**
     * Specify request name for action.
     *
     * @param requestName Request name.
     * @return itself.
     */
    def as(requestName: Expression[String]): ActionBuilder =
        new GetOrCreateCacheActionBuilder(cacheName, SimpleCacheConfiguration()).as(requestName)
}

/**
 * Builder step to specify simple cache parameters via DSL.
 *
 * @param cacheName Cache name.
 * @param simpleConfig Simple cache configuration instance.
 */
case class GetOrCreateCacheActionBuilderSimpleConfigStep(
    cacheName: Expression[String],
    simpleConfig: SimpleCacheConfiguration
) extends ActionBuilder {
    /**
     * Specify number of backup copies.
     *
     * @param backups Number of backup copies.
     * @return Build step to specify other cache parameters via the DSL.
     */
    def backups(backups: Integer): GetOrCreateCacheActionBuilderSimpleConfigStep =
        this.copy(simpleConfig = simpleConfig.copy(backups = backups))

    /**
     * Specify atomicity.
     *
     * @param atomicity Atomicity.
     * @return Build step to specify other cache parameters via the DSL.
     */
    def atomicity(atomicity: CacheAtomicityMode): GetOrCreateCacheActionBuilderSimpleConfigStep =
        this.copy(simpleConfig = simpleConfig.copy(atomicity = atomicity))

    /**
     * Specify cache partitioning mode.
     *
     * @param cacheMode Cache partitioning mode.
     * @return Build step to specify other cache parameters via the DSL.
     */
    def mode(cacheMode: CacheMode): GetOrCreateCacheActionBuilderSimpleConfigStep =
        this.copy(simpleConfig = simpleConfig.copy(cacheMode = cacheMode))

    /**
     * Builds an action.
     *
     * @param ctx The scenario context.
     * @param next The action that will be chained with the Action build by this builder.
     * @return The resulting action.
     */
    override def build(ctx: ScenarioContext, next: Action): Action =
        createCacheActionBuilder.build(ctx, next)

    /**
     * Specify request name for action.
     *
     * @param requestName Request name.
     * @return itself.
     */
    def as(requestName: Expression[String]): ActionBuilder =
        createCacheActionBuilder.as(requestName)

    private def createCacheActionBuilder[K, V]: GetOrCreateCacheActionBuilder[K, V] =
        new GetOrCreateCacheActionBuilder[K, V](
            cacheName,
            simpleConfig
        )
}

/**
 * @tparam K Type of the cache key.
 * @tparam V Type of the cache value.
 * @param cacheName Cache name.
 * @param config Cache configuration.
 */
class GetOrCreateCacheActionBuilder[K, V](cacheName: Expression[String], config: Configuration) extends ActionBuilder {
    /** Request name. */
    var requestName: Expression[String] = EmptyStringExpressionSuccess

    /**
     * Builds an action.
     *
     * @param ctx  The scenario context.
     * @param next The action that will be chained with the Action build by this builder.
     * @return The resulting action.
     */
    override def build(ctx: ScenarioContext, next: Action): Action =
        new GetOrCreateCacheAction(requestName, cacheName, config, next, ctx)

    /**
     * Specify request name for action.
     *
     * @param requestName Request name.
     * @return itself.
     */
    def as(requestName: Expression[String]): GetOrCreateCacheActionBuilder[K, V] = {
        this.requestName = requestName
        this
    }
}

/**
 * Abstract cache configuration
 */
sealed trait Configuration

/**
 * Simplified cache configuration.
 *
 * @param backups Number of backup copies.
 * @param atomicity Atomicity.
 * @param cacheMode Cache partitioning mode.
 */
case class SimpleCacheConfiguration(
    backups: Integer = 0,
    atomicity: CacheAtomicityMode = CacheAtomicityMode.ATOMIC,
    cacheMode: CacheMode = CacheMode.PARTITIONED
) extends Configuration

/**
 * Ignite Client (thin) cache configuration.
 *
 * @param cfg ClientCacheConfiguration instance.
 */
case class ThinConfiguration(cfg: ClientCacheConfiguration) extends Configuration

/**
 * Ignite node (thick) cache configuration.
 *
 * @param cfg CacheConfiguration instance.
 */
case class ThickConfiguration(cfg: CacheConfiguration[_, _]) extends Configuration
