/*
 * Copyright 2023 JSC SberTech
 */
package com.sbt.ignite.gatling.protocol

import scala.jdk.CollectionConverters._

import org.apache.ignite.Ignite
import org.apache.ignite.client.IgniteClient
import org.apache.ignite.configuration.ClientConfiguration
import org.apache.ignite.configuration.IgniteConfiguration
import org.apache.ignite.internal.IgniteComponentType.SPRING
import org.apache.ignite.internal.IgnitionEx
import org.apache.ignite.internal.util.IgniteUtils
import org.apache.ignite.internal.util.spring.IgniteSpringHelper

trait IgniteProtocolSupport {
    /** Ignite protocol builder starting point. */
    val igniteProtocol: IgniteProtocolBuilderBase.type = IgniteProtocolBuilderBase

    /**
     * Implicit to build protocol from the IgniteProtocolBuilder.
     * @param builder Protocol builder.
     * @return Protocol.
     */
    implicit def builder2igniteProtocol(builder: IgniteProtocolBuilder): IgniteProtocol = builder.build

    /**
     * Implicit to build protocol from the IgniteProtocolBuilderExplicitStartStep.
     * @param builder Protocol builder.
     * @return Protocol.
     */
    implicit def builderExplicitStartStep2igniteProtocol(builder: IgniteProtocolBuilderExplicitStartStep): IgniteProtocol = builder.build
}

/**
 * Base Ignite protocol builder.
 */
case object IgniteProtocolBuilderBase {
    /**
     * Specify Ignite API as pre-started Ignite (thin) Client instance.
     *
     * @param igniteClient IgniteClient instance.
     * @return Build step for additional protocol parameters.
     */
    def client(igniteClient: IgniteClient): IgniteProtocolBuilder =
        IgniteProtocolBuilder(IgniteClientCfg(igniteClient), explicitClientStart = false)

    /**
     * Specify Ignite API as Ignite (thin) client configuration.
     *
     * @param cfg ClientConfiguration instance.
     * @return Build step for additional protocol parameters.
     */
    def clientCfg(cfg: ClientConfiguration): IgniteProtocolBuilderExplicitStartStep =
        IgniteProtocolBuilderExplicitStartStep(IgniteClientConfigurationCfg(cfg))

    /**
     * Specify Ignite API as Ignite (thin) client Spring XML configuration file.
     *
     * @param cfgPath Path to Spring XML configuration file containing the ClientConfiguration bean.
     * @return Build step for additional protocol parameters.
     */
    def clientCfgPath(cfgPath: String): IgniteProtocolBuilderExplicitStartStep = {
        val spring: IgniteSpringHelper = SPRING.create(false)

        val clientCfg = spring.loadConfigurations(IgniteUtils.resolveSpringUrl(cfgPath), classOf[ClientConfiguration])
            .get1().asScala.head

        IgniteProtocolBuilderExplicitStartStep(IgniteClientConfigurationCfg(clientCfg))
    }

    /**
     * Specify Ignite API as Ignite (thin) Client instances pool.
     *
     * @param pool Pool of IgniteClient instances.
     * @return Build step for additional protocol parameters.
     */
    def clientPool(pool: IgniteClientPool): IgniteProtocolBuilder =
        IgniteProtocolBuilder(IgniteClientPoolCfg(pool), explicitClientStart = false)

    /**
     * Specify Ignite API as pre-started Ignite (thick) node instance.
     *
     * @param ignite Ignite instance.
     * @return Build step for additional protocol parameters.
     */
    def ignite(ignite: Ignite): IgniteProtocolBuilder =
        IgniteProtocolBuilder(IgniteNodeCfg(ignite), explicitClientStart = false)

    /**
     * Specify Ignite API as Ignite (thick) node configuration.
     *
     * @param cfg IgniteConfiguration instance.
     * @return Build step for additional protocol parameters.
     */
    def igniteCfg(cfg: IgniteConfiguration): IgniteProtocolBuilder =
        IgniteProtocolBuilder(IgniteConfigurationCfg(cfg), explicitClientStart = false)

    /**
     * Specify Ignite API as Ignite (thick) node Spring XML configuration file.
     *
     * @param cfgPath Path to Spring XML configuration file containing the IgniteConfiguration bean.
     * @return Build step for additional protocol parameters.
     */
    def igniteCfgPath(cfgPath: String): IgniteProtocolBuilder =
        IgniteProtocolBuilder(IgniteConfigurationCfg(IgnitionEx.loadConfiguration(cfgPath).get1()), explicitClientStart = false)
}

/**
 * Builder step for additional protocol parameters.
 *
 * @param cfg Ignite API configuration.
 */
case class IgniteProtocolBuilderExplicitStartStep(cfg: IgniteCfg) {
    /**
     * Specify the `withExplicitClientStart` flag.
     * @return Protocol builder further step.
     */
    def withExplicitClientStart: IgniteProtocolBuilder = IgniteProtocolBuilder(cfg, explicitClientStart = true)

    /**
     * Builds Ignite protocol instance.
     * @return Protocol builder further step.
     */
    def build: IgniteProtocol = IgniteProtocolBuilder(cfg, explicitClientStart = false).build
}

/**
 * Ignite protocol builder step for other parameters.
 *
 * @param cfg Ignite API configuration.
 * @param explicitClientStart Explicit client start flag.
 */
case class IgniteProtocolBuilder(cfg: IgniteCfg, explicitClientStart: Boolean) {
    /**
     * Builds Ignite protocol instance.
     * @return Protocol builder further step.
     */
    def build: IgniteProtocol = IgniteProtocol(cfg, explicitClientStart)
}
