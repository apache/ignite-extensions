/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.gatling.protocol

import io.gatling.commons.util.Clock
import io.gatling.core.CoreComponents
import io.gatling.core.config.GatlingConfiguration
import io.gatling.core.protocol.Protocol
import io.gatling.core.protocol.ProtocolKey
import org.apache.ignite.Ignite
import org.apache.ignite.client.IgniteClient
import org.apache.ignite.configuration.ClientConfiguration
import org.apache.ignite.configuration.IgniteConfiguration
import org.apache.ignite.gatling.api.IgniteApi

/**
 * Ignite protocol globals.
 */
object IgniteProtocol {
    /** Session key the IgniteApi instance is stored under. */
    val IgniteApiSessionKey = "__igniteApi__"

    /**
     * Session key the TransactionApi instance is stored under.
     *
     * Presence of the TransactionApi instance in session means that the Ignite transaction is active now.
     */
    val TransactionApiSessionKey = "__transactionApi__"

    /**
     * Session key for explicit locks flag.
     *
     * Presence of flag means that explicit locks were created in scenario.
     */
    val ExplicitLockWasUsedSessionKey = "__explicitLockWasUsed__"

    /**
     * Initialise components of the Ignite protocol on simulation start.
     */
    val IgniteProtocolKey: ProtocolKey[IgniteProtocol, IgniteComponents] = new ProtocolKey[IgniteProtocol, IgniteComponents] {
        override def protocolClass: Class[Protocol] =
            classOf[IgniteProtocol].asInstanceOf[Class[Protocol]]

        override def defaultProtocolValue(configuration: GatlingConfiguration): IgniteProtocol =
            IgniteProtocolBuilderBase.igniteCfg(new IgniteConfiguration()).build

        /**
         * Return lambda to init the Ignite protocol components.
         *
         * Note, lambda would start new Ignite API instance if none was passed as a protocol configuration parameter
         * (unless the `explicitClientStart` protocol parameter was used).
         *
         * If IGNITE_GATLING_USE_NANO_CLOCK system property is set substitutes the default Gatling clocks (which
         * measures time in millis) with clocks measuring time in nanoseconds. This clocks will be used to measure
         * duration of Ignite actions. This gives more precision for sub-millis requests.
         *
         * Note however that using nano clocks means the following limitations:
         *    - Native Gatling report generation doesn't work. So custom reporting should be implemented which would
         *      directly parse the Gatling generated simulation.log files.
         *    - Action groups duration measuring doesn't work. So testing scenarios should not contains any groups.
         *
         * @param coreComponents Gatling core components.
         * @return Lambda creating Ignite components from the Ignite protocol parameters provided.
         */
        override def newComponents(coreComponents: CoreComponents): IgniteProtocol => IgniteComponents =
            igniteProtocol => {
                val components = IgniteComponents(
                    coreComponents,
                    igniteProtocol,
                    IgniteApi(igniteProtocol),
                    Option(System.getProperty("IGNITE_GATLING_USE_NANO_CLOCK")).map(_ => new NanoClock())
                )

                igniteProtocol.cfg match {
                    case IgniteClientConfigurationCfg(_) if !igniteProtocol.explicitClientStart => igniteProtocol.api = components.igniteApi

                    case IgniteConfigurationCfg(_) if !igniteProtocol.explicitClientStart => igniteProtocol.api = components.igniteApi

                    case _ =>
                }

                components
            }
    }

    /**
     * Clock implementation measuring time in nanoseconds.
     *
     * NOTE, it returns nanoseconds from nowMillis() method !!!
     */
    private class NanoClock extends Clock {
        override def nowSeconds: Long = nowMillis / 1000000

        private val currentTimeMillisReference = System.currentTimeMillis
        private val nanoTimeReference = System.nanoTime

        override def nowMillis: Long = (System.nanoTime - nanoTimeReference) + currentTimeMillisReference * 1000000
    }
}

/**
 * Ignite protocol parameters.
 *
 * @param cfg Ignite API configuration.
 * @param explicitClientStart If true the default shared instance of Ignite API will not be started before the
 *                          simulation start and it will not be automatically inserted into the client session
 *                          upon injection into the scenario. To start Ignite API instance scenario should contain
 *                          explicit `startIgniteApi` and `closeIgniteApi` actions.
 * @param api Ignite API instance if started during the simulation. Stored here to be able to close it after
 *            simulation finish.
 */
case class IgniteProtocol(cfg: IgniteCfg, explicitClientStart: Boolean = false, var api: Option[IgniteApi] = None) extends Protocol {
    /**
     * Close all opened clients.
     */
    def close(): Unit =
        cfg match {
            case cfg: IgniteClientPoolCfg        => cfg.pool.close()
            case _: IgniteConfigurationCfg       => api.foreach(api => api.close()(_ => (), _ => ()))
            case _: IgniteClientConfigurationCfg => api.foreach(api => api.close()(_ => (), _ => ()))
            case _                               =>
        }
}

/**
 * Abstract Ignite API configuration.
 */
sealed trait IgniteCfg

/**
 * Ignite API configuration containing the pre-started Ignite (thin) Client instance.
 *
 * @param client Instance of Ignite (thin) Client.
 */
case class IgniteClientCfg(client: IgniteClient) extends IgniteCfg

/**
 * Ignite API configuration containing the pre-started Ignite (thick) node instance.
 *
 * @param ignite Instance of the Ignite grid.
 */
case class IgniteNodeCfg(ignite: Ignite) extends IgniteCfg

/**
 * Ignite API configuration containing the Ignite (thin) Client configuration instance.
 *
 * @param cfg Ignite (thin) client configuration.
 */
case class IgniteClientConfigurationCfg(cfg: ClientConfiguration) extends IgniteCfg

/**
 * Ignite API configuration containing the Ignite node (thick) configuration instance.
 *
 * @param cfg Ignite (thick) node configuration.
 */
case class IgniteConfigurationCfg(cfg: IgniteConfiguration) extends IgniteCfg

/**
 * Ignite API configuration containing the Ignite (thin) Client instances pool.
 *
 * @param pool Ignite (thin) client instances pool.
 */
case class IgniteClientPoolCfg(pool: IgniteClientPool) extends IgniteCfg
