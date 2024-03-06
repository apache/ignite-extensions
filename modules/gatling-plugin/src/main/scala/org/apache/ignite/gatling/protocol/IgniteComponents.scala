/*
 * Copyright 2023 JSC SberTech
 */
package org.apache.ignite.gatling.protocol

import org.apache.ignite.gatling.api.IgniteApi
import org.apache.ignite.gatling.protocol.IgniteProtocol.IgniteApiSessionKey
import com.typesafe.scalalogging.StrictLogging
import io.gatling.core.CoreComponents
import io.gatling.core.protocol.ProtocolComponents
import io.gatling.core.session.Session

/**
 * Ignite gatling protocol components holder.
 *
 * @param coreComponents Core Gatling components.
 * @param igniteProtocol Ignite protocol instance.
 * @param igniteApi Shared default Ignite API instance.
 */
case class IgniteComponents(coreComponents: CoreComponents, igniteProtocol: IgniteProtocol, igniteApi: Option[IgniteApi] = None)
    extends ProtocolComponents
    with StrictLogging {

    /**
     * Return lambda to init client session before injection into scenario.
     *
     * Lambda puts default shared Ignite API instance (if it exists) into the session object.
     *
     * @return Lambda to be called to init session.
     */
    override def onStart: Session => Session = session =>
        igniteApi
            .map(api => session.set(IgniteApiSessionKey, api))
            .getOrElse(session)

    /**
     * Return lambda to clean-up the client session after scenario finish.
     *
     * Lambda closes Ignite API instance if it is still in the session (unless it's a default shared instance).
     *
     * @return Lambda to be called to clean-up session.
     */
    override def onExit: Session => Unit = session =>
        if (igniteApi.isEmpty) {
            session(IgniteApiSessionKey)
                .asOption[IgniteApi]
                .foreach(_.close()(_ => (), _ => ()))
        }
}
