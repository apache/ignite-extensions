/*
 * Copyright 2023 JSC SberTech
 */
package org.apache.ignite.gatling.api.thin

import scala.util.Try

import org.apache.ignite.gatling.api.TransactionApi
import org.apache.ignite.client.ClientTransaction

/**
 * Implementation of TransactionApi working via the Ignite (thin) Client API.
 *
 * @param wrapped Enclosed IgniteClient instance.
 */
case class TransactionThinApi(wrapped: ClientTransaction) extends TransactionApi {
    override def commit(s: Unit => Unit, f: Throwable => Unit): Unit =
        Try(wrapped.commit())
            .fold(f, s)

    override def rollback(s: Unit => Unit, f: Throwable => Unit): Unit =
        Try(wrapped.rollback())
            .fold(f, s)

    override def close(s: Unit => Unit, f: Throwable => Unit): Unit =
        Try(wrapped.close())
            .fold(f, s)
}
