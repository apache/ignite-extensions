/*
 * Copyright 2023 JSC SberTech
 */
package com.sbt.ignite.gatling.api.node

import scala.util.Try

import com.sbt.ignite.gatling.api.TransactionApi
import org.apache.ignite.transactions.Transaction

/**
 * Implementation of TransactionApi working via the Ignite node (thick) API.
 *
 * @param wrapped Enclosed Ignite instance.
 */
case class TransactionNodeApi(wrapped: Transaction) extends TransactionApi {

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
