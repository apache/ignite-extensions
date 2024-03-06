/*
 * Copyright 2023 JSC SberTech
 */
package org.apache.ignite.gatling.api

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.util.Failure
import scala.util.Success

package object thin {
    /** Execution context to run callbacks. */
    implicit val Ec: ExecutionContext = scala.concurrent.ExecutionContext.global

    /**
     * Helper to execute future with the callback functions passed.
     */
    trait CompletionSupport {
        /**
         * @param fut Future to execute.
         * @param s Function to be called if future is competed successfully.
         * @param f Function to be called if exception occurs.
         * @tparam T Type of the future result.
         */
        def withCompletion[T](fut: Future[T])(s: T => Unit, f: Throwable => Unit): Unit = fut.onComplete {
            case Success(value) => s(value)

            case Failure(exception) => f(exception)
        }
    }
}
