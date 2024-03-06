/*
 * Copyright 2023 JSC SberTech
 */
package com.sbt.ignite.gatling.api

/**
 * Wrapper around the Ignite org.apache.ignite.transactions.Transaction object.
 */
trait TransactionApi {
    /**
     * Commits the enclosed transaction
     *
     * @param s Function to be called if operation is competed successfully.
     * @param f Function to be called if exception occurs.
     */
    def commit(s: Unit => Unit, f: Throwable => Unit): Unit

    /**
     * Rollbacks the enclosed transaction
     *
     * @param s Function to be called if operation is competed successfully.
     * @param f Function to be called if exception occurs.
     */
    def rollback(s: Unit => Unit, f: Throwable => Unit): Unit

    /**
     * Closes the enclosed transaction
     *
     * @param s Function to be called if operation is competed successfully.
     * @param f Function to be called if exception occurs.
     */
    def close(s: Unit => Unit, f: Throwable => Unit): Unit
}
