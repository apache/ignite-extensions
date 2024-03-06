/*
 * Copyright 2023 JSC SberTech
 */
package org.apache.ignite.gatling

/**
 * Ignite Gatling DSL definitions.
 */
object Predef extends IgniteDsl {
    /** Fully replicated cache mode. */
    val REPLICATED = org.apache.ignite.cache.CacheMode.REPLICATED
    /** Partitioned cache mode */
    val PARTITIONED = org.apache.ignite.cache.CacheMode.PARTITIONED

    /** Transactional cache atomicity mode. */
    val TRANSACTIONAL = org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL
    /** Atomic cache atomicity mode. */
    val ATOMIC = org.apache.ignite.cache.CacheAtomicityMode.ATOMIC

    /** Pessimistic transaction concurrency control. */
    val PESSIMISTIC = org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC
    /** Optimistic transaction concurrency control. */
    val OPTIMISTIC = org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC

    /** Repeatable read transaction isolation level. */
    val REPEATABLE_READ = org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ
    /** Read committed transaction isolation level. */
    val READ_COMMITTED = org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED
    /** Serializable read transaction isolation level. */
    val SERIALIZABLE = org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE
}
