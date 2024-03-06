/*
 * Copyright 2023 JSC SberTech
 */
package org.apache.ignite.gatling.protocol

import java.util.concurrent.ConcurrentLinkedQueue

import io.netty.util.internal.ThreadLocalRandom
import org.apache.ignite.Ignition
import org.apache.ignite.client.IgniteClient
import org.apache.ignite.configuration.ClientConfiguration


/**
 * Pool of the Ignite (thin) client instances.
 */
trait IgniteClientPool {
    /**
     * @return Initialized IgniteClient instance.
     */
    def apply(): IgniteClient

    /**
     * Close any IgniteClient instances created.
     */
    def close(): Unit
}

/**
 * Pool creating single IgniteClient instance per calling thread.
 *
 * @param cfg Thin client configuration.
 */
class IgniteClientPerThreadPool(cfg: ClientConfiguration) extends IgniteClientPool {
    private val clients = new ConcurrentLinkedQueue[IgniteClient]()

    private val client = ThreadLocal.withInitial[IgniteClient](() => {
        val client = Ignition.startClient(cfg)

        clients.add(client)

        client
    })

    override def apply(): IgniteClient = client.get()

    override def close(): Unit = {
        clients.forEach(c => c.close())

        clients.clear()
    }
}

/**
 * Fixed size IgniteClients pool.
 *
 * @param cfg Thin client configuration.
 * @param size Size of the pool.
 */
class IgniteClientFixedSizePool(cfg: ClientConfiguration, size: Int) extends IgniteClientPool {
    private val clients = (0 until size).map(_ => Ignition.startClient(cfg)).toArray

    override def apply(): IgniteClient = if (size == 1) {
        clients(0)
    } else {
        clients(ThreadLocalRandom.current().nextInt(size))
    }

    override def close(): Unit = {
        clients.foreach(c => c.close())
    }
}
