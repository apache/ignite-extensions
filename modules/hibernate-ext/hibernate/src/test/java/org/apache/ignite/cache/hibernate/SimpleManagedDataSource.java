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

package org.apache.ignite.cache.hibernate;

import java.io.PrintWriter;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Logger;
import javax.sql.DataSource;
import javax.sql.XAConnection;
import javax.sql.XADataSource;
import javax.transaction.xa.XAResource;
import jakarta.transaction.RollbackException;
import jakarta.transaction.Synchronization;
import jakarta.transaction.SystemException;
import jakarta.transaction.Transaction;
import jakarta.transaction.TransactionManager;

/**
 * Minimal managed data source that enlists XA connections in the current JTA transaction.
 * Uses {@code jakarta.transaction} API (compatible with Narayana).
 * <p>
 * Wraps an {@link XADataSource} and automatically enlists/delist the XA resource
 * on {@code getConnection()} / transaction completion.
 */
public class SimpleManagedDataSource implements DataSource {
    /** Underlying XA data source. */
    private final XADataSource xaDataSrc;

    /** Transaction manager for enlist/delist. */
    private final TransactionManager transactionMgr;

    /** Default auto-commit setting. */
    private boolean dfltAutoCommit = false;

    /** Sets the default auto-commit mode for connections. */
    public void setDefaultAutoCommit(boolean dfltAutoCommit) {
        this.dfltAutoCommit = dfltAutoCommit;
    }

    /** Tracks open XA connections keyed by wrapped connection identity. */
    private final ConcurrentMap<Object, XAConnection> xaConnections = new ConcurrentHashMap<>();

    /**
     * @param xaDataSrc The underlying XA data source.
     * @param transactionMgr The JTA transaction manager ({@code jakarta.transaction}).
     */
    public SimpleManagedDataSource(XADataSource xaDataSrc, TransactionManager transactionMgr) {
        this.xaDataSrc = xaDataSrc;
        this.transactionMgr = transactionMgr;
    }

    /** {@inheritDoc} */
    @Override public Connection getConnection() throws SQLException {
        return getConnection(null, null);
    }

    /** {@inheritDoc} */
    @Override public Connection getConnection(String username, String password) throws SQLException {
        XAConnection xaConn;
        try {
            xaConn = username != null ? xaDataSrc.getXAConnection(username, password) : xaDataSrc.getXAConnection();
        } catch (Exception e) {
            throw new SQLException("Failed to get XA connection", e);
        }

        Connection conn = xaConn.getConnection();
        conn.setAutoCommit(dfltAutoCommit);

        // Enlist XA resource in current transaction.
        Transaction tx;
        try {
            tx = transactionMgr.getTransaction();
        } catch (SystemException e) {
            try { xaConn.close(); } catch (SQLException ignore) {}
            throw new SQLException("Failed to get current transaction", e);
        }

        if (tx != null) {
            try {
                tx.enlistResource(xaConn.getXAResource());
            } catch (Exception e) {
                try { xaConn.close(); } catch (SQLException ignore) {}
                throw new SQLException("Failed to enlist XA resource", e);
            }

            // On transaction completion, delist the resource and close XA connection.
            try {
                tx.registerSynchronization(new DelistSynchronization(xaConn.getXAResource(), xaConn));
            }
            catch (RollbackException | SystemException e) {
                throw new RuntimeException(e);
            }
        }

        // Use a marker object as key so the proxy identity maps to the right XA connection.
        Object key = new Object();
        xaConnections.put(key, xaConn);

        @SuppressWarnings("unchecked")
        Connection proxy = (Connection) Proxy.newProxyInstance(
                Connection.class.getClassLoader(),
                new Class<?>[]{Connection.class},
                new ManagedConnectionInvocationHandler(conn, key, xaConnections));

        return proxy;
    }

    /** {@inheritDoc} */
    @Override public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new SQLException("Unsupported");
    }

    /** {@inheritDoc} */
    @Override public boolean isWrapperFor(Class<?> iface) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public PrintWriter getLogWriter() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void setLogWriter(PrintWriter out) {
        // no-op
    }

    /** {@inheritDoc} */
    @Override public void setLoginTimeout(int seconds) {
        // no-op
    }

    /** {@inheritDoc} */
    @Override public int getLoginTimeout() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Unsupported");
    }

    /**
     * Synchronization that delists the XA resource and closes the XA connection
     * after transaction completion.
     */
    private static final class DelistSynchronization implements Synchronization {
        private final XAResource xaResource;
        private final XAConnection xaConnection;

        DelistSynchronization(XAResource xaResource, XAConnection xaConnection) {
            this.xaResource = xaResource;
            this.xaConnection = xaConnection;
        }

        @Override
        public void beforeCompletion() {
            // nothing needed here - the TM handles XA protocol
        }

        @Override
        public void afterCompletion(int status) {
            try {
                xaConnection.close();
            } catch (SQLException ignore) {
                // ignore
            }
        }
    }

    /**
     * Invocation handler that delegates all calls to the real connection,
     * but cleans up the XA connection on {@code close()}.
     */
    private static final class ManagedConnectionInvocationHandler implements InvocationHandler {
        /** */
        private final Connection delegate;

        /** */
        private final Object xaKey;

        /** */
        private final ConcurrentMap<Object, XAConnection> xaConnections;

        /** */
        ManagedConnectionInvocationHandler(Connection delegate, Object xaKey,
                                          ConcurrentMap<Object, XAConnection> xaConnections) {
            this.delegate = delegate;
            this.xaKey = xaKey;
            this.xaConnections = xaConnections;
        }

        /** {@inheritDoc} */
        @Override public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            if ("close".equals(method.getName()) && method.getParameterCount() == 0) {
                XAConnection xaConn = xaConnections.remove(xaKey);

                if (xaConn != null) {
                    try {
                        xaConn.close();
                    }
                    catch (SQLException ignore) {
                        // already closed by DelistSynchronization if transaction completed
                    }
                }

                return null;
            }

            if ("equals".equals(method.getName())) {
                if (args[0] == delegate) return Boolean.TRUE;

                return Proxy.isProxyClass(args[0].getClass()) && Proxy.getInvocationHandler(args[0]) == this;
            }

            if ("hashCode".equals(method.getName()))
                return System.identityHashCode(delegate);

            if ("getClass".equals(method.getName()))
                return delegate.getClass();

            return method.invoke(delegate, args);
        }
    }
}
