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
import java.util.logging.Logger;
import jakarta.transaction.RollbackException;
import jakarta.transaction.Synchronization;
import jakarta.transaction.SystemException;
import jakarta.transaction.Transaction;
import jakarta.transaction.TransactionManager;
import javax.sql.DataSource;
import javax.sql.XAConnection;
import javax.sql.XADataSource;

/**
 * Minimal managed data source that enlists XA connections in the current JTA transaction
 * ({@code jakarta.transaction} API, compatible with Narayana).
 * <p>
 * Wraps an {@link XADataSource} and automatically enlists the XA resource on {@code getConnection()}
 * and closes the XA connection on transaction completion or proxy {@code close()}.
 */
public class SimpleManagedDataSource implements DataSource {
    /** */
    private final XADataSource xaDataSrc;

    /** */
    private final TransactionManager transactionMgr;

    /** */
    private boolean dfltAutoCommit = false;

    /** Sets the default auto-commit mode for connections. */
    public void setDefaultAutoCommit(boolean dfltAutoCommit) {
        this.dfltAutoCommit = dfltAutoCommit;
    }

    /**
     * @param xaDataSrc Data source.
     * @param transactionMgr Transaction manager.
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
    @Override public Connection getConnection(String username, String pwd) throws SQLException {
        XAConnection xaConn = openXaConnection(username, pwd);

        Connection conn = xaConn.getConnection();
        conn.setAutoCommit(dfltAutoCommit);

        Transaction tx = currentTransaction();

        if (tx != null) {
            try {
                tx.enlistResource(xaConn.getXAResource());
            }
            catch (Exception e) {
                closeQuietly(xaConn);

                throw new SQLException("Failed to enlist XA resource", e);
            }

            try {
                tx.registerSynchronization(new Synchronization() {
                    /** {@inheritDoc} */
                    @Override public void beforeCompletion() {
                        // No-op.
                    }

                    /** {@inheritDoc} */
                    @Override public void afterCompletion(int status) {
                        closeQuietly(xaConn);
                    }
                });
            }
            catch (RollbackException | SystemException e) {
                throw new RuntimeException(e);
            }
        }

        return (Connection)Proxy.newProxyInstance(Connection.class.getClassLoader(), new Class<?>[]{Connection.class},
            new CloseHandler(conn, xaConn));
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
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void setLoginTimeout(int seconds) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public int getLoginTimeout() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Unsupported");
    }

    /** */
    private XAConnection openXaConnection(String username, String pwd) throws SQLException {
        try {
            return username != null ? xaDataSrc.getXAConnection(username, pwd) : xaDataSrc.getXAConnection();
        }
        catch (Exception e) {
            throw new SQLException("Failed to get XA connection", e);
        }
    }

    /** */
    private Transaction currentTransaction() throws SQLException {
        try {
            return transactionMgr.getTransaction();
        }
        catch (SystemException e) {
            throw new SQLException("Failed to get current transaction", e);
        }
    }

    /** */
    private static void closeQuietly(XAConnection xaConn) {
        try {
            xaConn.close();
        }
        catch (SQLException ignore) {
            // No-op.
        }
    }

    /**
     * Invocation handler that delegates all calls to the real connection,
     * but closes the XA connection on {@code close()} if it hasn't been closed already.
     */
    private static final class CloseHandler implements InvocationHandler {
        /** */
        private final Connection delegate;

        /** */
        private XAConnection xaConn;

        /**
         * @param delegate Delegate.
         * @param xaConn Connection.
         */
        CloseHandler(Connection delegate, XAConnection xaConn) {
            this.delegate = delegate;
            this.xaConn = xaConn;
        }

        /** {@inheritDoc} */
        @Override public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            if ("close".equals(method.getName()) && method.getParameterCount() == 0) {
                XAConnection xa = xaConn;
                xaConn = null; // prevent double close

                if (xa != null)
                    closeQuietly(xa);

                return null;
            }

            switch (method.getName()) {
                case "equals" -> {
                    if (args[0] == delegate)
                        return Boolean.TRUE;

                    return Proxy.isProxyClass(args[0].getClass()) && Proxy.getInvocationHandler(args[0]) == this;
                }
                case "hashCode" -> {
                    return System.identityHashCode(delegate);
                }
                case "getClass" -> {
                    return delegate.getClass();
                }
            }

            return method.invoke(delegate, args);
        }
    }
}
