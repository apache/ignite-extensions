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

import java.util.Collections;
import javax.cache.configuration.Factory;
import jakarta.transaction.Synchronization;
import jakarta.transaction.TransactionManager;
import jakarta.transaction.UserTransaction;
import com.arjuna.ats.internal.jta.transaction.arjunacore.TransactionManagerImple;
import com.arjuna.ats.internal.jta.transaction.arjunacore.UserTransactionImple;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.h2.jdbcx.JdbcDataSource;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cache.spi.access.AccessType;
import org.hibernate.cfg.Environment;
import org.hibernate.engine.jdbc.connections.internal.DatasourceConnectionProviderImpl;
import org.hibernate.engine.jdbc.connections.spi.ConnectionProvider;
import org.hibernate.engine.transaction.jta.platform.internal.AbstractJtaPlatform;
import org.hibernate.engine.transaction.jta.platform.spi.JtaPlatform;
import org.hibernate.resource.transaction.backend.jta.internal.JtaTransactionCoordinatorBuilderImpl;
import org.jetbrains.annotations.Nullable;

/**
 *
 * Tests Hibernate L2 cache with TRANSACTIONAL access mode (Hibernate and Cache are configured
 * to used the same TransactionManager).
 */
public class HibernateL2CacheTransactionalSelfTest extends HibernateL2CacheSelfTest {
    /** */
    private static TransactionManager txMgr;

    /** */
    private static UserTransaction userTx;

    /** */
    private static class TestJtaPlatform extends AbstractJtaPlatform {
        /** {@inheritDoc} */
        @Override protected TransactionManager locateTransactionManager() {
            return txMgr;
        }

        /** {@inheritDoc} */
        @Override protected UserTransaction locateUserTransaction() {
            return userTx;
        }
    }

    /** */
    @SuppressWarnings("PublicInnerClass")
    public static class TestTmFactory implements Factory<TransactionManager> {
        /** */
        private static final long serialVersionUID = 0;

        /** {@inheritDoc} */
        @Override public TransactionManager create() {
            return txMgr;
        }
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        txMgr = new TransactionManagerImple();
        userTx = new UserTransactionImple();

        super.beforeTestsStarted();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        txMgr = null;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.getTransactionConfiguration().setTxManagerFactory(new TestTmFactory());
        cfg.getTransactionConfiguration().setUseJtaSynchronization(useJtaSynchronization());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected CacheConfiguration transactionalRegionConfiguration(String regionName) {
        CacheConfiguration cfg = super.transactionalRegionConfiguration(regionName);

        cfg.setNearConfiguration(null);

        return cfg;
    }

    /** {@inheritDoc} */
    @Nullable @Override protected StandardServiceRegistryBuilder registryBuilder() {
        StandardServiceRegistryBuilder builder = new StandardServiceRegistryBuilder();

        DatasourceConnectionProviderImpl connProvider = new DatasourceConnectionProviderImpl();

        JdbcDataSource h2DataSrc = new JdbcDataSource();
        h2DataSrc.setURL(CONNECTION_URL);

        SimpleManagedDataSource dataSrc = new SimpleManagedDataSource(h2DataSrc, txMgr); // JTA-aware data source (jakarta.transaction).
        dataSrc.setDefaultAutoCommit(false);

        connProvider.setDataSource(dataSrc);

        connProvider.configure(Collections.emptyMap());

        builder.addService(ConnectionProvider.class, connProvider);

        builder.addService(JtaPlatform.class, new TestJtaPlatform());

        builder.applySetting(Environment.TRANSACTION_COORDINATOR_STRATEGY, JtaTransactionCoordinatorBuilderImpl.class.getName());

        return builder;
    }

    /** {@inheritDoc} */
    @Override protected AccessType[] accessTypes() {
        return new AccessType[]{AccessType.TRANSACTIONAL};
    }

    /**
     * @return Whether to use {@link Synchronization}.
     */
    protected boolean useJtaSynchronization() {
        return false;
    }
}
