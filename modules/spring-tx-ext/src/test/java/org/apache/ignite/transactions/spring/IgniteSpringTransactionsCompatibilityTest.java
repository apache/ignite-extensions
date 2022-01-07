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

package org.apache.ignite.transactions.spring;

import java.util.ArrayList;
import java.util.Collection;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.compatibility.testframework.junits.Dependency;
import org.apache.ignite.compatibility.testframework.junits.IgniteCompatibilityAbstractTest;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteVersionUtils;
import org.apache.ignite.internal.util.GridJavaProcess;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteProductVersion;
import org.apache.ignite.springdata.proxy.IgniteCacheProxy;
import org.apache.ignite.springdata.proxy.IgniteClientCacheProxy;
import org.apache.ignite.springdata.proxy.IgniteNodeCacheProxy;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.configuration.ClientConnectorConfiguration.DFLT_PORT;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/** */
@RunWith(Parameterized.class)
public class IgniteSpringTransactionsCompatibilityTest extends IgniteCompatibilityAbstractTest {
    /** Version 2.8.0. */
    private static final IgniteProductVersion VER_2_8_0 = IgniteProductVersion.fromString("2.8.0");

    /** Version 2.9.0. */
    private static final IgniteProductVersion VER_2_9_0 = IgniteProductVersion.fromString("2.9.0");

    /** Version 2.10.0. */
    private static final IgniteProductVersion VER_2_10_0 = IgniteProductVersion.fromString("2.10.0");

    /** Version 2.11.0. */
    private static final IgniteProductVersion VER_2_11_0 = IgniteProductVersion.fromString("2.11.0");

    /**  */
    private static final IgniteProductVersion[] TEST_IGNITE_VERSIONS = new IgniteProductVersion[] {
        VER_2_8_0,
        VER_2_9_0,
        VER_2_10_0,
        VER_2_11_0,
        IgniteVersionUtils.VER,
    };

    /**  */
    private static final String[] TEST_SPRING_VERSIONS = new String[] {
        "4.3.0.RELEASE",
        "5.0.0.RELEASE",
        "5.1.0.RELEASE",
        "5.2.0.RELEASE",
        "5.3.0"
    };

    /** */
    private static final String TEST_CACHE_NAME = "testCache";

    /** */
    @Parameterized.Parameters(name = "IgniteVersion={0}, SpringVersion={1}")
    public static Iterable<Object[]> versions() {
        ArrayList<Object[]> testVersions = new ArrayList<>();

        for (IgniteProductVersion igniteVer : TEST_IGNITE_VERSIONS) {
            for (String springVer : TEST_SPRING_VERSIONS)
                testVersions.add(new Object[]{igniteVer, springVer});
        }

        return testVersions;
    }

    /** */
    @Parameterized.Parameter
    public IgniteProductVersion igniteVer;

    /** */
    @Parameterized.Parameter(1)
    public String springVer;

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected @NotNull Collection<Dependency> getDependencies(String igniteVer) {
        if (!F.isEmpty(igniteVer))
            return super.getDependencies(igniteVer);

        Collection<Dependency> res = new ArrayList<>();

        String igniteDepVer = toDependencyVersion(this.igniteVer);

        res.add(new Dependency("core", "org.apache.ignite", "ignite-core", igniteDepVer, false));
        res.add(new Dependency("core", "org.apache.ignite", "ignite-core", igniteDepVer, true));
        res.add(new Dependency("spring", "org.apache.ignite", "ignite-spring", igniteDepVer, false));

        res.add(new Dependency("spring-tx", "org.springframework", "spring-tx", springVer, false));
        res.add(new Dependency("spring-context", "org.springframework", "spring-context", springVer, false));
        res.add(new Dependency("spring-beans", "org.springframework", "spring-beans", springVer, false));
        res.add(new Dependency("spring-core", "org.springframework", "spring-core", springVer, false));
        res.add(new Dependency("spring-aop", "org.springframework", "spring-aop", springVer, false));
        res.add(new Dependency("spring-expression", "org.springframework", "spring-expression", springVer, false));

        if (this.igniteVer.compareTo(VER_2_11_0) <= 0)
            res.add(new Dependency("spring-jdbc", "org.springframework", "spring-jdbc", springVer, false));

        return res;
    }

    /** */
    protected void processNodeConfiguration(IgniteConfiguration cfg) {
        cfg.setCacheConfiguration(new CacheConfiguration<>(TEST_CACHE_NAME)
            .setAtomicityMode(TRANSACTIONAL));
    }

    /** {@inheritDoc} */
    @Override protected Collection<String> getProcessProxyJvmArgs(String ver) throws Exception {
        Collection<String> jvmArgs = super.getProcessProxyJvmArgs(ver);

        jvmArgs.add("-DIGNITE_HOME=" + System.getProperty("user.dir"));

        return jvmArgs;
    }

    /** */
    @Test
    public void testCompatibility() throws Exception {
        startGrid(1, toDependencyVersion(igniteVer), this::processNodeConfiguration, ignite -> {});

        GridJavaProcess proc = GridJavaProcess.exec(
            TestRunner.class.getName(),
            "",
            log,
            log::info,
            null,
            null,
            getProcessProxyJvmArgs(""),
            null
        );

        try {
            assertTrue(waitForCondition(() -> !proc.getProcess().isAlive(), getTestTimeout()));

            assertEquals(0, proc.getProcess().exitValue());
        }
        finally {
            if (proc.getProcess().isAlive())
                proc.kill();
        }
    }

    /** */
    public static class TestRunner {
        /** */
        private static void doIgniteClientConnectionTest() {
            try (AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext()) {
                ctx.register(IgniteClientSpringTransactionManagerApplicationContext.class);
                ctx.refresh();

                GridSpringTransactionService svc = ctx.getBean(GridSpringTransactionService.class);

                IgniteClient cli = ctx.getBean(IgniteClient.class);

                ClientCache<Integer, String> cache = cli.cache(TEST_CACHE_NAME);

                cache.clear();

                IgniteCacheProxy<Integer, String> cacheProxy = new IgniteClientCacheProxy<>(cache);

                svc.put(cacheProxy, 1);

                assertThrowsWithCause(() -> svc.putWithError(cacheProxy, 1), NumberFormatException.class);

                assertEquals(1, cache.size());
            }
        }

        /** */
        private static void doIgniteNodeConnectionTest() {
            try (AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext()) {
                ctx.register(IgniteNodeSpringTransactionManagerApplicationContext.class);
                ctx.refresh();

                GridSpringTransactionService svc = ctx.getBean(GridSpringTransactionService.class);

                Ignite cli = ctx.getBean(Ignite.class);

                IgniteCache<Integer, String> cache = cli.cache(TEST_CACHE_NAME);

                cache.clear();

                IgniteCacheProxy<Integer, String> cacheProxy = new IgniteNodeCacheProxy<>(cache);

                svc.put(cacheProxy, 1);

                assertThrowsWithCause(() -> svc.putWithError(cacheProxy, 1), NumberFormatException.class);

                assertEquals(1, cache.size());
            }
        }

        /** */
        public static void main(String[] args) {
            doIgniteNodeConnectionTest();

            doIgniteClientConnectionTest();
        }

        /** */
        @Configuration
        @EnableTransactionManagement
        public static class IgniteNodeSpringTransactionManagerApplicationContext {
            /** */
            @Bean
            public GridSpringTransactionService transactionService() {
                return new GridSpringTransactionService();
            }

            /** */
            @Bean
            public Ignite ignite() {
                return Ignition.start("config/ignite-client-node.xml");
            }

            /** */
            @Bean
            public AbstractSpringTransactionManager transactionManager() {
                SpringTransactionManager mgr = new SpringTransactionManager();

                mgr.setIgniteInstanceName("ignite-client-node");

                return mgr;
            }
        }

        /** */
        @Configuration
        @EnableTransactionManagement
        public static class IgniteClientSpringTransactionManagerApplicationContext {
            /** */
            @Bean
            public GridSpringTransactionService transactionService() {
                return new GridSpringTransactionService();
            }

            /** */
            @Bean
            public IgniteClient igniteClient() {
                return Ignition.startClient(new ClientConfiguration()
                    .setAddresses("127.0.0.1:" + DFLT_PORT));
            }

            /** */
            @Bean
            public AbstractSpringTransactionManager transactionManager(IgniteClient cli) {
                IgniteClientSpringTransactionManager mgr = new IgniteClientSpringTransactionManager();

                mgr.setClientInstance(cli);

                return mgr;
            }
        }
    }

    /** */
    private static String toDependencyVersion(IgniteProductVersion ver) {
        return ver.equals(IgniteVersionUtils.VER)
            ? IgniteVersionUtils.VER_STR
            : ver.major() + "." + ver.minor() + "." + ver.maintenance();
    }
}

