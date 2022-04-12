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

package org.apache.ignite.springdata.repository.support;

import java.util.Objects;
import java.util.stream.Stream;
import org.apache.ignite.Ignite;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.springdata.proxy.IgniteProxy;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.AbstractFactoryBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

/**
 * Represents factory for obtaining instance of {@link IgniteProxy} that provide client-independent connection to the
 * Ignite cluster.
 */
public class IgniteProxyFactory extends AbstractFactoryBean<IgniteProxy> implements ApplicationContextAware {
    /** Name of the bean that stores Ignite client instance to access the Ignite cluster. */
    private static final String IGNITE_INSTANCE_BEAN_NAME = "igniteInstance";

    /** Name of the bean that stores the configuration to instantiate the Ignite client. */
    private static final String IGNITE_CONFIG_BEAN_NAME = "igniteCfg";

    /** Name of the bean that stores the Spring configuration path to instantiate the Ignite client. */
    private static final String IGNITE_SPRING_CONFIG_PATH_BEAN_NAME = "igniteSpringCfgPath";

    /** Spring application context. */
    private ApplicationContext ctx;

    /** {@inheritDoc} */
    @Override public void setApplicationContext(ApplicationContext ctx) throws BeansException {
        this.ctx = ctx;
    }

    /** {@inheritDoc} */
    @Override public void destroy() throws Exception {
        Object igniteProxy = getObject();

        if (igniteProxy instanceof AutoCloseable)
            ((AutoCloseable)igniteProxy).close();
    }

    /** {@inheritDoc} */
    @Override public Class<?> getObjectType() {
        return IgniteProxy.class;
    }

    /** {@inheritDoc} */
    @Override protected IgniteProxy createInstance() {
        return Stream.<BeanFinder>of(
            () -> ctx.getBean(IGNITE_INSTANCE_BEAN_NAME),
            () -> ctx.getBean(IGNITE_CONFIG_BEAN_NAME),
            () -> ctx.getBean(IGNITE_SPRING_CONFIG_PATH_BEAN_NAME, String.class),
            () -> ctx.getBean(Ignite.class),
            () -> ctx.getBean(IgniteClient.class),
            () -> ctx.getBean(IgniteConfiguration.class),
            () -> ctx.getBean(ClientConfiguration.class)
        ).map(BeanFinder::getBean)
            .filter(Objects::nonNull)
            .findFirst()
            .map(IgniteProxy::of)
            .orElseThrow(() -> {
                return new IllegalArgumentException("No beans were found that provide connection configuration to" +
                    " the Ignite cluster. One of the beans with the following names is required : \"" +
                    IGNITE_INSTANCE_BEAN_NAME + "\", \"" + IGNITE_CONFIG_BEAN_NAME + "\" or \"" +
                    IGNITE_SPRING_CONFIG_PATH_BEAN_NAME + "\". You can also provide Ignite, IgniteClient," +
                    " IgniteConfiguration or ClientConfiguration bean to application context.");
            });
    }

    /**
     * Helper interface that wraps getBean method.
     */
    @FunctionalInterface
    private interface BeanFinder {
        /**
         * Get bean.
         * @return Bean or null if {@link BeansException} was thrown.
         */
        default Object getBean() {
            try {
                return get();
            } catch (BeansException ex) {
                return null;
            }
        }

        /**
         * Get bean.
         * @return Bean.
         * @throws BeansException If bean was not found.
         */
        Object get() throws BeansException;
    }
}
