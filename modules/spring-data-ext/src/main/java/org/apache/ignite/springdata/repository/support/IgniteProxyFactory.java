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

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.springdata.proxy.IgniteClientProxy;
import org.apache.ignite.springdata.proxy.IgniteProxy;
import org.apache.ignite.springdata.proxy.IgniteProxyImpl;
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
    private static final String IGNITE_SPRING_CONFIG_PATH_BEAN_NAME= "igniteSpringCfgPath";

    /** Spring application context. */
    private ApplicationContext ctx;

    /** {@inheritDoc} */
    @Override public void setApplicationContext(ApplicationContext ctx) throws BeansException {
        this.ctx = ctx;
    }

    /** {@inheritDoc} */
    @Override public void destroy() throws Exception {
        getObject().close();
    }

    /** {@inheritDoc} */
    @Override public Class<?> getObjectType() {
        return IgniteProxy.class;
    }

    /** {@inheritDoc} */
    @Override protected IgniteProxy createInstance() {
        try {
            Object igniteInstanceBean = ctx.getBean(IGNITE_INSTANCE_BEAN_NAME);

            if (igniteInstanceBean instanceof Ignite)
                return new IgniteProxyImpl((Ignite)igniteInstanceBean);
            else if (igniteInstanceBean instanceof IgniteClient)
                return new IgniteClientProxy((IgniteClient)igniteInstanceBean);

            throw new IllegalArgumentException("The Spring Bean with name \"" + IGNITE_INSTANCE_BEAN_NAME +
                "\" must be one of the following types: [" + Ignite.class.getName() + ", " +
                IgniteClient.class.getName() + ']');
        }
        catch (BeansException ex) {
            try {
                Object igniteCfgBean = ctx.getBean(IGNITE_CONFIG_BEAN_NAME);

                if (igniteCfgBean instanceof IgniteConfiguration) {
                    try {
                        return new IgniteProxyImpl(Ignition.ignite(
                            ((IgniteConfiguration)igniteCfgBean).getIgniteInstanceName()));
                    }
                    catch (Exception ignored) {
                        // No-op.
                    }

                    return new IgniteProxyImpl(Ignition.start((IgniteConfiguration)igniteCfgBean));
                }
                else if (igniteCfgBean instanceof ClientConfiguration)
                    return new IgniteClientProxy(Ignition.startClient((ClientConfiguration)igniteCfgBean));

                throw new IllegalArgumentException("The Spring Bean with name \"" + IGNITE_CONFIG_BEAN_NAME +
                    "\" must be one of the following types: [" + IgniteConfiguration.class.getName() + ", " +
                    ClientConfiguration.class.getName() + ']');
            }
            catch (BeansException ex2) {
                try {
                    String igniteCfgPath = ctx.getBean(IGNITE_SPRING_CONFIG_PATH_BEAN_NAME, String.class);

                    return new IgniteProxyImpl(Ignition.start(igniteCfgPath));
                }
                catch (BeansException ex3) {
                    throw new IllegalArgumentException("No beans were found that provide connection configuration to" +
                        " the Ignite cluster. One of the following beans are required : \"" +
                        IGNITE_INSTANCE_BEAN_NAME + "\", \"" + IGNITE_CONFIG_BEAN_NAME + "\" or \"" +
                        IGNITE_SPRING_CONFIG_PATH_BEAN_NAME + "\".");
                }
            }
        }
    }
}
