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
package org.apache.ignite.springdata.repository.config;

import java.util.Collection;
import java.util.Collections;
import org.apache.ignite.springdata.proxy.IgniteProxy;
import org.apache.ignite.springdata.repository.IgniteRepository;
import org.apache.ignite.springdata.repository.support.IgniteRepositoryFactoryBean;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.data.repository.config.AnnotationRepositoryConfigurationSource;
import org.springframework.data.repository.config.RepositoryConfigurationExtension;
import org.springframework.data.repository.config.RepositoryConfigurationExtensionSupport;
import org.springframework.data.repository.config.RepositoryConfigurationSource;

/**
 * Apache Ignite specific implementation of {@link RepositoryConfigurationExtension}.
 */
public class IgniteRepositoryConfigurationExtension extends RepositoryConfigurationExtensionSupport {
    /** Name of the auto-registered Ignite proxy factory bean. */
    private static final String IGNITE_PROXY_FACTORY_BEAN_NAME = "igniteResourceProvider";

    /** Name of the auto-registered Ignite proxy bean. */
    private static final String IGNITE_PROXY_BEAN_NAME = "igniteProxy";

    /** {@inheritDoc} */
    @Override public String getModuleName() {
        return "Apache Ignite";
    }

    /** {@inheritDoc} */
    @Override protected String getModulePrefix() {
        return "ignite";
    }

    /** {@inheritDoc} */
    @Override public String getRepositoryFactoryClassName() {
        return IgniteRepositoryFactoryBean.class.getName();
    }

    /** {@inheritDoc} */
    @Override protected Collection<Class<?>> getIdentifyingTypes() {
        return Collections.<Class<?>>singleton(IgniteRepository.class);
    }

    /** {@inheritDoc} */
    @Override public void registerBeansForRoot(BeanDefinitionRegistry registry, RepositoryConfigurationSource cfg) {
        Class<?> igniteProxyFactoryCls = ((AnnotationRepositoryConfigurationSource)cfg).getAttributes()
            .getClass("igniteProxyFactoryClass");

        registerIfNotAlreadyRegistered(
            BeanDefinitionBuilder.genericBeanDefinition(igniteProxyFactoryCls).getBeanDefinition(),
            registry,
            IGNITE_PROXY_FACTORY_BEAN_NAME,
            cfg);

        registerIfNotAlreadyRegistered(
            BeanDefinitionBuilder.genericBeanDefinition(IgniteProxy.class)
                .setFactoryMethodOnBean("igniteProxy", IGNITE_PROXY_FACTORY_BEAN_NAME)
                .getBeanDefinition(),
            registry,
            IGNITE_PROXY_BEAN_NAME,
            cfg);
    }
}
