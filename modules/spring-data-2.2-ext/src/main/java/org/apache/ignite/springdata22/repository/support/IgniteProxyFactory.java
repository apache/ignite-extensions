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

package org.apache.ignite.springdata22.repository.support;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.springdata.proxy.IgniteClientProxy;
import org.apache.ignite.springdata.proxy.IgniteProxy;
import org.apache.ignite.springdata.proxy.IgniteProxyImpl;
import org.apache.ignite.springdata22.repository.config.RepositoryConfig;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.config.BeanExpressionContext;
import org.springframework.beans.factory.config.BeanExpressionResolver;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.expression.StandardBeanExpressionResolver;

import static org.apache.ignite.springdata22.repository.support.IgniteRepositoryFactory.getRepositoryConfiguration;

/**
 * Represents factory for obtaining instances of {@link IgniteProxy} that provide client-independent connection to the
 * Ignite cluster.
 */
public class IgniteProxyFactory implements ApplicationContextAware, DisposableBean {
    /** Spring application expression resolver. */
    private final BeanExpressionResolver expressionResolver = new StandardBeanExpressionResolver();

    /** Repositories associated with Ignite proxy. */
    private final Map<Class<?>, IgniteProxy> igniteProxies = new ConcurrentHashMap<>();

    /** Spring application context. */
    private ApplicationContext ctx;

    /** Spring application bean expression context. */
    private BeanExpressionContext beanExpressionCtx;

    /**
     * @param repoInterface The repository interface class for which {@link IgniteProxy} will be created.
     * @return {@link IgniteProxy} instance.
     */
   public IgniteProxy igniteProxy(Class<?> repoInterface) {
        return igniteProxies.computeIfAbsent(repoInterface, k -> createIgniteProxy(repoInterface));
    }

    /** {@inheritDoc} */
    @Override public void setApplicationContext(ApplicationContext ctx) throws BeansException {
        this.ctx = ctx;

        beanExpressionCtx = new BeanExpressionContext(
            new DefaultListableBeanFactory(ctx.getAutowireCapableBeanFactory()),
            null);
    }

    /** {@inheritDoc} */
    @Override public void destroy() throws Exception {
        Set<IgniteProxy> proxies = new HashSet<>(igniteProxies.values());

        Exception destroyE = null;

        for (IgniteProxy proxy : proxies) {
            try {
                proxy.close();
            }
            catch (Exception e) {
                if (destroyE == null)
                    destroyE = e;
                else
                    destroyE.addSuppressed(e);
            }
        }

        if (destroyE != null)
            throw destroyE;
    }

    /**
     * Creates {@link IgniteProxy} to be used for providing access to the Ignite cluster for specified repository.
     *
     * @param repoInterface {@link Class} instance of the repository interface.
     * @return Instance of {@link IgniteProxy} associated with specified repository.
     *
     * @see RepositoryConfig
     */
    private IgniteProxy createIgniteProxy(Class<?> repoInterface) {
        RepositoryConfig repoCfg = getRepositoryConfiguration(repoInterface);

        try {
            Object igniteInstanceBean = ctx.getBean(evaluateExpression(repoCfg.igniteInstance()));

            if (igniteInstanceBean instanceof Ignite)
                return new IgniteProxyImpl((Ignite)igniteInstanceBean);
            else if (igniteInstanceBean instanceof IgniteClient)
                return new IgniteClientProxy((IgniteClient)igniteInstanceBean);

            throw new IllegalArgumentException("Invalid configuration for repository " + repoInterface.getName() +
                ". The Spring Bean corresponding to the \"igniteInstance\" property of repository configuration must" +
                " be one of the following types: [" + Ignite.class.getName() + ", " + IgniteClient.class.getName() +
                ']');
        }
        catch (BeansException ex) {
            try {
                Object igniteCfgBean = ctx.getBean(evaluateExpression(repoCfg.igniteCfg()));

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

                throw new IllegalArgumentException("Invalid configuration for repository " + repoInterface.getName() +
                    ". The Spring Bean corresponding to the \"igniteCfg\" property of repository configuration must" +
                    " be one of the following types: [" + IgniteConfiguration.class.getName() + ", " +
                    ClientConfiguration.class.getName() + ']');
            }
            catch (BeansException ex2) {
                try {
                    String igniteCfgPath = ctx.getBean(evaluateExpression(repoCfg.igniteSpringCfgPath()), String.class);

                    return new IgniteProxyImpl(Ignition.start(igniteCfgPath));
                }
                catch (BeansException ex3) {
                    throw new IllegalArgumentException("Invalid configuration for repository " +
                        repoInterface.getName() + ". No beans were found that provide connection configuration to the" +
                        " Ignite cluster. Check \"igniteInstance\", \"igniteCfg\", \"igniteSpringCfgPath\" parameters" +
                        " of " + RepositoryConfig.class.getName() + " repository annotation.");
                }
            }
        }
    }

    /**
     * Evaluates the SpEL expression.
     *
     * @param spelExpression SpEL expression
     * @return The result of evaluation of the SpEL expression.
     */
    private String evaluateExpression(String spelExpression) {
        return (String)expressionResolver.evaluate(spelExpression, beanExpressionCtx);
    }
}
