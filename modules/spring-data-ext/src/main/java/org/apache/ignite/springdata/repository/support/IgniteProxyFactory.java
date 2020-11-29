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
import org.apache.ignite.springdata.proxy.ClosableIgniteClientProxy;
import org.apache.ignite.springdata.proxy.ClosableIgniteProxy;
import org.apache.ignite.springdata.proxy.IgniteClientProxy;
import org.apache.ignite.springdata.proxy.IgniteProxy;
import org.apache.ignite.springdata.proxy.IgniteProxyImpl;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

/**
 * Represents factory for obtaining instances of {@link IgniteProxy} that provide client-independent connection to the
 * Ignite cluster.
 */
public class IgniteProxyFactory implements ApplicationContextAware {
    /** Spring application context. */
    private ApplicationContext ctx;

    /** @return {@link IgniteProxy} instance. */
    public IgniteProxy igniteProxy() {
        try {
            Object igniteInstanceBean = ctx.getBean("igniteInstance");

            if (igniteInstanceBean instanceof Ignite)
                return new IgniteProxyImpl((Ignite)igniteInstanceBean);
            else if (igniteInstanceBean instanceof IgniteClient)
                return new IgniteClientProxy((IgniteClient)igniteInstanceBean);

            throw new IllegalArgumentException("The Spring Bean with name \"igniteInstance\" must be one of the" +
                " following types: [" + Ignite.class.getName() + ", " + IgniteClient.class.getName() + ']');
        }
        catch (BeansException ex) {
            try {
                Object igniteCfgBean = ctx.getBean("igniteCfg");

                if (igniteCfgBean instanceof IgniteConfiguration) {
                    try {
                        return new IgniteProxyImpl(Ignition.ignite(
                            ((IgniteConfiguration)igniteCfgBean).getIgniteInstanceName()));
                    }
                    catch (Exception ignored) {
                        // No-op.
                    }

                    return new ClosableIgniteProxy(Ignition.start((IgniteConfiguration)igniteCfgBean));
                }
                else if (igniteCfgBean instanceof ClientConfiguration)
                    return new ClosableIgniteClientProxy(Ignition.startClient((ClientConfiguration)igniteCfgBean));

                throw new IllegalArgumentException("The Spring Bean with name \"igniteCfg\" must be one of the" +
                    " following types: [" + IgniteConfiguration.class.getName() + ", " +
                    ClientConfiguration.class.getName() + ']');
            }
            catch (BeansException ex2) {
                try {
                    String igniteCfgPath = ctx.getBean("igniteSpringCfgPath", String.class);

                    return new ClosableIgniteProxy(Ignition.start(igniteCfgPath));
                }
                catch (BeansException ex3) {
                    throw new IllegalArgumentException("No beans were found that provide connection configuration to" +
                        " the Ignite cluster. One of the following beans are required : \"igniteInstance\"," +
                        " \"igniteCfg\" or \"igniteSpringCfgPath\".");
                }
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void setApplicationContext(ApplicationContext ctx) throws BeansException {
        this.ctx = ctx;
    }
}
