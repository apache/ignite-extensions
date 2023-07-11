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

package org.apache.ignite.internal.management.openapi;

import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletMapping;
import org.junit.Test;

import static org.apache.ignite.internal.management.openapi.OpenApiCommandsInvokerPlugin.ATTR_OPENAPI_HOST;
import static org.apache.ignite.internal.management.openapi.OpenApiCommandsInvokerPlugin.ATTR_OPENAPI_PORT;
import static org.apache.ignite.internal.management.openapi.OpenApiCommandsInvokerPluginConfiguration.DFLT_HOST;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;

/** */
public class OpenApiCommandsInvokerPluginSelfTest extends GridCommonAbstractTest {
    /** */
    private int port = OpenApiCommandsInvokerPluginConfiguration.DFLT_PORT;

    /** */
    private int portRange = OpenApiCommandsInvokerPluginConfiguration.DFLT_PORT_RANGE;

    /** */
    private String rootUri = OpenApiCommandsInvokerPluginConfiguration.DFLT_ROOT_URI;

    /** */
    private Server srv = null;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        OpenApiCommandsInvokerPluginProvider provider = new OpenApiCommandsInvokerPluginProvider();

        OpenApiCommandsInvokerPluginConfiguration cfg = new OpenApiCommandsInvokerPluginConfiguration();

        cfg.setPort(port);
        cfg.setPortRange(portRange);
        cfg.setRootUri(rootUri);
        cfg.setServer(srv);

        provider.setConfig(cfg);

        return super.getConfiguration(igniteInstanceName).setPluginProviders(provider);
    }

    /** */
    @Test
    public void testNonDefaultConfig() throws Exception {
        port = 8081;
        rootUri = "/mapi";

        try (IgniteEx ignite = startGrid(0)) {
            assertEquals(port, ignite.context().nodeAttribute(ATTR_OPENAPI_PORT));
            assertEquals(DFLT_HOST, ignite.context().nodeAttribute(ATTR_OPENAPI_HOST));

            OpenApiCommandsInvokerPlugin plugin = plugin(ignite);

            OpenApiCommandsInvokerPluginConfiguration cfg = GridTestUtils.getFieldValue(plugin, "cfg");

            assertEquals(rootUri, cfg.getRootUri());
            assertEquals(port, cfg.getPort());

            ServletContextHandler hnd = (ServletContextHandler)GridTestUtils.<Server>getFieldValue(plugin, "srv").getHandler();

            ServletMapping mapping = hnd.getServletHandler().getServletMapping(rootUri + "/*");

            assertNotNull(mapping);
            assertEquals(OpenApiCommandsInvokerPlugin.INVOKER_SERVLET, mapping.getServletName());
        }
    }

    /** */
    @Test
    public void testPortRange() throws Exception {
        portRange = 1;

        try (IgniteEx ignite0 = startGrid(0);
             IgniteEx ignite1 = startGrid(1)) {
            assertEquals(port, ignite0.context().nodeAttribute(ATTR_OPENAPI_PORT));
            assertEquals(DFLT_HOST, ignite1.context().nodeAttribute(ATTR_OPENAPI_HOST));

            assertEquals(port + 1, ignite1.context().nodeAttribute(ATTR_OPENAPI_PORT));
            assertEquals(DFLT_HOST, ignite1.context().nodeAttribute(ATTR_OPENAPI_HOST));

            assertThrowsWithCause(() -> startGrid(2), IgniteException.class);
        }
    }


    /** */
    @Test
    public void testCustomServer() throws Exception {
        srv = new Server();

        ServerConnector connector = new ServerConnector(srv);

        int customPort = 9090;

        connector.setPort(customPort);
        connector.setHost(DFLT_HOST);

        srv.addConnector(connector);

        try (IgniteEx ignite = startGrid(0)) {
            assertEquals(customPort, ignite.context().nodeAttribute(ATTR_OPENAPI_PORT));
            assertEquals(DFLT_HOST, ignite.context().nodeAttribute(ATTR_OPENAPI_HOST));

            OpenApiCommandsInvokerPlugin plugin = plugin(ignite);

            assertSame(srv, GridTestUtils.getFieldValue(plugin, "srv"));
        }
    }

    /** */
    private static OpenApiCommandsInvokerPlugin plugin(IgniteEx ignite) {
        return GridTestUtils.getFieldValue(
            ignite.context().plugins().pluginProvider(OpenApiCommandsInvokerPluginProvider.NAME),
            "plugin"
        );
    }
}
