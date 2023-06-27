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

import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.plugin.PluginConfiguration;
import org.eclipse.jetty.server.Server;

/** */
public class OpenApiCommandsRegistryInvokerPluginConfiguration implements PluginConfiguration {
    /** Default port to bind to. */
    public static final int DFLT_PORT = 8080;

    /**
     * Default port range.
     * {@code 0} means that plugin will not try to use other ports to bind web server in case default port already used.
     */
    public static int DFLT_PORT_RANGE = 10;

    /** Default host. */
    public static final String DFLT_HOST = "localhost";

    /** */
    public static final String DFLT_ROOT_URI = "/management";

    /** Port to bind endpoint to. */
    private int port = DFLT_PORT;

    /** Port range to try to bind if {@link #port} already used. */
    private int portRange = DFLT_PORT_RANGE;

    /** Host to bind endpoint to. */
    private String host = DFLT_HOST;

    /** Root URI. */
    private String rootUri = DFLT_ROOT_URI;

    /** Preconfigured jetty server to use if custom setup required: SSL, etc. */
    private Server server;

    /** */
    public int getPort() {
        return port;
    }

    /** */
    public void setPort(int port) {
        this.port = port;
    }

    /** */
    public String getHost() {
        return host;
    }

    /** */
    public void setHost(String host) {
        this.host = host;
    }

    /** */
    public int getPortRange() {
        return portRange;
    }

    /** */
    public void setPortRange(int portRange) {
        this.portRange = portRange;
    }

    /** */
    public String getRootUri() {
        return rootUri;
    }

    /** */
    public void setRootUri(String rootUri) {
        this.rootUri = rootUri;
    }

    /** */
    public Server getServer() {
        return server;
    }

    /** */
    public void setServer(Server server) {
        this.server = server;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(OpenApiCommandsRegistryInvokerPluginConfiguration.class, this);
    }
}
