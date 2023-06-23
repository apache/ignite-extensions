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

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import javax.servlet.Servlet;
import javax.servlet.ServletConfig;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.management.api.Command;
import org.apache.ignite.internal.management.api.CommandUtils;
import org.apache.ignite.internal.management.api.CommandsRegistry;

import static javax.servlet.http.HttpServletResponse.SC_INTERNAL_SERVER_ERROR;
import static javax.servlet.http.HttpServletResponse.SC_NOT_FOUND;
import static javax.servlet.http.HttpServletResponse.SC_OK;
import static org.apache.ignite.internal.management.openapi.OpenApiCommandsRegistryInvokerPlugin.TEXT_PLAIN;

/** */
public class ManagementApiServlet implements Servlet {
    /** */
    private final IgniteEx grid;

    /** */
    private final String root;

    /** */
    public ManagementApiServlet(IgniteEx grid, String root) {
        this.grid = grid;
        this.root = root;
    }

    /** {@inheritDoc} */
    @Override public void service(ServletRequest req0, ServletResponse res0) throws IOException {
        if (!(req0 instanceof HttpServletRequest))
            throw new IllegalArgumentException("Not http");

        HttpServletRequest req = (HttpServletRequest)req0;
        HttpServletResponse resp = (HttpServletResponse)res0;

        if (!"GET".equals(req.getMethod()))
            throw new IllegalArgumentException("Only GET requests supported");

        String uri = req.getRequestURI();

        if (!uri.startsWith(root))
            throw new IllegalArgumentException("Wrong URI: " + uri);

        String cmdPath = uri.substring(root.length() + 1);

        if (cmdPath.length() == 0)
            throw new IllegalArgumentException("Empty command path: " + uri);

        Iterator<String> iter = Arrays.asList(cmdPath.split("/")).iterator();

        if (!iter.hasNext()) {
            respondWithError("Empty command", SC_INTERNAL_SERVER_ERROR, resp);

            return;
        }

        Command<?, ?> cmd = grid.commandsRegistry();

        while (iter.hasNext()) {
            cmd = ((CommandsRegistry<?, ?>)cmd).command(iter.next());

            if (cmd == null) {
                respondWithError("Unknown command", SC_NOT_FOUND, resp);

                return;
            }
        }

        if (!CommandUtils.executable(cmd)) {
            respondWithError("Command can't be execute", SC_INTERNAL_SERVER_ERROR, resp);

            return;
        }

        resp.setStatus(SC_OK);
        resp.setContentType(TEXT_PLAIN);
        resp.setCharacterEncoding("UTF-8");

        resp.getWriter().println("Hello, world!");

/*
        for (Map.Entry<String, String[]> e : req.getParameterMap().entrySet()) {
            if (F.isEmpty(e.getValue()))
                params.put(e.getKey(), "");
            else if (e.getValue().length == 1)
                params.put(e.getKey(), e.getValue()[0]);
            else
                throw new IllegalArgumentException("Array format is comma separated single parameter");
        }
*/

        //execute(cmd, params, resp.getWriter()::println);
    }

    /** */
    private static void respondWithError(String msg, int status, HttpServletResponse resp) throws IOException {
        resp.setStatus(status);
        resp.setContentType(TEXT_PLAIN);
        resp.setCharacterEncoding("UTF-8");

        resp.getWriter().print(msg);
    }

    /** {@inheritDoc} */
    @Override public void init(ServletConfig config) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public ServletConfig getServletConfig() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public String getServletInfo() {
        return ManagementApiServlet.class.getSimpleName();
    }

    /** {@inheritDoc} */
    @Override public void destroy() {
        // No-op.
    }
}
