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

import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.management.AbstractCommandInvoker;
import org.apache.ignite.internal.management.api.Command;
import org.apache.ignite.internal.util.typedef.F;

import javax.servlet.Servlet;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.apache.ignite.internal.management.openapi.OpenApiCommandsRegistryInvokerPlugin.TEXT_PLAIN;

/** */
public class ManagementApiServlet extends AbstractCommandInvoker implements Servlet {
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
    @Override public void service(ServletRequest req0, ServletResponse res0) throws ServletException, IOException {
        if (!(req0 instanceof HttpServletRequest))
            throw new IllegalArgumentException("Not http");

        HttpServletRequest req = (HttpServletRequest) req0;
        HttpServletResponse resp = (HttpServletResponse) res0;

        if (!"GET".equals(req.getMethod()))
            throw new IllegalArgumentException("Only GET requests supported");

        resp.setStatus(HttpServletResponse.SC_OK);
        resp.setContentType(TEXT_PLAIN);
        resp.setCharacterEncoding("UTF-8");

        String uri = req.getRequestURI();

        if (!uri.startsWith(root))
            throw new IllegalArgumentException("Wrong URI: " + uri);

        String[] cmdPath = uri.substring(root.length() + 1).split("/");

        if (cmdPath.length == 0)
            throw new IllegalArgumentException("Empty command path: " + uri);

        Command<?, ?, ?> cmd = command(Arrays.asList(cmdPath).iterator());

        Map<String, String> params = new HashMap<>();

        for (Map.Entry<String, String[]> e : req.getParameterMap().entrySet()) {
            if (F.isEmpty(e.getValue()))
                params.put(e.getKey(), "");
            else if (e.getValue().length == 1)
                params.put(e.getKey(), e.getValue()[0]);
            else
                throw new IllegalArgumentException("Array format is comma separated single parameter");
        }

        execute(cmd, params, resp.getWriter()::println);
    }

    /** {@inheritDoc} */
    @Override public IgniteEx grid() {
        return grid;
    }

    /** {@inheritDoc} */
    @Override public void init(ServletConfig config) throws ServletException {
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
