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
import java.io.PrintWriter;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Iterator;
import java.util.function.Function;
import javax.servlet.Servlet;
import javax.servlet.ServletConfig;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.client.GridClientException;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.management.api.Command;
import org.apache.ignite.internal.management.api.CommandInvoker;
import org.apache.ignite.internal.management.api.CommandUtils;
import org.apache.ignite.internal.management.api.CommandsRegistry;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;

import static javax.servlet.http.HttpServletResponse.SC_BAD_REQUEST;
import static javax.servlet.http.HttpServletResponse.SC_INTERNAL_SERVER_ERROR;
import static javax.servlet.http.HttpServletResponse.SC_NOT_FOUND;
import static javax.servlet.http.HttpServletResponse.SC_OK;
import static org.apache.ignite.internal.management.api.CommandUtils.CMD_WORDS_DELIM;
import static org.apache.ignite.internal.management.api.CommandUtils.fromFormattedCommandName;
import static org.apache.ignite.internal.management.openapi.OpenApiCommandsRegistryInvokerPlugin.TEXT_PLAIN;
import static org.apache.ignite.internal.management.openapi.OpenApiCommandsRegistryInvokerPlugin.parameterName;

/** */
public class ManagementApiServlet implements Servlet {
    /** */
    private final IgniteEx ignite;

    /** */
    private final String root;

    /** */
    static Object res;

    /** */
    public ManagementApiServlet(IgniteEx grid, String root) {
        this.ignite = grid;
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

        Command<?, ?> cmd = ignite.commandsRegistry();

        while (iter.hasNext()) {
            cmd = ((CommandsRegistry<?, ?>)cmd).command(fromFormattedCommandName(iter.next(), CMD_WORDS_DELIM));

            if (cmd == null) {
                respondWithError("Unknown command", SC_NOT_FOUND, resp);

                return;
            }
        }

        if (!CommandUtils.executable(cmd)) {
            respondWithError("Command can't be execute", SC_INTERNAL_SERVER_ERROR, resp);

            return;
        }

        try {
            invoke(cmd, req, resp);
        }
        catch (Throwable e) {
            commonResponse(resp, X.hasCause(e, IllegalArgumentException.class) ? SC_BAD_REQUEST : SC_INTERNAL_SERVER_ERROR);

            PrintWriter writer = resp.getWriter();

            writer.println("Failed to perform operation.");
            writer.println(errorMessage(e));

            if (e instanceof IllegalArgumentException)
                writer.println("Check arguments. " + errorMessage(e));
        }
    }

    /** */
    private <A extends IgniteDataTransferObject> void invoke(
        Command<A, ?> cmd,
        HttpServletRequest req,
        HttpServletResponse resp
    ) throws IOException, GridClientException {
        CommandInvoker<A> invoker = new CommandInvoker<>(cmd, new ParamsToArgument<A>(cmd.argClass(), req).argument(), ignite);

        if (invoker.prepare(resp.getWriter()::println))
            res = invoker.invoke(resp.getWriter()::println, false);

        commonResponse(resp, SC_OK);
    }

    /** */
    private static void respondWithError(String msg, int status, HttpServletResponse resp) throws IOException {
        commonResponse(resp, status);

        resp.getWriter().print(msg);
    }

    /** */
    private static void commonResponse(HttpServletResponse resp, int status) {
        resp.setStatus(status);
        resp.setContentType(TEXT_PLAIN);
        resp.setCharacterEncoding("UTF-8");
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

    /** */
    private static class ParamsToArgument<A extends IgniteDataTransferObject> implements Function<Field, Object> {
        /** */
        private final HttpServletRequest req;

        /** */
        private final Class<? extends A> argCls;

        /** */
        private ParamsToArgument(Class<? extends A> argCls, HttpServletRequest req) {
            this.argCls = argCls;
            this.req = req;
        }

        /** */
        public A argument() {
            // This will map vals to argument fields.
            return CommandUtils.argument(argCls, (fld, pos) -> apply(fld), this);
        }

        /** {@inheritDoc} */
        @Override public Object apply(Field fld) {
            String val = req.getParameter(parameterName(fld));

            return !F.isEmpty(val) ? CommandUtils.parseVal(val, fld.getType()) : null;
        }
    }

    /**
     * Generates readable error message from exception
     * @param e Exctption
     * @return error message
     */
    public static String errorMessage(Throwable e) {
        String msg = e.getMessage();

        if (F.isEmpty(msg))
            msg = e.getClass().getName();
        else if (msg.startsWith("Failed to handle request")) {
            int p = msg.indexOf("err=");

            msg = msg.substring(p + 4, msg.length() - 1);
        }

        return msg;
    }
}
