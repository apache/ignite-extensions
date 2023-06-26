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

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.commandline.ArgumentParser;
import org.apache.ignite.internal.commandline.CommandHandler;
import org.apache.ignite.internal.commandline.CommandLogger;
import org.apache.ignite.internal.commandline.ConnectionAndSslParameters;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.logger.IgniteLoggerEx;
import org.apache.ignite.internal.management.IgniteCommandRegistry;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.util.GridCommandHandlerFactoryAbstractTest;
import org.jetbrains.annotations.Nullable;

import static javax.servlet.http.HttpServletResponse.SC_BAD_REQUEST;
import static javax.servlet.http.HttpServletResponse.SC_OK;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_REST_TCP_PORT;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_INVALID_ARGUMENTS;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_UNEXPECTED_ERROR;
import static org.apache.ignite.internal.commandline.CommandHandler.setupJavaLogger;
import static org.apache.ignite.internal.commandline.CommandLogger.errorMessage;
import static org.apache.ignite.internal.management.api.CommandUtils.isBoolean;
import static org.apache.ignite.internal.management.api.CommandUtils.visitCommandParams;
import static org.apache.ignite.internal.management.openapi.OpenApiCommandsRegistryInvokerPlugin.ATTR_OPENAPI_HOST;
import static org.apache.ignite.internal.management.openapi.OpenApiCommandsRegistryInvokerPlugin.ATTR_OPENAPI_PORT;
import static org.apache.ignite.internal.management.openapi.OpenApiCommandsRegistryInvokerPlugin.commandUri;
import static org.apache.ignite.internal.management.openapi.OpenApiCommandsRegistryInvokerPlugin.parameterName;

/** */
public class OpenApiInvoker implements GridCommandHandlerFactoryAbstractTest.TestCommandHandler {
    /** */
    private final IgniteLoggerEx log;

    /** */
    private int gridClientPort;

    /** */
    private IgniteEx ignite;

    private OkHttpClient cli = new OkHttpClient();

    /** */
    public OpenApiInvoker() {
        this(null);
    }

    /** */
    public OpenApiInvoker(@Nullable IgniteLogger log) {
        this.log = (IgniteLoggerEx)(log == null ? setupJavaLogger("rest-invoker", CommandHandler.class) : log);
    }

    /** {@inheritDoc} */
    @Override public int execute(List<String> value) {
        try {
            ArgumentParser parser = new ArgumentParser(log, new IgniteCommandRegistry());

            ConnectionAndSslParameters<IgniteDataTransferObject> p = parser.parseAndValidate(value);

            String url = cmdURL(p);

            Request req = new Request.Builder()
                .url(url)
                .build();

            try (Response resp = cli.newCall(req).execute()) {
                log.info(resp.body().string());

                if (resp.code() == SC_OK)
                    return EXIT_CODE_OK;
                else if (resp.code() == SC_BAD_REQUEST)
                    return EXIT_CODE_INVALID_ARGUMENTS;
                else
                    return EXIT_CODE_UNEXPECTED_ERROR;
            }
        }
        catch (IllegalArgumentException e) {
            log.error("Check arguments. " + errorMessage(e));
            log.info("Command [] finished with code: " + EXIT_CODE_INVALID_ARGUMENTS);

            return EXIT_CODE_INVALID_ARGUMENTS;
        }
        catch (Throwable e) {
            log.error("Failed to perform operation.");
            log.error(CommandLogger.errorMessage(e));

            return EXIT_CODE_UNEXPECTED_ERROR;
        }
    }

    private String cmdURL(ConnectionAndSslParameters<IgniteDataTransferObject> p) {
        StringBuilder url = new StringBuilder();

        OpenApiCommandsRegistryInvokerPluginConfiguration cfg = new OpenApiCommandsRegistryInvokerPluginConfiguration();

        IgniteEx ignite = ignite(p);

        Integer httpPort = (Integer)ignite.context().nodeAttribute(ATTR_OPENAPI_PORT);
        String httpHost = (String)ignite.context().nodeAttribute(ATTR_OPENAPI_HOST);

        url.append("http://").append(httpHost).append(':').append(httpPort)
            .append(cfg.getRootUri())
            .append(commandUri(new LinkedList<>(p.cmdPath()))).append('?');

        Map<String, String> params = new HashMap<>();

        Consumer<Field> fldCnsmr = fld -> params.put(parameterName(fld), toString(U.field(p.commandArg(), fld.getName())));

        visitCommandParams(p.command().argClass(), fldCnsmr, fldCnsmr, (optional, flds) -> flds.forEach(fldCnsmr));

        for (Map.Entry<String, String> e : params.entrySet())
            url.append(e.getKey()).append('=').append(e.getValue()).append('&');

        return url.toString();
    }

    /** */
    private IgniteEx ignite(ConnectionAndSslParameters<IgniteDataTransferObject> p) {
        int port = p.port();

        if (port == this.gridClientPort)
            return ignite;

        for (Ignite node : IgnitionEx.allGrids()) {
            Integer nodePort = ((IgniteEx)node).localNode().<Integer>attribute(ATTR_REST_TCP_PORT);

            if (nodePort != null && port == nodePort) {
                this.gridClientPort = port;

                ignite = (IgniteEx)node;

                return ignite;
            }
        }

        throw new IllegalStateException("Unknown grid for port: " + port);
    }

    /** */
    private static String toString(Object val) {
        if (val == null || (isBoolean(val.getClass()) && !(boolean)val))
            return "";

        if (val.getClass().isArray()) {
            int length = Array.getLength(val);

            if (length == 0)
                return "";

            StringBuffer sb = new StringBuffer();

            for (int i = 0; i < length; i++) {
                if (i != 0)
                    sb.append(',');

                sb.append(toString(Array.get(val, i)));
            }

            return sb.toString();
        }

        return Objects.toString(val);
    }


    /** {@inheritDoc} */
    @Override public <T> T getLastOperationResult() {
        return (T)ManagementApiServlet.res;
    }

    /** {@inheritDoc} */
    @Override public void flushLogger() {
        log.flush();
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return "rest";
    }
}
