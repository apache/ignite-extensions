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

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;
import io.swagger.v3.jaxrs2.integration.OpenApiServlet;
import io.swagger.v3.oas.integration.GenericOpenApiContextBuilder;
import io.swagger.v3.oas.integration.OpenApiContextLocator;
import io.swagger.v3.oas.integration.SwaggerConfiguration;
import io.swagger.v3.oas.integration.api.OpenApiContext;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.Operation;
import io.swagger.v3.oas.models.PathItem;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.media.ArraySchema;
import io.swagger.v3.oas.models.media.BooleanSchema;
import io.swagger.v3.oas.models.media.Content;
import io.swagger.v3.oas.models.media.IntegerSchema;
import io.swagger.v3.oas.models.media.MediaType;
import io.swagger.v3.oas.models.media.NumberSchema;
import io.swagger.v3.oas.models.media.Schema;
import io.swagger.v3.oas.models.media.StringSchema;
import io.swagger.v3.oas.models.parameters.Parameter;
import io.swagger.v3.oas.models.responses.ApiResponse;
import io.swagger.v3.oas.models.responses.ApiResponses;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteVersionUtils;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.management.api.Argument;
import org.apache.ignite.internal.management.api.Command;
import org.apache.ignite.internal.management.api.CommandUtils;
import org.apache.ignite.internal.management.api.CommandsRegistry;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.IgnitePlugin;
import org.apache.ignite.plugin.PluginContext;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

import static io.swagger.v3.oas.integration.api.OpenApiContext.OPENAPI_CONTEXT_ID_KEY;
import static io.swagger.v3.oas.models.parameters.Parameter.StyleEnum.SIMPLE;
import static java.util.Collections.singletonList;
import static javax.servlet.DispatcherType.REQUEST;
import static javax.servlet.http.HttpServletResponse.SC_INTERNAL_SERVER_ERROR;
import static javax.servlet.http.HttpServletResponse.SC_OK;
import static org.apache.ignite.internal.management.api.CommandUtils.CMD_WORDS_DELIM;
import static org.apache.ignite.internal.management.api.CommandUtils.executable;
import static org.apache.ignite.internal.management.api.CommandUtils.toFormattedCommandName;
import static org.apache.ignite.internal.management.api.CommandUtils.toFormattedFieldName;
import static org.apache.ignite.internal.management.api.CommandUtils.valueExample;
import static org.apache.ignite.internal.management.api.CommandUtils.visitCommandParams;

/**
 *
 */
public class OpenApiCommandsRegistryInvokerPlugin implements IgnitePlugin {
    /** */
    public static final String API_CTX_ID = "ignite-management-api-ctx";

    /** */
    public static final String API_ID = "ignite-management-api";

    /** */
    public static final String INVOKER_BASE_URL = "/management";

    /** */
    public static final String TEXT_PLAIN = "text/plain";

    /** */
    private PluginContext ctx;

    /** */
    private IgniteLogger log;

    /** */
    private IgniteEx grid;

    /** */
    private Server srv;

    /** */
    private final OpenAPI api = new OpenAPI();

    /** */
    public void context(PluginContext ctx) {
        this.ctx = ctx;
        grid = (IgniteEx)ctx.grid();
        log = ctx.log(OpenApiCommandsRegistryInvokerPlugin.class);
    }

    /** */
    public void onIgniteStart() {
        // TODO: make this configurable.
        int port = 8080;
        String host = "localhost";
        String protocol = "http";

        api.info(new Info()
            .title("Ignite Management API")
            .description("This endpoint expose Apache Ignite management API commands")
            .version(IgniteVersionUtils.VER_STR)
        ).servers(singletonList(
            new io.swagger.v3.oas.models.servers.Server()
                .url(protocol + "://" + host + ":" + port + INVOKER_BASE_URL)
                .description("Ignite node[id=" + grid.localNode().id() + ']')
        ));

        grid.commandsRegistry().commands().forEachRemaining(cmd -> register(cmd.getKey(), new LinkedList<>(), cmd.getValue()));

        try {
            SwaggerConfiguration cfg = new SwaggerConfiguration();

            cfg.setId(API_ID);
            cfg.setOpenAPI(api);
            cfg.prettyPrint(true);

            OpenApiContext ctx = new GenericOpenApiContextBuilder<>()
                .ctxId(API_CTX_ID)
                .openApiConfiguration(cfg)
                .buildContext(true);

            OpenApiContextLocator.getInstance().putOpenApiContext(API_CTX_ID, ctx);

            srv = new Server();

            ServerConnector connector = new ServerConnector(srv);

            connector.setPort(port);
            connector.setHost(host);

            srv.addConnector(connector);

            ServletContextHandler handler = new ServletContextHandler();

            ServletHolder apiExposeServlet = new ServletHolder(OpenApiServlet.class);

            apiExposeServlet.setInitParameter(OPENAPI_CONTEXT_ID_KEY, API_CTX_ID);

            handler.addServlet(apiExposeServlet, "/api/*");
            handler.addServlet(
                new ServletHolder(new ManagementApiServlet(grid, INVOKER_BASE_URL)),
                INVOKER_BASE_URL + "/*"
            );
            handler.addFilter(HeaderFilter.class, "/*", EnumSet.of(REQUEST));

            srv.setHandler(handler);

            srv.start();
        }
        catch (Exception e) {
            throw new IgniteException(e);
        }
    }

    /** */
    public <A extends IgniteDataTransferObject> void register(String name, List<String> parents, Command<A, ?> cmd) {
        if (cmd instanceof CommandsRegistry) {
            parents.add(toFormattedCommandName(cmd.getClass(), CMD_WORDS_DELIM));

            ((CommandsRegistry<?, ?>)cmd).commands().forEachRemaining(cmd0 -> register(cmd0.getKey(), parents, cmd0.getValue()));

            parents.remove(parents.size() - 1);

            if (!executable(cmd))
                return;
        }

        StringBuilder path = new StringBuilder();

        for (String parent : parents)
            path.append('/').append(parent);

        path.append('/').append(toFormattedCommandName(name.getClass()));

        List<Parameter> params = new ArrayList<>();

        Consumer<Field> fldCnsmr = fld -> params.add(new Parameter()
            .style(SIMPLE)
            .in("query")
            .name(CommandUtils.toFormattedFieldName(fld))
            .schema(schema(fld.getType()))
            .description(fld.getAnnotation(Argument.class).description())
            .example(valueExample(fld))
            .required(!fld.getAnnotation(Argument.class).optional()));

        // TODO: support oneOf in spec.
        visitCommandParams(
            cmd.argClass(),
            fld -> {
                assert !fld.getAnnotation(Argument.class).optional();

                path.append("/{").append(toFormattedFieldName(fld)).append('}');

                params.add(new Parameter()
                    .style(SIMPLE)
                    .in("path")
                    .name(toFormattedFieldName(fld))
                    .description(fld.getAnnotation(Argument.class).description())
                    .example(valueExample(fld)));
            },
            fldCnsmr,
            (optional, flds) -> flds.forEach(fldCnsmr)
        );

        Content plainText = new Content().addMediaType(TEXT_PLAIN, new MediaType());

        api.path(
            path.toString(),
            new PathItem().get(new Operation()
                .description(cmd.description())
                .parameters(params)
                .responses(new ApiResponses()
                    .addApiResponse(Integer.toString(SC_OK), new ApiResponse()
                        .description("Command output")
                        .content(plainText))
                    .addApiResponse(Integer.toString(SC_INTERNAL_SERVER_ERROR), new ApiResponse()
                        .description("Error text")
                        .content(plainText)))
        ));
    }

    /** */
    private Schema<?> schema(Class<?> cls) {
        if (cls == Float.class || cls == float.class)
            return new NumberSchema().format("float");
        else if (cls == Double.class || cls == double.class)
            return new NumberSchema().format("double");
        else if (cls == short.class
            || cls == Short.class
            || cls == byte.class
            || cls == Byte.class)
            return new IntegerSchema().format(null);
        else if (cls == int.class || cls == Integer.class)
            return new IntegerSchema();
        else if (cls == long.class || cls == Long.class)
            return new IntegerSchema().format("int64");
        else if (cls == Boolean.class || cls == boolean.class)
            return new BooleanSchema();
        else if (cls == String.class)
            return new StringSchema();
        else if (cls == UUID.class)
            return new StringSchema().format("uuid");
        else if (cls == IgniteUuid.class)
            return new StringSchema();
        else if (cls.isArray())
            return new ArraySchema().items(schema(cls.getComponentType()));

        throw new IllegalArgumentException("Type not supported: " + cls);
    }

    /** */
    public void onIgniteStop() {
        try {
            if (srv != null)
                srv.stop();
        }
        catch (Exception e) {
            log.warning("Error stop OpenApi server", e);
        }
    }
}
