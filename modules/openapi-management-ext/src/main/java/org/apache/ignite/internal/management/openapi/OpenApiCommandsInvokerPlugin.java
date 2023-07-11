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
import java.lang.reflect.Field;
import java.net.BindException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import io.swagger.v3.jaxrs2.integration.OpenApiServlet;
import io.swagger.v3.oas.integration.GenericOpenApiContextBuilder;
import io.swagger.v3.oas.integration.OpenApiConfigurationException;
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
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteNodeAttributes;
import org.apache.ignite.internal.IgniteVersionUtils;
import org.apache.ignite.internal.management.IgniteCommandRegistry;
import org.apache.ignite.internal.management.api.Argument;
import org.apache.ignite.internal.management.api.ArgumentGroup;
import org.apache.ignite.internal.management.api.Command;
import org.apache.ignite.internal.management.api.CommandUtils;
import org.apache.ignite.internal.management.api.CommandsRegistry;
import org.apache.ignite.internal.managers.discovery.IgniteClusterNode;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.IgnitePlugin;
import org.apache.ignite.plugin.PluginContext;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.NetworkConnector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.servlets.CrossOriginFilter;

import static io.swagger.v3.oas.integration.api.OpenApiContext.OPENAPI_CONTEXT_ID_KEY;
import static io.swagger.v3.oas.models.parameters.Parameter.StyleEnum.SIMPLE;
import static java.util.Collections.singletonList;
import static javax.servlet.DispatcherType.REQUEST;
import static javax.servlet.http.HttpServletResponse.SC_INTERNAL_SERVER_ERROR;
import static javax.servlet.http.HttpServletResponse.SC_OK;
import static org.apache.ignite.internal.management.api.CommandUtils.NAME_PREFIX;
import static org.apache.ignite.internal.management.api.CommandUtils.executable;
import static org.apache.ignite.internal.management.api.CommandUtils.toFormattedCommandName;
import static org.apache.ignite.internal.management.api.CommandUtils.valueExample;
import static org.apache.ignite.internal.management.api.CommandUtils.visitCommandParams;

/**
 * Plugin expose management API commands via REST interface.
 * <a href="https://www.openapis.org/">Open API</a> used to describe commands.
 *
 * @see <a href="https://swagger.io/">Swagger</a>
 * @see <a href="https://www.openapis.org/">Open API</a>
 * @see Command
 * @see CommandsRegistry
 * @see IgniteCommandRegistry
 * @see IgniteEx#commandsRegistry()
 */
public class OpenApiCommandsInvokerPlugin implements IgnitePlugin {
    /** Init parameter name for {@link OpenApiServlet} to expose API description. */
    public static final String API_CTX_ID = "ignite-management-api-ctx";

    /** Ignite management API name. */
    public static final String API_ID = "ignite-management-api";

    /** */
    public static final String TEXT_PLAIN = "text/plain";

    /**
     * Node attribute to store port used by endpoint.
     * @see GridKernalContext#addNodeAttribute(String, Object)
     * @see IgniteClusterNode#attribute(String)
     */
    public static final String ATTR_OPENAPI_PORT = IgniteNodeAttributes.ATTR_PREFIX + "openapi.plugin.port";

    /**
     * Node attribute to store host used by endpoint.
     * @see GridKernalContext#addNodeAttribute(String, Object)
     * @see IgniteClusterNode#attribute(String)
     */
    public static final String ATTR_OPENAPI_HOST = IgniteNodeAttributes.ATTR_PREFIX + "openapi.plugin.host";

    /** Servlet name */
    public static final String INVOKER_SERVLET = "ignite-management-api-invoker";

    /** */
    private PluginContext ctx;

    /** */
    private IgniteLogger log;

    /** */
    private OpenApiCommandsInvokerPluginConfiguration cfg;

    /** */
    private Server srv;

    /** */
    public void context(PluginContext ctx, OpenApiCommandsInvokerPluginConfiguration cfg) {
        this.ctx = ctx;
        this.cfg = cfg;
        log = ctx.log(OpenApiCommandsInvokerPlugin.class);
    }

    /** */
    public void onIgniteStart() {
        log.info("Starting OpenApi invoker plugin[cfg=" + cfg + ']');

        try {
            initApiDescription();

            int port = -1;
            String host = null;

            if (cfg.getServer() != null) {
                srv = cfg.getServer();

                tryStart();

                for (Connector conn : srv.getConnectors()) {
                    if (conn instanceof NetworkConnector) {
                        port = ((NetworkConnector)conn).getPort();
                        host = ((NetworkConnector)conn).getHost();

                        break;
                    }
                }
            }
            else {
                port = cfg.getPort();
                host = cfg.getHost();

                Exception err = null;

                while (port <= cfg.getPort() + cfg.getPortRange()) {
                    try {
                        srv = new Server();

                        ServerConnector connector = new ServerConnector(srv);

                        connector.setPort(port);
                        connector.setHost(cfg.getHost());

                        srv.addConnector(connector);

                        tryStart();

                        err = null;

                        break;
                    }
                    catch (IOException e) {
                        if (!(e.getCause() instanceof BindException))
                            throw new IgniteException(e);

                        err = e;

                        port++;
                    }
                    catch (Exception e) {
                        throw new IgniteException(e);
                    }
                }

                if (err != null)
                    throw err;
            }

            log.info("OpenApi invoker started[rootURL=http://" + host + ":" + port + cfg.getRootUri() + ']');

            ((IgniteEx)ctx.grid()).context().addNodeAttribute(ATTR_OPENAPI_PORT, port);
            ((IgniteEx)ctx.grid()).context().addNodeAttribute(ATTR_OPENAPI_HOST, host);
        }
        catch (Exception e) {
            srv = null;

            throw new IgniteException(e);
        }
    }

    /** */
    private void tryStart() throws Exception {
        ServletContextHandler handler = new ServletContextHandler();

        ServletHolder apiExposeServlet = new ServletHolder(OpenApiServlet.class);

        apiExposeServlet.setInitParameter(OPENAPI_CONTEXT_ID_KEY, API_CTX_ID);

        handler.addServlet(apiExposeServlet, "/api/*");
        handler.addServlet(
            new ServletHolder(INVOKER_SERVLET, new ManagementApiServlet((IgniteEx)ctx.grid(), cfg.getRootUri())),
            cfg.getRootUri() + "/*"
        );
        handler.addFilter(CrossOriginFilter.class, "/*", EnumSet.of(REQUEST));

        srv.setHandler(handler);

        srv.start();
    }

    /**
     * Creates and pass to Open API internals Ignite Management API descrption.
     * @throws OpenApiConfigurationException If failed.
     */
    private void initApiDescription() throws OpenApiConfigurationException {
        OpenAPI api = new OpenAPI();

        api.info(new Info()
            .title("Ignite Management API")
            .description("This endpoint expose Apache Ignite management API commands")
            .version(IgniteVersionUtils.VER_STR)
        ).servers(singletonList(new io.swagger.v3.oas.models.servers.Server()
            .url("http://" + cfg.getHost() + ":" + cfg.getPort() + cfg.getRootUri())
            .description("Ignite node[id=" + ctx.grid().cluster().localNode().id() + ']')
        ));

        LinkedList<Command<?, ?>> path = new LinkedList<>();

        ((IgniteEx)ctx.grid()).commandsRegistry().commands().forEachRemaining(cmd -> {
            path.push(cmd.getValue());
            addToApi(api, path);
            path.pop();
        });

        SwaggerConfiguration cfg = new SwaggerConfiguration();

        cfg.setId(API_ID);
        cfg.setOpenAPI(api);
        cfg.prettyPrint(true);

        OpenApiContext ctx = new GenericOpenApiContextBuilder<>()
            .ctxId(API_CTX_ID)
            .openApiConfiguration(cfg)
            .buildContext(true);

        OpenApiContextLocator.getInstance().putOpenApiContext(API_CTX_ID, ctx);
    }

    /**
     * Adds command to API description.
     * @param api API description.
     * @param path Path to the command in {@link CommandsRegistry} hierarchy.
     */
    public void addToApi(OpenAPI api, LinkedList<Command<?, ?>> path) {
        if (path.peek() instanceof CommandsRegistry) {
            ((CommandsRegistry<?, ?>)path.peek()).commands().forEachRemaining(cmd0 -> {
                path.push(cmd0.getValue());
                addToApi(api, path);
                path.pop();
            });

            if (!executable(path.peek()))
                return;
        }

        List<Parameter> params = new ArrayList<>();

        BiConsumer<ArgumentGroup, Field> fldCnsmr = (argGrp, fld) -> params.add(new Parameter()
            .style(SIMPLE)
            .in("query")
            .name(parameterName(fld))
            .schema(schema(fld.getType()))
            .description(fld.getAnnotation(Argument.class).description())
            .example(valueExample(fld))
            .required(argGrp == null && !fld.getAnnotation(Argument.class).optional()));

        visitCommandParams(
            path.peek().argClass(),
            fld -> fldCnsmr.accept(null, fld),
            fld -> fldCnsmr.accept(null, fld),
            (argGrp, flds) -> flds.forEach(fld -> fldCnsmr.accept(argGrp, fld))
        );

        Content plainText = new Content().addMediaType(TEXT_PLAIN, new MediaType());

        api.path(
            commandUri(path),
            new PathItem().get(new Operation()
                .description(path.peek().description())
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

    /**
     * @param path Path to the command in {@link CommandsRegistry} hierarchy.
     * @return URI to call command via REST interface.
     */
    public static String commandUri(LinkedList<Command<?, ?>> path) {
        StringBuilder uri = new StringBuilder();

        for (int i = path.size() - 1; i >= 0; i--) {
            String cmdUri;

            if (i == (path.size() - 1))
                cmdUri = toFormattedCommandName(path.get(i).getClass());
            else {
                String parentUri = toFormattedCommandName(path.get(i + 1).getClass());

                cmdUri = toFormattedCommandName(path.get(i).getClass());

                assert cmdUri.startsWith(parentUri);

                cmdUri = cmdUri.substring(parentUri.length() + 1);
            }

            uri.append('/').append(cmdUri);
        }

        return uri.toString();
    }

    /**
     * @param fld Argument class field.
     * @return URL parameter name.
     */
    public static String parameterName(Field fld) {
        String name = CommandUtils.toFormattedFieldName(fld);

        return name.startsWith(NAME_PREFIX) ? name.substring(2) : name;
    }

    /**
     * @param cls Argument class field type.
     * @return Schema description for class.
     */
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
        else if (cls.isEnum()) {
            StringSchema enm = new StringSchema();

            enm.setEnum(Arrays.stream(cls.getEnumConstants())
                .map(cons -> ((Enum<?>)cons).name())
                .collect(Collectors.toList()));

            return enm;
        }

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
