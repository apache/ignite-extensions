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
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.dto.IgniteDataTransferObject;
import org.apache.ignite.internal.management.api.Command;
import org.apache.ignite.internal.management.api.CommandUtils;
import org.apache.ignite.internal.management.api.CommandsRegistry;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.ignite.internal.management.api.CommandUtils.CMD_WORDS_DELIM;
import static org.apache.ignite.internal.management.api.CommandUtils.executeAndPrint;
import static org.apache.ignite.internal.management.api.CommandUtils.formattedName;
import static org.apache.ignite.internal.management.api.CommandUtils.fromFormattedName;
import static org.apache.ignite.internal.management.api.CommandUtils.parseVal;
import static org.apache.ignite.internal.management.openapi.OpenApiCommandsRegistryInvokerPlugin.TEXT_PLAIN;

/** */
public class ManagementApiServlet extends HttpServlet {
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
    @Override protected void doGet(
        HttpServletRequest req,
        HttpServletResponse resp
    ) throws IOException {
        resp.setStatus(HttpServletResponse.SC_OK);
        resp.setContentType(TEXT_PLAIN);
        resp.setCharacterEncoding("UTF-8");

        String uri = req.getRequestURI();

        if (!uri.startsWith(root))
            throw new IllegalArgumentException("Wrong URI: " + uri);

        String[] cmdPathAndPosArgs = uri.substring(root.length() + 1).split("/");

        if (cmdPathAndPosArgs.length == 0)
            throw new IllegalArgumentException("Empty command path: " + uri);

        Command<?, ?, ?> cmd =
            grid.context().commands().command(fromFormattedName(cmdPathAndPosArgs[0], CMD_WORDS_DELIM));

        if (cmd == null)
            throw new IllegalArgumentException("Unknown command: " + cmdPathAndPosArgs[0]);

        AtomicInteger i = new AtomicInteger(1);

        while (cmd instanceof CommandsRegistry && i.get() < cmdPathAndPosArgs.length) {
            Command<?, ?, ?> cmd0 =
                ((CommandsRegistry)cmd).command(fromFormattedName(cmdPathAndPosArgs[i.get()], CMD_WORDS_DELIM));

            if (cmd0 == null)
                break;

            cmd = cmd0;

            i.incrementAndGet();
        }

        parseAndExecute(req, resp, cmdPathAndPosArgs, cmd, i);
    }

    /** */
    private <A extends IgniteDataTransferObject> void parseAndExecute(
        HttpServletRequest req,
        HttpServletResponse resp,
        String[] cmdPathAndPosArgs,
        Command<A, ?, ?> cmd,
        AtomicInteger i
    ) throws IOException {
        try {
            A arg = CommandUtils.arguments(
                cmd.args(),
                (fld, pos) -> i.get() + pos < cmdPathAndPosArgs.length
                    ? parseVal(cmdPathAndPosArgs[i.get() + pos], fld.getType())
                    : null,
                fld -> {
                    String val = req.getParameter(formattedName(fld.getName(), CMD_WORDS_DELIM));

                    return val == null ? null : parseVal(val, fld.getType());
                }
            );

            executeAndPrint(grid, cmd, arg, resp.getWriter()::println);
        }
        catch (InstantiationException | IllegalAccessException e) {
            throw new IgniteException(e);
        }
    }
}
