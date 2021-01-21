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

package org.apache.ignite.internal.performancestatistics;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Iterator;
import java.util.LinkedList;
import org.apache.ignite.internal.performancestatistics.handlers.PrintHandler;
import org.apache.ignite.internal.processors.performancestatistics.FilePerformanceStatisticsReader;
import org.apache.ignite.internal.processors.performancestatistics.OperationType;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import static java.lang.String.join;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toCollection;

/**
 * Performance statistics printer.
 */
public class PerformanceStatisticsPrinter {
    /**
     * @param args Program arguments or '-h' to get usage help.
     */
    public static void main(String... args) throws Exception {
        Parameters params = parseArguments(args);

        validateParameters(params);

        PrintStream ps;

        if (params.outFile != null) {
            try {
                ps = new PrintStream(Files.newOutputStream(new File(params.outFile).toPath()));
            }
            catch (IOException e) {
                throw new IllegalArgumentException("Cannot write to output file", e);
            }
        }
        else
            ps = System.out;

        try {
            PrintHandler hnd = new PrintHandler(ps, params.ops, params.from, params.to);

            new FilePerformanceStatisticsReader(hnd).read(singletonList(new File(params.statFiles)));
        }
        finally {
            if (params.outFile != null)
                ps.close();
        }
    }

    /**
     * Parses arguments or print help.
     *
     * @param args Arguments to parse.
     * @return Program arguments.
     */
    private static Parameters parseArguments(String[] args) {
        if (args == null || args.length == 0 || "--help".equalsIgnoreCase(args[0]) || "-h".equalsIgnoreCase(args[0])) {
            String ops = join(", ",
                Arrays.stream(OperationType.values()).map(Enum::toString).collect(toCollection(LinkedList::new)));

            System.out.println("The script is used to print performance statistics files to the console or file." +
                    U.nl() + U.nl() +
                    "Usage: print-statistics.sh path_to_files [--out out_file] [--ops op_types] " +
                    "[--from startTimeFrom] [--to startTimeTo]" + U.nl() + U.nl() +
                    "  path_to_files - Performance statistics file or files directory." + U.nl() +
                    "  out_file      - Output file." + U.nl() +
                    "  op_types      - Comma separated list of operation types to filter the output." + U.nl() +
                    "  from          - The minimum operation start time to filter the output." + U.nl() +
                    "  to            - The maximum operation start time to filter the output." + U.nl() + U.nl() +
                    "Times must be specified in the Unix time format in milliseconds." + U.nl() +
                    "List of operation types: " + ops + '.');

            System.exit(0);
        }

        Parameters params = new Parameters();

        Iterator<String> iter = Arrays.asList(args).iterator();

        params.statFiles = iter.next();

        while (iter.hasNext()) {
            String arg = iter.next();

            if ("--out".equalsIgnoreCase(arg)) {
                A.ensure(iter.hasNext(), "Expected output file name");

                params.outFile = iter.next();
            }
            else if ("--ops".equalsIgnoreCase(arg)) {
                A.ensure(iter.hasNext(), "Expected operation types");

                String[] ops = iter.next().split(",");

                A.ensure(ops.length > 0, "Expected at least one operation");

                params.ops = new BitSet();

                for (String op : ops) {
                    OperationType opType = enumIgnoreCase(op, OperationType.class);

                    A.ensure(opType != null, "Unknown operation type [op=" + op + ']');

                    params.ops.set(opType.id());
                }
            }
            else if ("--from".equalsIgnoreCase(arg)) {
                A.ensure(iter.hasNext(), "Expected time from");

                params.from = Long.parseLong(iter.next());
            }
            else if ("--to".equalsIgnoreCase(arg)) {
                A.ensure(iter.hasNext(), "Expected time to");

                params.to = Long.parseLong(iter.next());
            }
            else
                throw new IllegalArgumentException("Unknown command: " + arg);
        }

        return params;
    }

    /** @param params Validates parameters. */
    private static void validateParameters(Parameters params) {
        File statFiles = new File(params.statFiles);

        A.ensure(statFiles.exists(), "Performance statistics file or files directory does not exists");

        if (params.outFile != null) {
            File out = new File(params.outFile);

            try {
                boolean created = out.createNewFile();

                if (!created) {
                    throw new IllegalArgumentException("Failed to create output file: file with the given name " +
                        "already exists");
                }
            }
            catch (IOException e) {
                throw new IllegalArgumentException("Failed to create the output file", e);
            }
        }
    }

    /**
     * Gets the enum for the given name ignore case.
     *
     * @param name Enum name.
     * @param cls Enum class.
     * @return The enum or {@code null} if not found.
     */
    private static <E extends Enum<E>> @Nullable E enumIgnoreCase(String name, Class<E> cls) {
        for (E e : cls.getEnumConstants()) {
            if (e.name().equalsIgnoreCase(name))
                return e;
        }

        return null;
    }

    /** Printer parameters. */
    private static class Parameters {
        /** Performance statistics file or files path. */
        private String statFiles;

        /** Output file. */
        @Nullable private String outFile;

        /** Operation types to print. */
        @Nullable private BitSet ops;

        /** The minimum operation start time to filter the output. */
        private long from = Long.MIN_VALUE;

        /** The maximum operation start time to filter the output. */
        private long to = Long.MAX_VALUE;
    }
}
