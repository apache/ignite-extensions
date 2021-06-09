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

package org.apache.ignite.cdc;

import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.util.typedef.X;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.cdc.KafkaToIgniteLoader.KAFKA_PROPERTIES;
import static org.apache.ignite.internal.IgniteVersionUtils.ACK_VER_STR;
import static org.apache.ignite.internal.IgniteVersionUtils.COPYRIGHT;
import static org.apache.ignite.startup.cmdline.CommandLineStartup.isHelp;

/**
 * This class defines command-line {@link KafkaToIgniteCdcStreamer} startup. This startup can be used to start Ignite
 * {@link KafkaToIgniteCdcStreamer} application outside of any hosting environment from command line.
 * This startup is a Java application with {@link #main(String[])} method that accepts command line arguments.
 * It accepts on parameter which is Ignite Spring XML configuration file path.
 * You can run this class from command line without parameters to get help message.
 */
public class KafkaToIgniteCommandLineStartup {
    /** Quite log flag. */
    private static final boolean QUITE = IgniteSystemProperties.getBoolean(IgniteSystemProperties.IGNITE_QUIET);

    /**
     * Main entry point.
     * @param args Command line arguments.
     */
    public static void main(String[] args) {
        if (!QUITE) {
            X.println("Kafka To Ignite Command Line Startup, ver. " + ACK_VER_STR);
            X.println(COPYRIGHT);
            X.println();
        }

        if (args.length > 1)
            exit("Too many arguments.", true, -1);

        if (args.length > 0 && isHelp(args[0]))
            exit(null, true, 0);

        if (args.length > 0 && args[0].isEmpty())
            exit("Empty argument.", true, 1);

        if (args.length > 0 && args[0].charAt(0) == '-')
            exit("Invalid arguments: " + args[0], true, -1);

        try {
            KafkaToIgniteLoader.loadKafkaToIgniteStreamer(args[0]).run();
        }
        catch (Throwable e) {
            e.printStackTrace();

            String note = "";

            if (X.hasCause(e, ClassNotFoundException.class))
                note = "\nNote! You may use 'USER_LIBS' environment variable to specify your classpath.";

            exit("Failed to run app: " + e.getMessage() + note, false, -1);
        }
    }

    /**
     * Exists with optional error message, usage show and exit code.
     *
     * @param errMsg Optional error message.
     * @param showUsage Whether or not to show usage information.
     * @param exitCode Exit code.
     */
    private static void exit(@Nullable String errMsg, boolean showUsage, int exitCode) {
        if (errMsg != null)
            X.error(errMsg);

        if (showUsage) {
            X.error(
                "Usage:",
                "    kafka-to-ignite.{sh|bat} [?]|[path]",
                "    Where:",
                "    ?, /help, -help, - show this message.",
                "    -v               - verbose mode (quiet by default).",
                "    path            - path to Spring XML configuration file.",
                "                      Path can be absolute or relative to IGNITE_HOME.",
                " ",
                "Spring file should contain bean definition of 'org.apache.ignite.configuration.IgniteConfiguration' " +
                    "and 'org.apache.ignite.cdc.KafkaToIgniteCdcStreamerConfiguration' " +
                    "and bean of class 'java.util.Properties' with '" + KAFKA_PROPERTIES + "' name " +
                    "that contains properties to connect to Apache Kafka cluster. " +
                    "Note that bean will be fetched by the type and its ID is not used.");
        }

        System.exit(exitCode);
    }
}
