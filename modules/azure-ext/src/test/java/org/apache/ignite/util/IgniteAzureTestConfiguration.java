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
package org.apache.ignite.util;

import org.apache.ignite.internal.util.typedef.X;

/**
 * Azure tests configuration.
 */
public class IgniteAzureTestConfiguration {
    /**
     * @return Account Name
     */
    public static String getAccountName() {
        String id = X.getSystemOrEnv("test.azure.account.name");

        assert id != null : "Environment variable 'test.azure.account.name' is not set";

        return id;
    }

    /**
     * @return Account Key
     */
    public static String getAccountKey() {
        String path = X.getSystemOrEnv("test.azure.account.key");

        assert path != null : "Environment variable 'test.azure.account.key' is not set";

        return path;
    }

    /**
     * @return Endpoint
     */
    public static String getEndpoint() {
        String name = X.getSystemOrEnv("test.azure.endpoint");

        assert name != null : "Environment variable 'test.azure.endpoint' is not set";

        return name;
    }
}
