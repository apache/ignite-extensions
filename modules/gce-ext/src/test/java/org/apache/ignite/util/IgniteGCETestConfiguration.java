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
 * Google Compute Engine tests configuration.
 */
public class IgniteGCETestConfiguration {
    /**
     * @return Service Account Id.
     */
    public static String getServiceAccountId() {
        String id = X.getSystemOrEnv("test.gce.account.id");

        assert id != null : "Environment variable 'test.gce.account.id' is not set";

        return id;
    }

    /**
     * @return Service Account p12 file path.
     */
    public static String getP12FilePath() {
        String path = X.getSystemOrEnv("test.gce.p12.path");

        assert path != null : "Environment variable 'test.gce.p12.path' is not set";

        return path;
    }

    /**
     * @return GCE project name.
     */
    public static String getProjectName() {
        String name = X.getSystemOrEnv("test.gce.project.name");

        assert name != null : "Environment variable 'test.gce.project.name' is not set";

        return name;
    }
}
