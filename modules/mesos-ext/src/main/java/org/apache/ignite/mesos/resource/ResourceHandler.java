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

package org.apache.ignite.mesos.resource;

import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.io.Content;
import org.eclipse.jetty.io.Content.Source;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Response;
import org.eclipse.jetty.util.Callback;

/**
 * HTTP controller which provides on slave resources.
 */
public class ResourceHandler extends Handler.Abstract {
    /** */
    public static final String IGNITE_PREFIX = "/ignite/";

    /** */
    public static final String LIBS_PREFIX = "/libs/";

    /** */
    public static final String CONFIG_PREFIX = "/config/";

    /** */
    public static final String DEFAULT_CONFIG = CONFIG_PREFIX + "default/";

    /** */
    private String libsDir;

    /** */
    private String cfgPath;

    /** */
    private String igniteDir;

    /**
     * @param libsDir Directory with user's libs.
     * @param cfgPath Path to config file.
     * @param igniteDir Directory with ignites.
     */
    public ResourceHandler(String libsDir, String cfgPath, String igniteDir) {
        this.libsDir = libsDir;
        this.cfgPath = cfgPath;
        this.igniteDir = igniteDir;
    }

    /** {@inheritDoc} */
    @Override public boolean handle(Request req, Response res, Callback callback) {
        String url = req.getHttpURI().getPath();

        String[] path = url.split("/");

        String fileName = path[path.length - 1];

        String srvcPath = url.substring(0, url.length() - fileName.length());

        switch (srvcPath) {
            case IGNITE_PREFIX:
                handleFileRequest(res, callback, "application/zip-archive", igniteDir + "/" + fileName);

                return true;

            case LIBS_PREFIX:
                handleFileRequest(res, callback, "application/java-archive", libsDir + "/" + fileName);

                return true;

            case CONFIG_PREFIX:
                handleFileRequest(res, callback, "application/xml", cfgPath);

                return true;

            case DEFAULT_CONFIG:
                handleStreamRequest(res, callback, "application/xml",
                    Thread.currentThread().getContextClassLoader().getResourceAsStream(fileName),
                    fileName);

                return true;

            default:
                res.setStatus(404);
                callback.succeeded();

                return true;
        }
    }

    /**
     * @param res Jetty response.
     * @param callback Callback to complete the request.
     * @param contentType MIME content type.
     * @param path Path to file.
     */
    private void handleFileRequest(Response res, Callback callback, String contentType, String path) {
        Path path0 = Paths.get(path);

        res.getHeaders().put(HttpHeader.CONTENT_TYPE, contentType);
        res.getHeaders().put(HttpHeader.CONTENT_DISPOSITION, "attachment; filename=\"" + path0.getFileName() + "\"");

        Content.copy(Source.from(path0), res, Callback.from(callback::succeeded, e -> {
            res.setStatus(500);
            callback.failed(e);
        }));
    }

    /**
     * @param res Jetty response.
     * @param callback Callback to complete the request.
     * @param contentType MIME content type.
     * @param stream Input stream.
     * @param attachmentName Attachment name.
     */
    private void handleStreamRequest(Response res, Callback callback, String contentType,
        InputStream stream, String attachmentName) {
        res.getHeaders().put(HttpHeader.CONTENT_TYPE, contentType);
        res.getHeaders().put(HttpHeader.CONTENT_DISPOSITION, "attachment; filename=\"" + attachmentName + "\"");

        Content.copy(Source.from(stream), res, Callback.from(callback::succeeded, e -> {
            res.setStatus(500);
            callback.failed(e);
        }));
    }
}
