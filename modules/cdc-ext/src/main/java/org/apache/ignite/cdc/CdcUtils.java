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

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.binary.BinaryMetadata;
import org.apache.ignite.internal.binary.BinaryTypeImpl;
import org.apache.ignite.internal.processors.cache.binary.CacheObjectBinaryProcessorImpl;

/** Utility methods for CDC. */
public class CdcUtils {
    /**
     * Register {@code meta} inside {@code ign} instance.
     *
     * @param ign Ignite instance.
     * @param meta Binary metadata to register.
     */
    public static void registerBinaryMeta(IgniteEx ign, BinaryMetadata meta) {
        ign.context().cacheObjects().addMeta(
            meta.typeId(),
            new BinaryTypeImpl(
                ((CacheObjectBinaryProcessorImpl)ign.context().cacheObjects()).binaryContext(),
                meta
            ),
            false
        );
    }

    /**
     * Register {@code mapping} inside {@code ign} instance.
     *
     * @param ign Ignite instance.
     * @param mapping Type mapping to register.
     */
    public static void registerMapping(IgniteEx ign, TypeMapping mapping) {
        assert mapping.platform().ordinal() <= Byte.MAX_VALUE;

        try {
            ign.context().marshallerContext().registerClassName(
                (byte)mapping.platform().ordinal(),
                mapping.typeId(),
                mapping.typeName(),
                false
            );
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }
}
