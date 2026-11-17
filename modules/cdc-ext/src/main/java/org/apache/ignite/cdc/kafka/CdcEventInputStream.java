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

package org.apache.ignite.cdc.kafka;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;
import java.util.Set;
import org.apache.ignite.cdc.CdcEvent;

/**
 * Stream to read {@link CdcEvent} serialized with the Java serialization, possibly, by another Ignite version.
 * <p>
 * Some classes of the event object graph have no explicit {@code serialVersionUID}, so the value is computed
 * from the class signature and differs between Ignite versions, although serialized form of the class is the same.
 * The stream ignores {@code serialVersionUID} mismatch for such classes.
 */
class CdcEventInputStream extends ObjectInputStream {
    /**
     * Names of the classes to ignore {@code serialVersionUID} mismatch for.
     * Note, only the classes that don't write any data to the stream by itself are allowed here.
     */
    private static final Set<String> IGNORE_SERIAL_VER_UID = Set.of(
        // Abstract superclass of BinaryObjectImpl, the class has no state.
        // The class has no explicit serialVersionUID in 2.18.0, the computed value differs from the one in other versions.
        "org.apache.ignite.internal.binary.BinaryObjectExImpl"
    );

    /**
     * @param in Input stream.
     * @throws IOException If failed.
     */
    CdcEventInputStream(InputStream in) throws IOException {
        super(in);
    }

    /** {@inheritDoc} */
    @Override protected ObjectStreamClass readClassDescriptor() throws IOException, ClassNotFoundException {
        ObjectStreamClass desc = super.readClassDescriptor();

        if (!IGNORE_SERIAL_VER_UID.contains(desc.getName()))
            return desc;

        ObjectStreamClass locDesc = ObjectStreamClass.lookup(
            Class.forName(desc.getName(), false, CdcEventInputStream.class.getClassLoader()));

        return locDesc == null || locDesc.getSerialVersionUID() == desc.getSerialVersionUID() ? desc : locDesc;
    }
}
