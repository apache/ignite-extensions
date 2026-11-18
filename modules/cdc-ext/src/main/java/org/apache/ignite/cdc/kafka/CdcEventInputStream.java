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
import org.apache.ignite.cdc.CdcEvent;

/** Stream to read {@link CdcEvent} serialized with the Java serialization, possibly, by another Ignite version. */
class CdcEventInputStream extends ObjectInputStream {
    /** The class has no explicit serialVersionUID in 2.17 and 2.18.0, the values computed by JVM differ from each other. */
    static final String INCOMPATIBLE_CLS = "org.apache.ignite.internal.binary.BinaryObjectExImpl";

    /** */
    CdcEventInputStream(InputStream in) throws IOException {
        super(in);
    }

    /** {@inheritDoc} */
    @Override protected ObjectStreamClass readClassDescriptor() throws IOException, ClassNotFoundException {
        ObjectStreamClass desc = super.readClassDescriptor();

        if (INCOMPATIBLE_CLS.equals(desc.getName())) {
            ObjectStreamClass locDesc =
                ObjectStreamClass.lookup(Class.forName(desc.getName(), false, CdcEventInputStream.class.getClassLoader()));

            return locDesc == null || locDesc.getSerialVersionUID() == desc.getSerialVersionUID()
                ? desc : locDesc;
        }

        return desc;
    }
}
