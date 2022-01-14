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

package org.apache.ignite.springdata.compoundkey;

import java.util.Objects;

/** Extended city compound key */
public class CityKeyExt extends CityKey {

    /** city extended identifier */
    private long idExt;

    public CityKeyExt(int id, String countryCode, long idExt) {
        super(id, countryCode);
        this.idExt = idExt;
    }

    /**
     * @return city extended identifier
     * */
    public long getIdExt() {
        return idExt;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        CityKeyExt that = (CityKeyExt) o;
        return idExt == that.idExt;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Objects.hash(super.hashCode(), idExt);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "CityKeyExt{" +
                "idExt=" + idExt +
                "} " + super.toString();
    }
}
