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

package org.apache.ignite.facade;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.IgniteClient;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/** */
public class IgniteFacadeTest {
    /** */
    @Test
    public void testIgniteFacade() {
        checkCommonMethods(Ignite.class, IgniteClient.class, IgniteFacade.class, Collections.singleton("close"));
    }

    /** */
    @Test
    public void testIgniteCacheFacade() {
        checkCommonMethods(IgniteCache.class, ClientCache.class, IgniteCacheFacade.class, null);
    }

    /** */
    private void checkCommonMethods(Class<?> lhs, Class<?> rhs, Class<?> facade, Set<String> exclusions) {
        Map<String, Set<Method>> lhsMethods = extractMethods(lhs);
        Map<String, Set<Method>> facadeMethods = extractMethods(facade);

        Map<String, Set<Method>> intersection = new HashMap<>();

        for (Method rhsMethod : rhs.getMethods()) {
            Set<Method> lhsMethodNameMatches = lhsMethods.get(rhsMethod.getName());

            if (lhsMethodNameMatches == null)
                continue;

            for (Method lhsMethod : lhsMethodNameMatches) {
                if (equals(lhsMethod, rhsMethod))
                    intersection.computeIfAbsent(lhsMethod.getName(), k -> new HashSet<>()).add(lhsMethod);
            }
        }

        intersection.forEach((k, v) -> {
            if (exclusions != null && exclusions.contains(k))
                return;

            Set<Method> facadeMethodNameMatches = facadeMethods.get(k);

            assertNotNull(v + " methods are absent in " + facade.getName(), facadeMethodNameMatches);

            for (Method intersectionMethod : v) {
                boolean isFound = false;

                for (Method facadeMethod : facadeMethodNameMatches) {
                    if (equals(intersectionMethod, facadeMethod)) {
                        isFound = true;

                        break;
                    }
                }

                assertTrue(intersectionMethod + "method is absent in " + facade.getName(), isFound);
            }
        });
    }

    /** */
    private Map<String, Set<Method>> extractMethods(Class<?> cls) {
        Map<String, Set<Method>> res = new HashMap<>();

        for (Method method : cls.getMethods())
            res.computeIfAbsent(method.getName(), K -> new HashSet<>()).add(method);

        return res;
    }

    /** */
    private boolean equals(Method lhs, Method rhs) {
        return Arrays.equals(lhs.getTypeParameters(), rhs.getTypeParameters()) && lhs.getReturnType() == rhs.getReturnType();
    }
}
