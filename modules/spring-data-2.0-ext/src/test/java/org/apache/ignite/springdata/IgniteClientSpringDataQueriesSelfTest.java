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

package org.apache.ignite.springdata;

import org.apache.ignite.springdata.misc.IgniteClientApplicationConfiguration;
import org.apache.ignite.springdata.misc.IgniteClientPersonRepository;
import org.apache.ignite.springdata.misc.IgniteClientUnsupportedApplicationConfiguration;
import org.apache.ignite.springdata.misc.Person;
import org.apache.ignite.springdata.misc.PersonRepositoryOtherIgniteInstance;
import org.apache.ignite.springdata.misc.PersonSecondRepository;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

/** Tests Sping Data query operations when thin client is used for accessing the Ignite cluster. */
public class IgniteClientSpringDataQueriesSelfTest extends IgniteSpringDataQueriesSelfTest {
    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        ctx = new AnnotationConfigApplicationContext();

        ctx.register(IgniteClientApplicationConfiguration.class);
        ctx.refresh();

        repo = ctx.getBean(IgniteClientPersonRepository.class);
        repo2 = ctx.getBean(PersonSecondRepository.class);
        repoTWO = ctx.getBean(PersonRepositoryOtherIgniteInstance.class);

        for (int i = 0; i < CACHE_SIZE; i++) {
            repo.save(i, new Person("person" + Integer.toHexString(i),
                "lastName" + Integer.toHexString((i + 16) % 256)));
            repoTWO.save(i, new Person("TWOperson" + Integer.toHexString(i),
                "lastName" + Integer.toHexString((i + 16) % 256)));
        }
    }

    /** Tests that creation of repository with text queries are prohibited if thin client is used to access the cluster. */
    @Test
    public void testTextQueryUnsupportedFailure() {
        ctx = new AnnotationConfigApplicationContext();
        ctx.register(IgniteClientUnsupportedApplicationConfiguration.class);

        GridTestUtils.assertThrowsAnyCause(
            log,
            () -> {
                ctx.refresh();

                return null;
            },
            IllegalStateException.class,
            "Invalid Spring Data query configuration for method org.apache.ignite.springdata.misc." +
                "IgnitePersonRepository#textQueryByFirstNameWithProjectionNamedParameter." +
                " Text queries are not suppported when a thin client is used to access the Ignite cluster.");
    }
}
