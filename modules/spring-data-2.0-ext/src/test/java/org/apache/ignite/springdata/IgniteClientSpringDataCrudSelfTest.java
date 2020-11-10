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

import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.springdata.misc.IgniteClientApplicationConfiguration;
import org.apache.ignite.springdata.misc.PersonRepository;
import org.apache.ignite.springdata.misc.IgniteClientConfigRepository;
import org.apache.ignite.springdata20.repository.support.IgniteRepositoryFactory;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

/** Tests Spring Data CRUD operation when thin client is used for accessing the Ignite cluster. */
public class IgniteClientSpringDataCrudSelfTest extends IgniteSpringDataCrudSelfTest {
    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        ctx = new AnnotationConfigApplicationContext();

        ctx.register(IgniteClientApplicationConfiguration.class);
        ctx.refresh();

        repo = ctx.getBean(PersonRepository.class);
    }

    /** Text queries are not supported when {@link IgniteClient} is used for acessing the Ignite cluster. */
    @Override public void testUpdateQueryMixedCaseProjectionIndexedParameterLuceneTextQuery() {
        GridTestUtils.assertThrows(log,
            () -> repo.textQueryByFirstNameWithProjectionNamedParameter("person"), IllegalStateException.class,
            "Query of type TextQuery is not supported by thin client. Check" +
                " org.apache.ignite.springdata.misc.PersonRepository#textQueryByFirstNameWithProjectionNamedParameter" +
                " method configuration or use Ignite node instance to connect to the Ignite cluster.");
    }

    /**
     * Tests repository configuration in case {@link ClientConfiguration} is used to provide access to Ignite cluster.
     */
    @Test
    public void testRepositoryWithClientConfiguration() {
        IgniteRepositoryFactory factory = new IgniteRepositoryFactory(ctx);

        IgniteClientConfigRepository repo = factory.getRepository(IgniteClientConfigRepository.class);

        assertTrue(repo.count() > 0);
    }
}
