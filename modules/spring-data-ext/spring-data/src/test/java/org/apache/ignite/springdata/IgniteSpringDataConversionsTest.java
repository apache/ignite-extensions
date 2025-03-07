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

import java.time.LocalDateTime;
import org.apache.ignite.springdata.misc.ApplicationConfiguration;
import org.apache.ignite.springdata.misc.CustomConvertersApplicationConfiguration;
import org.apache.ignite.springdata.misc.Person;
import org.apache.ignite.springdata.misc.PersonRepository;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

/**
 * Test for Ignite Spring Data conversions.
 */
public class IgniteSpringDataConversionsTest extends GridCommonAbstractTest {
    /** Repository. */
    protected PersonRepository repo;

    /** Context. */
    protected static AnnotationConfigApplicationContext ctx;

    /** {@inheritDoc} */
    @Override protected void beforeTest() {
        ctx = new AnnotationConfigApplicationContext();
    }

    /** */
    @Test
    public void testPutGetWithDefaultConverters() {
        init(ApplicationConfiguration.class);

        Person person = new Person("some_name", "some_surname", LocalDateTime.now());

        assertEquals(person, savePerson(person));
    }

    /** */
    @Test
    public void testPutGetWithCustomConverters() {
        init(CustomConvertersApplicationConfiguration.class);

        Person person = new Person("some_name", "some_surname", LocalDateTime.now());

        assertNull(savePerson(person).getCreatedAt());
    }

    /**
     * @param componentClasses Component classes.
     */
    private void init(Class<? extends ApplicationConfiguration> componentClasses) {
        ctx.register(componentClasses);
        ctx.refresh();

        repo = ctx.getBean(PersonRepository.class);
    }

    /**
     * @param person Person to save.
     * @return Saved person.
     */
    private Person savePerson(Person person) {
        int id = 1;

        assertEquals(person, repo.save(id, person));
        assertTrue(repo.existsById(id));

        return repo.selectByFirstNameWithCreatedAt(person.getFirstName()).get(0);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() {
        ctx.close();
    }
}
