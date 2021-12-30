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

import org.apache.ignite.springdata.compoundkey.City;
import org.apache.ignite.springdata.compoundkey.CityKey;
import org.apache.ignite.springdata.compoundkey.CityKeyExt;
import org.apache.ignite.springdata.compoundkey.CityRepository;
import org.apache.ignite.springdata.compoundkey.CompoundKeyApplicationConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

/**
 * Test with using conpoud key in spring-data
 * */
public class IgniteSpringDataCompoundExtendedKeyTest extends GridCommonAbstractTest {
    /** Application context */
    protected static AnnotationConfigApplicationContext ctx;

    /** City repository */
    protected static CityRepository repo;

    /** Cities count */
    private static final int TOTAL_COUNT = 6;

    /** Zimbabwe county code */
    private static final String ZW = "ZW";

    /** test city Kabul */
    private static final City KABUL = new City("Kabul", "Kabol", 1780000);

    /** test city Harare */
    private static final City HARARE = new City("Harare", "Harare", 3120917);

    /**
     * Performs context initialization before tests.
     */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        ctx = new AnnotationConfigApplicationContext();
        ctx.register(CompoundKeyApplicationConfiguration.class);
        ctx.refresh();

        repo = ctx.getBean(CityRepository.class);
    }

    /**
     * Load data
     * */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        loadData();

        assertEquals(TOTAL_COUNT, repo.count());
    }

    /**
     * Clear data
     * */
    @Override protected void afterTest() throws Exception {
        repo.deleteAll();

        assertEquals(0, repo.count());

        super.afterTest();
    }

    /**
     * Performs context destroy after tests.
     */
    @Override protected void afterTestsStopped() {
        ctx.close();
    }

    /** load data*/
    public void loadData() {
        repo.save(new CityKeyExt(1, "AFG", 11), new City("Kabul", "Kabol",1780000));
        repo.save(new CityKeyExt(2, "AFG", 12), new City("Qandahar", "Qandahar",237500));
        repo.save(new CityKeyExt(3, "AFG", 13), new City("Herat", "Herat",186800));
        repo.save(new CityKeyExt(4, "AFG", 14), new City("Mazar-e-Sharif", "Balkh",127800));
        repo.save(new CityKeyExt(5, "NLD", 25), new City("Amsterdam", "Noord-Holland",731200));
        repo.save(new CityKeyExt(6, "ZW", 36), new City("Harare", "Harare", 3120917));
    }

    /** Test */
    @Test
    public void test() {
        assertEquals(Optional.of(KABUL), repo.findById(new CityKeyExt(1, "AFG", 11)));
        assertEquals(Optional.of(HARARE), repo.findById(new CityKeyExt(6, "ZW", 36)));
    }

    /** Test */
    @Test
    public void deleteAllById() {
        Set<CityKey> keys = new HashSet<>();
        keys.add(new CityKeyExt(1, "AFG", 11));
        keys.add(new CityKeyExt(2, "AFG", 12));
        keys.add(new CityKeyExt(3, "AFG", 13));
        keys.add(new CityKeyExt(4, "AFG", 14));
        keys.add(new CityKeyExt(5, "NLD", 25));
        keys.add(new CityKeyExt(6, "ZW", 36));

        repo.deleteAllById(keys);
        assertEquals(0, repo.count());
    }
}
