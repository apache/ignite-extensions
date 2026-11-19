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

package org.apache.ignite.spi.discovery.tcp.ipfinder.s3_v2.client;


import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.util.*;

import static org.apache.ignite.spi.aws2.S3Utils.doesBucketExist;

/**
 * Class to test {@link DummyS3Client}.
 */
public class DummyS3ClientTest extends GridCommonAbstractTest {
    /**
     * Instance of {@link DummyS3Client} to be used for tests.
     */
    private S3Client s3;

    /**
     * Holds fake key prefixes.
     */
    private Set<String> fakeKeyPrefixSet;

    /**
     * {@inheritDoc}
     */
    @Override
    protected void beforeTest() {
        fakeKeyPrefixSet = new HashSet<>();
        fakeKeyPrefixSet.add("/test/path/val");
        fakeKeyPrefixSet.add("/test/val/test/path");
        fakeKeyPrefixSet.add("/test/test/path/val");

        Map<String, Set<String>> fakeObjMap = new HashMap<>();

        fakeObjMap.put("testBucket", fakeKeyPrefixSet);

        s3 = new DummyS3Client(fakeObjMap);
    }

    /**
     * Test cases to check the 'doesBucketExist' method.
     */
    @Test
    public void testDoesBucketExist() {
        assertTrue("The bucket 'testBucket' should exist", doesBucketExist(s3, "testBucket"));
        assertFalse("The bucket 'nonExistentBucket' should not exist", doesBucketExist(s3, "nonExistentBucket"));
    }

    /**
     * Test cases for various object listing functions for S3 bucket.
     */
    @Test
    public void testListObjects() {
        ListObjectsResponse listing = s3.listObjects(ListObjectsRequest.builder().bucket("testBucket").build());

        List<S3Object> summaries = listing.contents();

        assertFalse("'testBucket' contains keys", summaries.isEmpty());
        assertTrue("'testBucket' contains more keys to fetch", listing.isTruncated());
        for (S3Object s3Object : summaries) {
            assertTrue(fakeKeyPrefixSet.contains(s3Object.key()));
        }


        try {
            s3.listObjects(ListObjectsRequest.builder().bucket("nonExistentBucket").build());
        } catch (S3Exception e) {
            System.out.println("Exception message: ++++++++++++++++++  "+e.getMessage());
            assertTrue(e.getMessage().startsWith("The specified bucket"));
        }
    }

    /**
     * Test cases for various object listing functions for S3 bucket and key prefix.
     */
    @Test
    public void testListObjectsWithAPrefix() {

        assertThatBucketContain("testBucket", "/test", 3);
        assertThatBucketContain("testBucket", "/test/path", 1);
        assertThatBucketContain("testBucket", "/test/path1", 0);

        try {
            s3.listObjects( ListObjectsRequest.builder().bucket( "nonExistentBucket").
                    prefix( "/test").build());
        } catch (S3Exception e) {
            System.out.println("Exception message: ++++++++++++++++++  "+e.getMessage());
            assertTrue(e.getMessage().contains("The specified bucket"));
        }
    }

    private void assertThatBucketContain(String testBucket, String prefix, int expectedCount) {
        ListObjectsResponse listing = s3.listObjects( ListObjectsRequest.builder().bucket(  testBucket )
                .prefix( prefix).build());
        List<S3Object> summaries = listing.contents();
        if( expectedCount>0 ) {
            assertFalse("'" + testBucket + "' must contain key with prefix '" + prefix + "'", summaries.isEmpty());
        }
        assertEquals("'"+testBucket+"' contains expected number of keys with prefix '"+prefix+"'", expectedCount, summaries.size() );
        for( S3Object s3Object : summaries ){
            assertTrue( "Unexpected prefix:" + s3Object.key(), s3Object.key().startsWith( prefix ) );
        }
    }

    /**
     * Test case to check if a bucket is created properly.
     */
    @Test
    public void testCreateBucket() {
        s3.createBucket(CreateBucketRequest.builder().bucket("testBucket1").build());
        assertTrue("The bucket 'testBucket1' should exist", doesBucketExist( s3,"testBucket1"));
        try {
            s3.createBucket( CreateBucketRequest.builder().bucket("testBucket1").build());
        } catch (BucketAlreadyExistsException e) {
            assertTrue(e.getMessage().length() > 5);
        }catch (Throwable e){
            fail("Unexpected exception: "+e.getMessage());
        }
    }


}
