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

package org.apache.ignite.spi.checkpoint.s3_v2;

import org.apache.ignite.GridTestIoUtils;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.lang.GridAbsClosure;
import org.apache.ignite.internal.util.lang.GridAbsClosureX;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.checkpoint.GridCheckpointTestState;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.spi.GridSpiAbstractTest;
import org.apache.ignite.testframework.junits.spi.GridSpiTest;
import org.apache.ignite.util.IgniteS3TestConfiguration;
import org.junit.Ignore;
import org.junit.Test;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Grid S3 checkpoint SPI self test.
 */
@GridSpiTest(spi = S3CheckpointSpi.class, group = "Checkpoint SPI")
@Ignore("https://issues.apache.org/jira/browse/IGNITE-2420")
public class S3CheckpointSpiSelfTest extends GridSpiAbstractTest<S3CheckpointSpi> {
    /** */
    private static final int CHECK_POINT_COUNT = 10;

    /** */
    private static final String KEY_PREFIX = "testCheckpoint";

    /** {@inheritDoc} */
    @Override protected void spiConfigure(S3CheckpointSpi spi) throws Exception {
        AwsCredentials cred = IgniteS3TestConfiguration.getAwsCredentials();

        spi.setAwsCredentials(cred);

        spi.setBucketNameSuffix(getBucketNameSuffix());

        super.spiConfigure(spi);
    }

    /**
     * @throws Exception If error.
     */
    @Override protected void afterSpiStopped() throws Exception {

        AwsCredentialsProvider credProvider = IgniteS3TestConfiguration.getAwsCredentialsProvider();

        S3Client s3 = S3Client.builder().credentialsProvider(credProvider).build();

        String bucketName = S3CheckpointSpi.BUCKET_NAME_PREFIX + "unit-test-bucket";

        try {
            ListObjectsV2Request request = ListObjectsV2Request.builder()
                    .bucket(bucketName)
                    .build();
            ListObjectsV2Iterable list = s3.listObjectsV2Paginator(request);


            ListObjectsV2Iterable responsePages = s3.listObjectsV2Paginator(request);

            // Iterate through each page of results
            for (ListObjectsV2Response response : responsePages) {
                // Delete each object in the current page
                response.contents().forEach(s3Object -> {
                    s3.deleteObject(DeleteObjectRequest.builder()
                            .bucket(bucketName)
                            .key(s3Object.key())
                            .build());
                });
            }
        }
        catch (SdkClientException e) {
            throw new IgniteSpiException("Failed to read checkpoint bucket: " + bucketName, e);
        }
    }

    /**
     * @throws Exception Thrown in case of any errors.
     */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-2420")
    @Test
    public void testSaveLoadRemoveWithoutExpire() throws Exception {
        String dataPrefix = "Test check point data ";

        // Save states.
        for (int i = 0; i < CHECK_POINT_COUNT; i++) {
            GridCheckpointTestState state = new GridCheckpointTestState(dataPrefix + i);

            getSpi().saveCheckpoint(KEY_PREFIX + i, GridTestIoUtils.serializeJdk(state), 0, true);
        }

        // Load and check states.
        for (int i = 0; i < CHECK_POINT_COUNT; i++) {
            final String key = KEY_PREFIX + i;

            assertWithRetries(new GridAbsClosureX() {
                @Override public void applyx() throws IgniteCheckedException {
                    assertNotNull("Missing checkpoint: " + key,
                        getSpi().loadCheckpoint(key));
                }
            });

            // Doing it again as pulling value from repeated assertion is tricky,
            // and all assertions below shouldn't be retried in case of failure.
            byte[] serState = getSpi().loadCheckpoint(key);

            GridCheckpointTestState state = GridTestIoUtils.deserializeJdk(serState);

            assertNotNull("Can't load checkpoint state for key: " + key, state);
            assertEquals("Invalid state loaded [expected='" + dataPrefix + i + "', received='" + state.getData() + "']",
                dataPrefix + i, state.getData());
        }

        // Remove states.
        for (int i = 0; i < CHECK_POINT_COUNT; i++) {
            final String key = KEY_PREFIX + i;

            assertWithRetries(new GridAbsClosureX() {
                @Override public void applyx() throws IgniteCheckedException {
                    assertTrue(getSpi().removeCheckpoint(key));
                }
            });
        }

        // Check that states was removed.
        for (int i = 0; i < CHECK_POINT_COUNT; i++) {
            final String key = KEY_PREFIX + i;

            assertWithRetries(new GridAbsClosureX() {
                @Override public void applyx() throws IgniteCheckedException {
                    assertNull(getSpi().loadCheckpoint(key));
                }
            });
        }
    }

    /**
     * @throws Exception Thrown in case of any errors.
     */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-2420")
    @Test
    public void testSaveWithExpire() throws Exception {
        // Save states.
        for (int i = 0; i < CHECK_POINT_COUNT; i++) {
            GridCheckpointTestState state = new GridCheckpointTestState("Test check point data " + i + '.');

            getSpi().saveCheckpoint(KEY_PREFIX + i, GridTestIoUtils.serializeJdk(state), 1, true);
        }

        // For small expiration intervals no warranty that state will be removed.
        Thread.sleep(100);

        // Check that states was removed.
        for (int i = 0; i < CHECK_POINT_COUNT; i++) {
            final String key = KEY_PREFIX + i;

            assertWithRetries(new GridAbsClosureX() {
                @Override public void applyx() throws IgniteCheckedException {
                    assertNull("Checkpoint state should not be loaded with key: " + key,
                        getSpi().loadCheckpoint(key));
                }
            });
        }
    }

    /**
     * @throws Exception Thrown in case of any errors.
     */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-2420")
    @Test
    public void testDuplicates() throws Exception {
        int idx1 = 1;
        int idx2 = 2;

        GridCheckpointTestState state1 = new GridCheckpointTestState(Integer.toString(idx1));
        GridCheckpointTestState state2 = new GridCheckpointTestState(Integer.toString(idx2));

        getSpi().saveCheckpoint(KEY_PREFIX, GridTestIoUtils.serializeJdk(state1), 0, true);
        getSpi().saveCheckpoint(KEY_PREFIX, GridTestIoUtils.serializeJdk(state2), 0, true);

        assertWithRetries(new GridAbsClosureX() {
            @Override public void applyx() throws IgniteCheckedException {
                assertNotNull(getSpi().loadCheckpoint(KEY_PREFIX));
            }
        });

        byte[] serState = getSpi().loadCheckpoint(KEY_PREFIX);

        GridCheckpointTestState state = GridTestIoUtils.deserializeJdk(serState);

        assertNotNull(state);
        assertEquals(state2, state);

        // Remove.
        getSpi().removeCheckpoint(KEY_PREFIX);

        assertWithRetries(new GridAbsClosureX() {
            @Override public void applyx() throws IgniteCheckedException {
                assertNull(getSpi().loadCheckpoint(KEY_PREFIX));
            }
        });
    }

    /**
     * Wrapper around {@link GridTestUtils#retryAssert(org.apache.ignite.IgniteLogger, int, long, GridAbsClosure)}.
     * Provides s3-specific timeouts.
     * @param assertion Closure with assertion inside.
     * @throws org.apache.ignite.internal.IgniteInterruptedCheckedException If was interrupted.
     */
    private void assertWithRetries(GridAbsClosureX assertion) throws IgniteInterruptedCheckedException {
        GridTestUtils.retryAssert(log, 6, 5000, assertion);
    }

    /**
     * Gets a Bucket name suffix
     * Bucket name suffix should be unique for the host to parallel test run on one bucket.
     * Please note that the final bucket name should not exceed 63 chars.
     *
     * @return Bucket name suffix.
     */
    static String getBucketNameSuffix() {
        String bucketNameSuffix;
        try {
            bucketNameSuffix = IgniteS3TestConfiguration.getBucketName(
                "unit-test-" + InetAddress.getLocalHost().getHostName().toLowerCase());
        }
        catch (UnknownHostException e) {
            bucketNameSuffix = IgniteS3TestConfiguration.getBucketName(
                "unit-test-rnd-" + ThreadLocalRandom.current().nextInt(100));
        }

        return bucketNameSuffix;
    }
}
