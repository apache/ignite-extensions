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

package org.apache.ignite.spi.discovery.tcp.ipfinder.s3_v2;

import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinderAbstractSelfTest;
import org.apache.ignite.spi.discovery.tcp.ipfinder.s3_v2.encrypt.EncryptionService;
import org.apache.ignite.util.IgniteS3TestConfiguration;
import org.jetbrains.annotations.Nullable;
import org.junit.Ignore;
import org.junit.Test;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Abstract TcpDiscoveryS3IpFinder to test with different ways of setting AWS credentials.
 */
public abstract class TcpDiscoveryS3IpFinderAbstractSelfTest
    extends TcpDiscoveryIpFinderAbstractSelfTest<TcpDiscoveryS3IpFinder> {
    /** Bucket endpoint */
    @Nullable protected String bucketEndpoint;

    /** Server-side encryption algorithm for Amazon S3-managed encryption keys. */
    @Nullable protected String SSEAlgorithm;

    /** Key prefix of the address. */
    @Nullable protected String keyPrefix;

    /** Encryption service. */
    @Nullable protected EncryptionService encryptionSvc;

    /**
     * Constructor.
     *
     * @throws Exception If any error occurs.
     */
    TcpDiscoveryS3IpFinderAbstractSelfTest() throws Exception {
    }

    /** {@inheritDoc} */
    @Override protected TcpDiscoveryS3IpFinder ipFinder() throws Exception {
        TcpDiscoveryS3IpFinder finder = new TcpDiscoveryS3IpFinder();

        resources.inject(finder);

        assert finder.isShared() : "Ip finder should be shared by default.";

        setAwsCredentials(finder);
        setBucketEndpoint(finder);
        setRegion(finder);
        setBucketName(finder);
        setSSEAlgorithm(finder);
        setKeyPrefix(finder);
        setEncryptionService(finder);

        for (int i = 0; i < 5; i++) {
            Collection<InetSocketAddress> addrs = finder.getRegisteredAddresses();

            if (!addrs.isEmpty())
                finder.unregisterAddresses(addrs);
            else
                return finder;

            U.sleep(1000);
        }

        if (!finder.getRegisteredAddresses().isEmpty())
            throw new Exception("Failed to initialize IP finder.");

        return finder;
    }

    /** {@inheritDoc} */
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-2420")
    @Test
    @Override public void testIpFinder() throws Exception {
        super.testIpFinder();
    }

    /**
     * Set AWS credentials into the provided {@code finder}.
     *
     * @param finder finder credentials to set into
     */
    protected abstract void setAwsCredentials(TcpDiscoveryS3IpFinder finder);

    /**
     * Set Bucket endpoint into the provided {@code finder}.
     *
     * @param finder finder endpoint to set into.
     */
    private void setBucketEndpoint(TcpDiscoveryS3IpFinder finder) {
        finder.setBucketEndpoint(bucketEndpoint);
    }

    /**
     * Set server-side encryption algorithm for Amazon S3-managed encryption keys into the provided {@code finder}.
     *
     * @param finder finder encryption algorithm to set into.
     */
    private void setSSEAlgorithm(TcpDiscoveryS3IpFinder finder) {
        finder.setSSEAlgorithm(SSEAlgorithm);
    }

    /**
     * Set Bucket endpoint into the provided {@code finder}.
     *
     * @param finder finder endpoint to set into.
     */
    protected void setBucketName(TcpDiscoveryS3IpFinder finder) {
        finder.setBucketName(getBucketName());
    }

    protected void setRegion(TcpDiscoveryS3IpFinder finder) {
        finder.setAwsRegion( getAwsRegion());
    }

    /**
     * Set the ip address key prefix into the provided {@code finder}.
     *
     * @param finder finder encryption algorithm to set into.
     */
    protected void setKeyPrefix(TcpDiscoveryS3IpFinder finder) {
        finder.setKeyPrefix(keyPrefix);
    }

    /**
     * Set encryption service into the provided {@code finder}.
     *
     * @param finder finder encryption service to set into.
     */
    protected void setEncryptionService(TcpDiscoveryS3IpFinder finder) {
        finder.setEncryptionService(encryptionSvc);
    }

    /**
     * Gets Bucket name. Bucket name should be unique for the host to parallel test run on one bucket. Please note that
     * the final bucket name should not exceed 63 chars.
     *
     * @return Bucket name.
     */
    static String getBucketName() {
        String bucketName;
        try {
            bucketName = IgniteS3TestConfiguration.getBucketName(
                "ip-finder-unit-test-" + InetAddress.getLocalHost().getHostName().toLowerCase());
        }
        catch (UnknownHostException e) {
            bucketName = IgniteS3TestConfiguration.getBucketName(
                "ip-finder-unit-test-rnd-" + ThreadLocalRandom.current().nextInt(100));
        }

        return bucketName;
    }

    static String getAwsRegion() {
        return IgniteS3TestConfiguration.getAwsRegion("us-west-2");
    }
}
