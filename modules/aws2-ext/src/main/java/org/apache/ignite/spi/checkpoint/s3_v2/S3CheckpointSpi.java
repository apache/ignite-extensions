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

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.spi.*;
import org.apache.ignite.spi.checkpoint.CheckpointListener;
import org.apache.ignite.spi.checkpoint.CheckpointSpi;
import org.jetbrains.annotations.Nullable;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.*;

/**
 * This class defines Amazon S3-based implementation for checkpoint SPI.
 * <p>
 * For information about Amazon S3 visit <a href="http://aws.amazon.com">aws.amazon.com</a>.
 * <p>
 * <h1 class="header">Configuration</h1>
 * <h2 class="header">Mandatory</h2>
 * This SPI has one mandatory configuration parameter:
 * <ul>
 *      <li>AWS credentials (see {@link #setAwsCredentials(AwsCredentials)}
 * </ul>
 * <h2 class="header">Optional</h2>
 * This SPI has following optional configuration parameters:
 * <ul>
 *      <li>Bucket name suffix (see {@link #setBucketNameSuffix(String)})</li>
 *      <li>Client configuration (see {@link #setClientConfiguration(ClientOverrideConfiguration)})</li>
 *      <li>Bucket endpoint (see {@link #setBucketEndpoint(String)})</li>
 *      <li>Server side encryption algorithm (see {@link #setSSEAlgorithm(String)})</li>
 * </ul>
 * <h2 class="header">Java Example</h2>
 * {@link S3CheckpointSpi} can be configured as follows:
 * <pre name="code" class="java">
 * IgniteConfiguration cfg = new IgniteConfiguration();
 *
 * S3CheckpointSpi spi = new S3CheckpointSpi();
 *
 * AWSCredentials cred = new BasicAWSCredentials(YOUR_ACCESS_KEY_ID, YOUR_SECRET_ACCESS_KEY);
 *
 * spi.setAwsCredentials(cred);
 *
 * spi.setBucketNameSuffix("checkpoints");
 *
 * // Override default checkpoint SPI.
 * cfg.setCheckpointSpi(cpSpi);
 *
 * // Start grid.
 * G.start(cfg);
 * </pre>
 * <h2 class="header">Spring Example</h2>
 * {@link S3CheckpointSpi} can be configured from Spring XML configuration file:
 * <pre name="code" class="xml">
 * &lt;bean id="grid.custom.cfg" class="org.apache.ignite.configuration.IgniteConfiguration" singleton="true"&gt;
 *     ...
 *        &lt;property name=&quot;checkpointSpi&quot;&gt;
 *            &lt;bean class=&quot;org.apache.ignite.spi.checkpoint.s3.S3CheckpointSpi&quot;&gt;
 *                &lt;property name=&quot;awsCredentials&quot;&gt;
 *                    &lt;bean class=&quot;com.amazonaws.auth.BasicAWSCredentials&quot;&gt;
 *                        &lt;constructor-arg value=&quot;YOUR_ACCESS_KEY_ID&quot; /&gt;
 *                        &lt;constructor-arg value=&quot;YOUR_SECRET_ACCESS_KEY&quot; /&gt;
 *                    &lt;/bean&gt;
 *                &lt;/property&gt;
 *            &lt;/bean&gt;
 *        &lt;/property&gt;
 *     ...
 * &lt;/bean&gt;
 * </pre>
 * <p>
 * Note that storing data in AWS S3 service will result in charges to your AWS account.
 * Choose another implementation of {@link org.apache.ignite.spi.checkpoint.CheckpointSpi} for local or
 * home network tests.
 * <p>
 * <img src="http://ignite.apache.org/images/spring-small.png">
 * <br>
 * For information about Spring framework visit <a href="http://www.springframework.org/">www.springframework.org</a>
 * @see org.apache.ignite.spi.checkpoint.CheckpointSpi
 */
@IgniteSpiMultipleInstancesSupport(true)
public class S3CheckpointSpi extends IgniteSpiAdapter implements CheckpointSpi {
    /** Logger. */
    @SuppressWarnings({"FieldAccessedSynchronizedAndUnsynchronized"})
    @LoggerResource
    private IgniteLogger log;

    /** Ignite instance. */
    @IgniteInstanceResource
    private Ignite ignite;

    /** Task that takes care about outdated files. */
    private S3TimeoutWorker timeoutWrk;

    /** Listener. */
    private CheckpointListener lsnr;

    /** Prefix to use in bucket name generation. */
    public static final String BUCKET_NAME_PREFIX = "ignite-checkpoint-";

    /** Suffix to use in bucket name generation. */
    public static final String DFLT_BUCKET_NAME_SUFFIX = "default-bucket";

    /** Client to interact with S3 storage. */
    @GridToStringExclude
    private S3Client s3;

    /** Bucket name suffix (set by user). */
    private String bucketNameSuffix;

    /** Bucket name (generated). */
    private String bucketName;

    /**
     * AWS region.
     */
    private String awsRegion = "us-east-1";

    /** Bucket endpoint (set by user). */
    @Nullable private String bucketEndpoint;

    /** Server side encryption algorithm */
    @Nullable private String sseAlg;

    /** Amazon client configuration. */
    private ClientOverrideConfiguration cfgOverride;

    /**
     * AWS Credentials.
     */
    @GridToStringExclude
    private AwsCredentials cred;

    /**
     * AWS Credentials.
     */
    @GridToStringExclude
    private AwsCredentialsProvider credProvider;

    /** Mutex. */
    private final Object mux = new Object();

    /**
     * Gets S3 bucket name to use.
     *
     * @return S3 bucket name to use.
     */
    public String getBucketName() {
        return bucketName;
    }

    /**
     * Gets S3 bucket endpoint to use.
     *
     * @return S3 bucket endpoint to use.
     */
    @Nullable public String getBucketEndpoint() {
        return bucketEndpoint;
    }

    /**
     * Gets S3 server-side encryption algorithm.
     *
     * @return S3 server-side encryption algorithm to use.
     */
    @Nullable public String getSSEAlgorithm() {
        return sseAlg;
    }

    /**
     * Gets S3 access key.
     *
     * @return S3 access key.
     */
    public String getAccessKey() {
        return cred.accessKeyId();
    }

    /**
     * Gets S3 secret key.
     *
     * @return S3 secret key.
     */
    public String getSecretAccessKey() {
        return cred.secretAccessKey();
    }


    public String getAwsRegion() {
        return awsRegion;
    }

    public S3CheckpointSpi setAwsRegion(String awsRegion) {
        this.awsRegion = awsRegion;
        return this;
    }

    /**
     * Sets bucket name suffix.
     *
     * @param bucketNameSuffix Bucket name suffix.
     * @return {@code this} for chaining.
     */
    @IgniteSpiConfiguration(optional = true)
    public S3CheckpointSpi setBucketNameSuffix(String bucketNameSuffix) {
        this.bucketNameSuffix = bucketNameSuffix;

        return this;
    }

    /**
     * Sets bucket endpoint.
     * If the endpoint is not set then S3CheckpointSpi will go to each region to find a corresponding bucket.
     * For information about possible endpoint names visit
     * <a href="http://docs.aws.amazon.com/general/latest/gr/rande.html#s3_region">docs.aws.amazon.com</a>
     *
     * @param bucketEndpoint Bucket endpoint, for example, {@code }s3.us-east-2.amazonaws.com.
     * @return {@code this} for chaining.
     */
    @IgniteSpiConfiguration(optional = true)
    public S3CheckpointSpi setBucketEndpoint(String bucketEndpoint) {
        this.bucketEndpoint = bucketEndpoint;

        return this;
    }

    /**
     * Sets server-side encryption algorithm for Amazon S3-managed encryption keys.
     * For information about possible S3-managed encryption keys visit
     * <a href="http://docs.aws.amazon.com/AmazonS3/latest/dev/UsingServerSideEncryption.html">docs.aws.amazon.com</a>.
     *
     * @param sseAlg Server-side encryption algorithm, for example, AES256 or SSES3.
     * @return {@code this} for chaining.
     */
    @IgniteSpiConfiguration(optional = true)
    public S3CheckpointSpi setSSEAlgorithm(String sseAlg) {
        this.sseAlg = sseAlg;

        return this;
    }

    /**
     * Sets Amazon client configuration.
     * <p>
     * For details refer to Amazon S3 API reference.
     *
     * @param cfg Amazon client configuration.
     * @return {@code this} for chaining.
     */
    @IgniteSpiConfiguration(optional = true)
    public S3CheckpointSpi setClientConfiguration(ClientOverrideConfiguration cfg) {
        this.cfgOverride = cfg;

        return this;
    }

    /**
     * Sets AWS credentials.
     * <p>
     * For details refer to Amazon S3 API reference.
     *
     * @param cred AWS credentials.
     * @return {@code this} for chaining.
     */
    @IgniteSpiConfiguration(optional = false)
    public S3CheckpointSpi setAwsCredentials(AwsCredentials cred) {
        this.cred = cred;

        return this;
    }

    /**
     * Instantiates {@code AmazonS3Client} instance.
     *
     * @return Client instance to use to connect to AWS.
     */
    S3Client createAmazonS3Client() {
        S3ClientBuilder builder = S3Client.builder();

        // Set credentials
        if (cred != null) {
            builder.credentialsProvider(StaticCredentialsProvider.create(cred));
        } else if (credProvider != null) {
            builder.credentialsProvider(credProvider);
        }
        if (cfgOverride != null) {
            builder.overrideConfiguration(cfgOverride);
        }

        if (!F.isEmpty(bucketEndpoint)) {
            builder.endpointOverride(URI.create(bucketEndpoint));
        }
        builder.region(Region.of(awsRegion));

        return builder.build();
    }

    private boolean doesBucketExist(String bucketName) {
        try {
            s3.headBucket(HeadBucketRequest.builder().bucket(bucketName).build());
            return true;
        } catch (S3Exception e) {
            // Bucket doesn't exist if a 404 status code is returned
            if (e.statusCode() == 404) {
                return false;
            }
            throw e; // Re-throw other exceptions
        }
    }

    /** {@inheritDoc} */
    @Override public void spiStart(String igniteInstanceName) throws IgniteSpiException {
        // Start SPI start stopwatch.
        startStopwatch();

        assertParameter(cred != null, "awsCredentials != null");

        if (log.isDebugEnabled()) {
            log.debug(configInfo("awsCredentials", cred));
            log.debug(configInfo("clientConfiguration", cfgOverride));
            log.debug(configInfo("bucketNameSuffix", bucketNameSuffix));
            log.debug(configInfo("bucketEndpoint", bucketEndpoint));
            log.debug(configInfo("SSEAlgorithm", sseAlg));
        }

        if (cfgOverride == null)
            U.warn(log, "Amazon client configuration is not set (will use default).");

        if (F.isEmpty(bucketNameSuffix)) {
            U.warn(log, "Bucket name suffix is null or empty (will use default bucket name).");

            bucketName = BUCKET_NAME_PREFIX + DFLT_BUCKET_NAME_SUFFIX;
        }
        else
            bucketName = BUCKET_NAME_PREFIX + bucketNameSuffix;

        s3 = createAmazonS3Client();
        

        if (!doesBucketExist(bucketName)) {
            try {
                s3.createBucket(CreateBucketRequest.builder()
                        .bucket(bucketName)
                        .build());

                if (log.isDebugEnabled())
                    log.debug("Created S3 bucket: " + bucketName);

                while (!doesBucketExist(bucketName))
                    try {
                        U.sleep(200);
                    }
                    catch (IgniteInterruptedCheckedException e) {
                        throw new IgniteSpiException("Thread has been interrupted.", e);
                    }
            }
            catch (SdkClientException e) {
                try {
                    if (!doesBucketExist(bucketName))
                        throw new IgniteSpiException("Failed to create bucket: " + bucketName, e);
                }
                catch (SdkClientException ignored) {
                    throw new IgniteSpiException("Failed to create bucket: " + bucketName, e);
                }
            }
        }

        Collection<S3TimeData> s3TimeDataLst = new LinkedList<>();

        try {
            ListObjectsV2Request request = ListObjectsV2Request.builder()
                    .bucket(bucketName)
                    .build();
            ListObjectsV2Iterable responsePages = s3.listObjectsV2Paginator(request);

            // Process each page of results
            for (ListObjectsV2Response page : responsePages) {
                for (S3Object s3Object : page.contents()) {
                    S3CheckpointData data = read(s3Object.key());

                    if (data != null) {
                        s3TimeDataLst.add(new S3TimeData(data.getExpireTime(), data.getKey()));

                        if (log.isDebugEnabled()) {
                            log.debug("Registered existing checkpoint from key: " + data.getKey());
                        }
                    }
                }
            }
        }
        catch (SdkClientException e) {
            throw new IgniteSpiException("Failed to read checkpoint bucket: " + bucketName, e);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteSpiException("Failed to marshal/unmarshal objects in bucket: " + bucketName, e);
        }

        // Track expiration for only those data that are made by this node
        timeoutWrk = new S3TimeoutWorker();

        timeoutWrk.add(s3TimeDataLst);

        timeoutWrk.start();

        registerMBean(igniteInstanceName, new S3CheckpointSpiMBeanImpl(this), S3CheckpointSpiMBean.class);

        // Ack ok start.
        if (log.isDebugEnabled())
            log.debug(startInfo());
    }

    /** {@inheritDoc} */
    @Override public void spiStop() throws IgniteSpiException {
        if (timeoutWrk != null) {
            IgniteUtils.interrupt(timeoutWrk);
            IgniteUtils.join(timeoutWrk, log);
        }

        unregisterMBean();

        // Ack ok stop.
        if (log.isDebugEnabled())
            log.debug(stopInfo());
    }

    /** {@inheritDoc} */
    @Override public byte[] loadCheckpoint(String key) throws IgniteSpiException {
        assert !F.isEmpty(key);

        try {
            S3CheckpointData data = read(key);

            return data != null ?
                data.getExpireTime() == 0 || data.getExpireTime() > U.currentTimeMillis() ?
                    data.getState() :
                    null :
                null;
        }
        catch (SdkClientException e) {
            throw new IgniteSpiException("Failed to read checkpoint key: " + key, e);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteSpiException("Failed to marshal/unmarshal objects in checkpoint key: " + key, e);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean saveCheckpoint(String key, byte[] state, long timeout, boolean overwrite)
        throws IgniteSpiException {
        assert !F.isEmpty(key);

        long expireTime = 0;

        if (timeout > 0) {
            expireTime = U.currentTimeMillis() + timeout;

            if (expireTime < 0)
                expireTime = Long.MAX_VALUE;
        }

        try {
            if (hasKey(key)) {
                if (!overwrite)
                    return false;

                if (log.isDebugEnabled())
                    log.debug("Overriding existing key: " + key);
            }

            S3CheckpointData data = new S3CheckpointData(state, expireTime, key);

            write(data);
        }
        catch (SdkClientException e) {
            throw new IgniteSpiException("Failed to write checkpoint data [key=" + key + ", state=" +
                Arrays.toString(state) + ']', e);
        }
        catch (IgniteCheckedException e) {
            throw new IgniteSpiException("Failed to marshal checkpoint data [key=" + key + ", state=" +
                Arrays.toString(state) + ']', e);
        }

        if (timeout > 0)
            timeoutWrk.add(new S3TimeData(expireTime, key));

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean removeCheckpoint(String key) {
        assert !F.isEmpty(key);

        timeoutWrk.remove(key);

        boolean rmv = false;

        try {
            rmv = delete(key);
        }
        catch (SdkClientException e) {
            U.error(log, "Failed to delete data by key: " + key, e);
        }

        if (rmv) {
            CheckpointListener tmpLsnr = lsnr;

            if (tmpLsnr != null)
                tmpLsnr.onCheckpointRemoved(key);
        }

        return rmv;
    }

    /**
     * Reads checkpoint data.
     *
     * @param key Key name to read data from.
     * @return Checkpoint data object.
     * @throws IgniteCheckedException Thrown if an error occurs while unmarshalling.
     * @throws SdkClientException If an error occurs while querying Amazon S3.
     */
    @Nullable private S3CheckpointData read(String key) throws IgniteCheckedException, SdkClientException {
        assert !F.isEmpty(key);

        if (log.isDebugEnabled())
            log.debug("Reading data from S3 [bucket=" + bucketName + ", key=" + key + ']');

        try {
            GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .build();
            ResponseInputStream<GetObjectResponse> objectResponse = s3.getObject(getObjectRequest);
            InputStream in = objectResponse;
            try  {
                return S3CheckpointData.fromStream(in);
            }
            catch (IOException e) {
                throw new IgniteCheckedException("Failed to unmarshal S3CheckpointData [bucketName=" +
                    bucketName + ", key=" + key + ']', e);
            }
            finally {
                U.closeQuiet(in);
            }
        }
        catch (AwsServiceException e) {
            if (e.statusCode() != 404)
                throw e;
        }
        return null;
    }

    /**
     * Writes given checkpoint data to a given S3 bucket. Data is serialized to
     * the binary stream and saved to the S3.
     *
     * @param data Checkpoint data.
     * @throws IgniteCheckedException Thrown if an error occurs while marshalling.
     * @throws SdkClientException If an error occurs while querying Amazon S3.
     */
    private void write(S3CheckpointData data) throws IgniteCheckedException, SdkClientException {
        assert data != null;

        if (log.isDebugEnabled())
            log.debug("Writing data to S3 [bucket=" + bucketName + ", key=" + data.getKey() + ']');

        byte[] buf = data.toBytes();

        PutObjectRequest.Builder requestBuilder = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(data.getKey())
                .contentLength((long) buf.length); // Set content length

        // Add server-side encryption algorithm if provided
        if (sseAlg != null && !sseAlg.isEmpty()) {
            requestBuilder.serverSideEncryption(sseAlg);
        }

        PutObjectRequest request = requestBuilder.build();

        // Upload the object
        s3.putObject(request, RequestBody.fromBytes(buf));
    }

    /**
     * Deletes checkpoint data.
     *
     * @param key Key of the data in storage.
     * @return {@code True} if operations succeeds and data is actually removed.
     * @throws SdkClientException If an error occurs while querying Amazon S3.
     */
    private boolean delete(String key) throws SdkClientException {
        assert !F.isEmpty(key);

        if (log.isDebugEnabled())
            log.debug("Removing data from S3 [bucket=" + bucketName + ", key=" + key + ']');

        if (!hasKey(key))
            return false;

        s3.deleteObject( dr -> dr.bucket(bucketName).key(key));

        return true;
    }

    /**
     * Returns {@code true} if mapping presents for the provided key.
     *
     * @param key Key to check mapping for.
     * @return {@code true} if mapping presents for key.
     * @throws SdkClientException If an error occurs while querying Amazon S3.
     */
    boolean hasKey(String key) throws SdkClientException {
        assert !F.isEmpty(key);

        try {
            HeadObjectRequest headObjectRequest = HeadObjectRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .build();

            // Get the metadata and check the content length
            HeadObjectResponse response = s3.headObject(headObjectRequest);
            return response.contentLength() != 0;
        }
        catch (AwsServiceException e) {
            if (e.statusCode() != 404)
                throw e;
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override public void setCheckpointListener(CheckpointListener lsnr) {
        this.lsnr = lsnr;
    }

    /** {@inheritDoc} */
    @Override public S3CheckpointSpi setName(String name) {
        super.setName(name);

        return this;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(S3CheckpointSpi.class, this);
    }

    /**
     * Implementation of {@link org.apache.ignite.spi.IgniteSpiThread} that takes care about outdated S3 data.
     * Every checkpoint has expiration date after which it makes no sense to
     * keep it. This worker periodically cleans S3 bucket according to checkpoints
     * expiration time.
     */
    private class S3TimeoutWorker extends IgniteSpiThread {
        /** List of data with access and expiration date. */
        private Map<String, S3TimeData> map = new HashMap<>();

        /**
         * Constructor.
         */
        S3TimeoutWorker() {
            super(ignite.name(), "grid-s3-checkpoint-worker", log);
        }

        /** {@inheritDoc} */
        @Override public void body() throws InterruptedException {
            long nextTime = 0;

            Collection<String> rmvKeys = new HashSet<>();

            while (!isInterrupted()) {
                rmvKeys.clear();

                synchronized (mux) {
                    long delay = U.currentTimeMillis() - nextTime;

                    if (nextTime != 0 && delay > 0)
                        mux.wait(delay);

                    long now = U.currentTimeMillis();

                    nextTime = -1;

                    // check map one by one and physically remove
                    // if (now - last modification date) > expiration time
                    for (Iterator<Map.Entry<String, S3TimeData>> iter = map.entrySet().iterator(); iter.hasNext();) {
                        Map.Entry<String, S3TimeData> entry = iter.next();

                        String key = entry.getKey();

                        S3TimeData timeData = entry.getValue();

                        if (timeData.getExpireTime() > 0)
                            if (timeData.getExpireTime() <= now) {
                                try {
                                    delete(key);

                                    if (log.isDebugEnabled())
                                        log.debug("Data was deleted by timeout: " + key);
                                }
                                catch (SdkClientException e) {
                                    U.error(log, "Failed to delete data by key: " + key, e);
                                }

                                iter.remove();

                                rmvKeys.add(timeData.getKey());
                            }
                            else if (timeData.getExpireTime() < nextTime || nextTime == -1)
                                nextTime = timeData.getExpireTime();
                    }
                }

                CheckpointListener tmpLsnr = lsnr;

                if (tmpLsnr != null)
                    for (String key : rmvKeys)
                        tmpLsnr.onCheckpointRemoved(key);
            }

            synchronized (mux) {
                map.clear();
            }
        }

        /**
         * Adds data to a list of files this task should look after.
         *
         * @param timeData File expiration and access information.
         */
        void add(S3TimeData timeData) {
            assert timeData != null;

            synchronized (mux) {
                map.put(timeData.getKey(), timeData);

                mux.notifyAll();
            }
        }

        /**
         * Adds list of data this task should look after.
         *
         * @param newData List of data.
         */
        void add(Iterable<S3TimeData> newData) {
            assert newData != null;

            synchronized (mux) {
                for (S3TimeData data : newData)
                    map.put(data.getKey(), data);

                mux.notifyAll();
            }
        }

        /**
         * Removes data.
         *
         * @param key Checkpoint key.
         */
        public void remove(String key) {
            assert key != null;

            synchronized (mux) {
                map.remove(key);
            }
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(S3TimeoutWorker.class, this);
        }
    }

    /**
     * MBean implementation for S3CheckpointSpi.
     */
    private class S3CheckpointSpiMBeanImpl extends IgniteSpiMBeanAdapter implements S3CheckpointSpiMBean {
        /** {@inheritDoc} */
        S3CheckpointSpiMBeanImpl(IgniteSpiAdapter spiAdapter) {
            super(spiAdapter);
        }

        /** {@inheritDoc} */
        @Override public String getBucketName() {
            return S3CheckpointSpi.this.getBucketName();
        }

        /** {@inheritDoc} */
        @Override public String getBucketEndpoint() {
            return S3CheckpointSpi.this.getBucketName();
        }

        /** {@inheritDoc} */
        @Override public String getSSEAlgorithm() {
            return S3CheckpointSpi.this.getSSEAlgorithm();
        }

        /** {@inheritDoc} */
        @Override public String getAccessKey() {
            return S3CheckpointSpi.this.getAccessKey();
        }

       
    }
}
