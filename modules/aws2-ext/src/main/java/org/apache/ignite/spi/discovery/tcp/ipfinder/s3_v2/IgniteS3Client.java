package org.apache.ignite.spi.discovery.tcp.ipfinder.s3_v2;


import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.F;
import org.jetbrains.annotations.Nullable;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;

import java.net.URI;

public class IgniteS3Client {


    /** Client to interact with S3 storage. */
    @GridToStringExclude
    private S3Client s3;

    /** Bucket name. */
    String bucketName;

    /** Bucket endpoint. */
    @Nullable
    private String bucketEndpoint;

    /** Server side encryption algorithm. */
    @Nullable String sseAlg;

    public S3Client getS3() {
        return s3;
    }


    public String getBucketName() {
        return bucketName;
    }

    public IgniteS3Client setBucketName(String bucketName) {
        this.bucketName = bucketName;
        return this;
    }

    public @Nullable String getBucketEndpoint() {
        return bucketEndpoint;
    }

    public IgniteS3Client setBucketEndpoint(@Nullable String bucketEndpoint) {
        this.bucketEndpoint = bucketEndpoint;
        return this;
    }

    public @Nullable String getSseAlg() {
        return sseAlg;
    }

    public IgniteS3Client setSseAlg(@Nullable String sseAlg) {
        this.sseAlg = sseAlg;
        return this;
    }

    public ClientOverrideConfiguration getCfgOverride() {
        return cfgOverride;
    }

    public IgniteS3Client setCfgOverride(ClientOverrideConfiguration cfgOverride) {
        this.cfgOverride = cfgOverride;
        return this;
    }

    public AwsCredentials getCred() {
        return cred;
    }

    public IgniteS3Client setCred(AwsCredentials cred) {
        this.cred = cred;
        return this;
    }

    public AwsCredentialsProvider getCredProvider() {
        return credProvider;
    }

    public IgniteS3Client setCredProvider(AwsCredentialsProvider credProvider) {
        this.credProvider = credProvider;
        return this;
    }

    public String getAwsRegion() {
        return awsRegion;
    }

    public IgniteS3Client setAwsRegion(String awsRegion) {
        this.awsRegion = awsRegion;
        return this;
    }

    /** Amazon client configuration. */
    private ClientOverrideConfiguration cfgOverride;

    /** AWS Credentials. */
    @GridToStringExclude
    private AwsCredentials cred;

    /** AWS Credentials. */
    @GridToStringExclude
    private AwsCredentialsProvider credProvider;

    /** AWS region. */
    private String awsRegion = "us-east-1";


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
}
