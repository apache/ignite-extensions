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

import org.apache.ignite.spi.aws2.S3Utils;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import java.util.*;

/**
 * Class to simulate the functionality of {@link S3Client}.
 */
public class DummyS3Client implements S3Client {
    /** Map of Bucket names as keys and the keys as set of values. */
    private final Map<String, Set<String>> objMap;

    /**
     * Constructor.
     */
    public DummyS3Client() {
        this.objMap = new HashMap<>();
    }

    @Override
    public String serviceName() {
        return "S3";
    }

    @Override
    public void close() {

    }

    /**
     * Constructor to add an object map with fake data.
     */
    public DummyS3Client(Map<String, Set<String>> objMap) {
        this.objMap = Objects.requireNonNull(objMap, "Object map cannot be null");
    }


    @Override
    public ListObjectsResponse listObjects(ListObjectsRequest listObjectsRequest) throws NoSuchBucketException, AwsServiceException, software.amazon.awssdk.core.exception.SdkClientException, S3Exception {
        checkBucketExists(listObjectsRequest.bucket());
        return DummyListObjectsResponseProvider.from(listObjectsRequest, objMap.get(listObjectsRequest.bucket()));
    }

    @Override
    public ListObjectsV2Response listObjectsV2(ListObjectsV2Request listObjectsRequest) throws NoSuchBucketException, AwsServiceException, SdkClientException, S3Exception {
        checkBucketExists(listObjectsRequest.bucket());
        return DummyListObjectsResponseProvider.v2from(listObjectsRequest, objMap.get(listObjectsRequest.bucket()));
    }

    @Override
    public CreateBucketResponse createBucket(CreateBucketRequest r) throws BucketAlreadyExistsException, BucketAlreadyOwnedByYouException, AwsServiceException, SdkClientException, S3Exception {
        String bucketName = r.bucket();
        if( objMap.containsKey( bucketName)){
            throw BucketAlreadyExistsException.builder().message("The specified bucket already exist")
                    .statusCode(409)
                    .build();
        }
        objMap.put(bucketName, new HashSet<>());
        return CreateBucketResponse.builder()
                .build();
    }



    @Override
    public HeadBucketResponse headBucket(HeadBucketRequest headBucketRequest) throws NoSuchBucketException, AwsServiceException, SdkClientException, S3Exception {
        if( objMap.containsKey(headBucketRequest.bucket()) ){
            return HeadBucketResponse.builder().build();
        }
        NoSuchBucketException sdkException = NoSuchBucketException.builder()
                .statusCode(404)
                .message("The specified bucket [" + headBucketRequest.bucket() + "] does not exist")
                .build();
        throw sdkException;
    }

    @Override
    public PutObjectResponse putObject(PutObjectRequest putObjectRequest, RequestBody requestBody) throws InvalidRequestException, InvalidWriteOffsetException, TooManyPartsException, EncryptionTypeMismatchException, AwsServiceException, SdkClientException, S3Exception {
        String bucketName = putObjectRequest.bucket();
        checkBucketExists(bucketName);
        Set<String> keys = objMap.get(bucketName);
        keys.add(putObjectRequest.key());
        return PutObjectResponse.builder().build();
    }

    @Override
    public DeleteObjectResponse deleteObject(DeleteObjectRequest deleteObjectRequest) throws AwsServiceException, SdkClientException, S3Exception {
        String bucketName = deleteObjectRequest.bucket();
        checkBucketExists(bucketName);
        Set<String> keys = objMap.get(bucketName);
        keys.remove(deleteObjectRequest.key());
        return DeleteObjectResponse.builder().build();
    }


    /**
     * Check if a bucket exists.
     *
     * @param bucketName bucket name to check.
     * @throws NoSuchBucketException If the specified bucket does not exist.
     */
    private void checkBucketExists(String bucketName) {
        if( objMap.containsKey(bucketName) ){
            return;
        }

        throw  NoSuchBucketException.builder().message("The specified bucket ["+bucketName+"] does not exist")
                .statusCode(404).build();
    }
}
