package org.apache.ignite.spi.aws2;


import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

public class S3Utils {

    public static boolean doesBucketExist(S3Client s3, String bucketName) {
        try {
            s3.headBucket(HeadBucketRequest.builder().bucket(bucketName).build());
            return true;
        } catch (S3Exception e) {
//            https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadBucket.html
            // Bucket doesn't exist if a 404, 400, or 403 status code is returned
            if (e.statusCode() == 404 || e.statusCode() == 400 || e.statusCode() == 403) {
                return false;
            }
            throw e; // Re-throw other exceptions
        }
    }
}
