package org.apache.ignite.spi.discovery.tcp.ipfinder.s3_v2.client;


import software.amazon.awssdk.services.s3.model.*;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class DummyListObjectsResponseProvider {
    public static ListObjectsResponse from(ListObjectsRequest listObjectsRequest, Set<String> allKeys) {
        ListObjectsResponse.Builder builder = ListObjectsResponse.builder();
        builder.marker("m1");
        String prefix = listObjectsRequest.prefix();
        Set<String> keys = allKeys;
        if(prefix!=null && !prefix.isEmpty()) {
            keys = allKeys.stream().filter(key -> key.startsWith(prefix)).collect(Collectors.toSet());
        }
        List<S3Object> s3Objects = keys.stream().map(key -> S3Object.builder().key(key).build()).collect(Collectors.toList());
        builder.contents(s3Objects);
        builder.isTruncated(true);
        return builder.build();
    }

    public static ListObjectsV2Response v2from(ListObjectsV2Request listObjectsRequest, Set<String> allKeys) {
        ListObjectsV2Response.Builder builder = ListObjectsV2Response.builder();
        String prefix = listObjectsRequest.prefix();
        Set<String> keys = allKeys;
        if(prefix!=null && !prefix.isEmpty()) {
            keys = allKeys.stream().filter(key -> key.startsWith(prefix)).collect(Collectors.toSet());
        }
        List<S3Object> s3Objects = keys.stream().map(key -> S3Object.builder().key(key).build()).collect(Collectors.toList());
        builder.contents(s3Objects);

        return builder.build();
    }
}
