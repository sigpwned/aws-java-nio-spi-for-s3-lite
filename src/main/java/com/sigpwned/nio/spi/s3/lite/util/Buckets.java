package com.sigpwned.nio.spi.s3.lite.util;

import java.io.IOException;
import java.io.UncheckedIOException;
import com.sigpwned.aws.sdk.lite.s3.S3Client;
import com.sigpwned.aws.sdk.lite.s3.exception.NoSuchBucketException;
import com.sigpwned.aws.sdk.lite.s3.model.HeadBucketRequest;
import com.sigpwned.aws.sdk.lite.s3.model.HeadBucketResponse;

public final class Buckets {
  private Buckets() {}

  /**
   * @throws NoSuchBucketException if there is no such bucket
   * @throws UncheckedIOException if there is any other problem finding region
   */
  public static String getBucketRegion(S3Client client, String bucketName) {
    HeadBucketResponse response;
    try {
      response = client.headBucket(HeadBucketRequest.builder().bucket(bucketName).build());
    } catch (NoSuchBucketException e) {
      throw e;
    } catch (Exception e) {
      throw new UncheckedIOException(new IOException("Failed to fetch bucket metadata", e));
    }
    return response.sdkHttpResponse().firstMatchingHeader("x-amz-bucket-region")
        .orElseThrow(() -> new UncheckedIOException(new IOException("No region header")));
  }
}
