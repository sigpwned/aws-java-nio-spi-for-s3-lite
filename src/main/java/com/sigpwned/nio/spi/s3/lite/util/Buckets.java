/*-
 * =================================LICENSE_START==================================
 * AWS Java NIO SPI for S3 Lite
 * ====================================SECTION=====================================
 * Copyright (C) 2023 Andy Boothe
 * ====================================SECTION=====================================
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * ==================================LICENSE_END===================================
 */
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
