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
package com.sigpwned.nio.spi.s3.lite;

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toSet;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;
import com.sigpwned.aws.sdk.lite.core.Endpoint;
import com.sigpwned.aws.sdk.lite.core.auth.AwsCredentials;
import com.sigpwned.aws.sdk.lite.core.auth.credentials.AwsBasicCredentials;
import com.sigpwned.aws.sdk.lite.core.io.RequestBody;
import com.sigpwned.aws.sdk.lite.s3.S3Client;
import com.sigpwned.aws.sdk.lite.s3.S3ClientBuilder;
import com.sigpwned.aws.sdk.lite.s3.exception.NoSuchKeyException;
import com.sigpwned.aws.sdk.lite.s3.model.CreateBucketRequest;
import com.sigpwned.aws.sdk.lite.s3.model.GetObjectRequest;
import com.sigpwned.aws.sdk.lite.s3.model.HeadObjectRequest;
import com.sigpwned.aws.sdk.lite.s3.model.PutObjectRequest;
import com.sigpwned.httpmodel.core.util.MoreByteStreams;

public class S3FileSystemProviderTest {
  public S3Client client;

  @Rule
  public LocalStackContainer localstack =
      new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.0.0"))
          .withServices(LocalStackContainer.Service.S3);

  @Before
  public void setupS3FileSystemProviderTest() {
    final URI endpoint = localstack.getEndpoint();
    final AwsCredentials credentials =
        AwsBasicCredentials.of(localstack.getAccessKey(), localstack.getSecretKey());
    final String region = localstack.getRegion();

    final S3ClientBuilder defaultS3ClientBuilder =
        S3Client.builder().credentialsProvider(() -> credentials).region(region)
            .endpointProvider((endpointParams) -> Endpoint.builder().url(endpoint).build());

    client = defaultS3ClientBuilder.build();

    S3FileSystemProvider.setDefaultClientBuilderSupplier(() -> {
      return defaultS3ClientBuilder;
    });
  }

  @After
  public void cleanupS3FileSystemProviderTest() throws IOException {}

  @Test
  public void smokeTest() {}

  @Test
  public void readTest() throws IOException {
    final String bucketName = "example";
    final String key = "hello.txt";
    final String contents = "Hello, world!";

    client.createBucket(CreateBucketRequest.builder().bucket(bucketName).build());

    client.putObject(PutObjectRequest.builder().bucket(bucketName).key(key).build(),
        RequestBody.fromString(contents, StandardCharsets.UTF_8));

    String text;
    try (InputStream in = Files.newInputStream(Paths
        .get(URI.create(format("%s://%s/%s", S3FileSystemProvider.SCHEME, bucketName, key))))) {
      text = new String(MoreByteStreams.toByteArray(in), StandardCharsets.UTF_8);
    }

    assertThat(text, is(contents));
  }

  @Test
  public void writeTest() throws IOException {
    final String bucketName = "example";
    final String key = "hello.txt";
    final String contents = "Hello, world!";

    client.createBucket(CreateBucketRequest.builder().bucket(bucketName).build());

    try (OutputStream out = Files.newOutputStream(Paths
        .get(URI.create(format("%s://%s/%s", S3FileSystemProvider.SCHEME, bucketName, key))))) {
      out.write(contents.getBytes(StandardCharsets.UTF_8));
    }

    String text;
    try (InputStream in =
        client.getObject(GetObjectRequest.builder().bucket(bucketName).key(key).build())) {
      text = new String(MoreByteStreams.toByteArray(in), StandardCharsets.UTF_8);
    }

    assertThat(text, is(contents));
  }

  // TODO create file target exists test
  // TODO create file target not exists test
  // TODO truncate write
  // TODO append write target exists test
  // TODO append write target not exists test

  @Test
  public void copyTest() throws IOException {
    final String bucketName = "example";
    final String key1 = "hello.txt";
    final String key2 = "world.txt";
    final String contents = "Hello, world!";

    client.createBucket(CreateBucketRequest.builder().bucket(bucketName).build());

    client.putObject(PutObjectRequest.builder().bucket(bucketName).key(key1).build(),
        RequestBody.fromString(contents, StandardCharsets.UTF_8));

    Files.copy(
        Paths.get(URI.create(format("%s://%s/%s", S3FileSystemProvider.SCHEME, bucketName, key1))),
        Paths.get(URI.create(format("%s://%s/%s", S3FileSystemProvider.SCHEME, bucketName, key2))));

    String text;
    try (InputStream in =
        client.getObject(GetObjectRequest.builder().bucket(bucketName).key(key2).build())) {
      text = new String(MoreByteStreams.toByteArray(in), StandardCharsets.UTF_8);
    }

    assertThat(text, is(contents));
  }


  @Test
  public void listTest() throws IOException {
    final String bucketName = "example";
    final String directoryName = "alpha";
    final String fileName1 = "hello.txt";
    final String fileName2 = "world.txt";

    client.createBucket(CreateBucketRequest.builder().bucket(bucketName).build());

    client.putObject(
        PutObjectRequest.builder().bucket(bucketName)
            .key(directoryName + S3FileSystemProvider.SEPARATOR + fileName1).build(),
        RequestBody.fromString(fileName1, StandardCharsets.UTF_8));

    client.putObject(
        PutObjectRequest.builder().bucket(bucketName)
            .key(directoryName + S3FileSystemProvider.SEPARATOR + fileName2).build(),
        RequestBody.fromString(fileName2, StandardCharsets.UTF_8));

    Set<Path> contents;
    try (Stream<Path> dirstream = Files.list(Paths.get(URI
        .create(format("%s://%s/%s/", S3FileSystemProvider.SCHEME, bucketName, directoryName))))) {
      contents = dirstream.collect(toSet());
    }

    assertThat(contents,
        is(new HashSet<>(asList(
            Paths.get(URI.create(format("%s://%s/%s/%s", S3FileSystemProvider.SCHEME, bucketName,
                directoryName, fileName1))),
            Paths.get(URI.create(format("%s://%s/%s/%s", S3FileSystemProvider.SCHEME, bucketName,
                directoryName, fileName2)))))));
  }

  @Test(expected = NoSuchKeyException.class)
  public void deleteTest() throws IOException {
    final String bucketName = "example";
    final String key = "hello.txt";
    final String contents = "Hello, world!";

    client.createBucket(CreateBucketRequest.builder().bucket(bucketName).build());

    client.putObject(PutObjectRequest.builder().bucket(bucketName).key(key).build(),
        RequestBody.fromString(contents, StandardCharsets.UTF_8));

    try {
      client.headObject(HeadObjectRequest.builder().bucket(bucketName).key(key).build());
    } catch (NoSuchKeyException e) {
      throw new IOException("file not created", e);
    }

    Files.delete(
        Paths.get(URI.create(format("%s://%s/%s", S3FileSystemProvider.SCHEME, bucketName, key))));

    client.headObject(HeadObjectRequest.builder().bucket(bucketName).key(key).build());
  }
}
