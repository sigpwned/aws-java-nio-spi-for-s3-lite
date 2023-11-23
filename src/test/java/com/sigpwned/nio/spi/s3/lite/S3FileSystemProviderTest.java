package com.sigpwned.nio.spi.s3.lite;

import static java.lang.String.format;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;
import com.sigpwned.aws.sdk.lite.core.Endpoint;
import com.sigpwned.aws.sdk.lite.core.credentials.AwsBasicCredentials;
import com.sigpwned.aws.sdk.lite.core.credentials.AwsCredentials;
import com.sigpwned.aws.sdk.lite.core.io.RequestBody;
import com.sigpwned.aws.sdk.lite.s3.S3Client;
import com.sigpwned.aws.sdk.lite.s3.S3ClientBuilder;
import com.sigpwned.aws.sdk.lite.s3.model.CreateBucketRequest;
import com.sigpwned.aws.sdk.lite.s3.model.GetObjectRequest;
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

    final S3ClientBuilder<?> defaultS3ClientBuilder =
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
}
