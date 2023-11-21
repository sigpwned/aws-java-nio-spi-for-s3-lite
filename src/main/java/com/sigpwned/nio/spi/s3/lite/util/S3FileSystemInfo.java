package com.sigpwned.nio.spi.s3.lite.util;

import java.net.URI;
import java.util.Objects;
import com.sigpwned.aws.sdk.lite.s3.internal.BucketUtils;

/**
 * <p>
 * Populates fields with information extracted by the S3 URI provided. This implementation is for
 * standard AWS buckets as described in section "Accessing a bucket using S3://"
 * <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/access-bucket-intro.html">here</a>
 * <p>
 *
 * <p>
 * It also computes the file system key that can be used to identify a runtime instance of a
 * S3FileSystem (for caching purposes for example). In this implementation the key is the bucket
 * name (which is unique in the AWS S3 namespace).
 * </p>
 *
 * <p>
 * Borrwed with love from s3-nio.
 * </p>
 */
public class S3FileSystemInfo {
  /**
   * syntax: s3://$BUCKET/$KEY
   */
  public static final String S3_SCHEME = "s3";

  /**
   * syntax: s3x://[$USER:$PASS@]$ENDPOINT/$KEY
   */
  public static final String S3X_SCHEME = "s3x";

  public static S3FileSystemInfo fromUri(URI uri) {
    if (uri == null)
      throw new IllegalArgumentException("uri can not be null");
    switch (uri.getScheme().toLowerCase()) {
      case S3_SCHEME:
        return new S3FileSystemInfo(uri.getAuthority(), null, uri.getAuthority(), null, null);
      case S3X_SCHEME:
        String accessKey, accessSecret;
        if (uri.getUserInfo() != null) {
          int pos = uri.getUserInfo().indexOf(':');
          accessKey = (pos < 0) ? uri.getUserInfo() : uri.getUserInfo().substring(0, pos);
          accessSecret = (pos < 0) ? null : uri.getUserInfo().substring(pos + 1);
        } else {
          accessKey = accessSecret = null;
        }

        String endpoint = uri.getHost();
        if (uri.getPort() > 0) {
          endpoint += ":" + uri.getPort();
        }

        String bucket = uri.getPath().split("/")[1];

        String key = endpoint + '/' + bucket;
        if (accessKey != null) {
          key = accessKey + '@' + key;
        }

        return new S3FileSystemInfo(key, endpoint, bucket, accessKey, accessSecret);
      default:
        throw new IllegalArgumentException("unrecognized scheme " + uri.getScheme());
    }
  }

  /**
   * This is a unique key for this filesystem, not the S3 URI path.
   */
  private final String key;
  private final String endpoint;
  private final String bucket;
  private final String accessKey;
  private final String accessSecret;

  /**
   * Creates a new instance and populates it with key and bucket. The name of the bucket must follow
   * AWS S3
   * <a href="https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucketnamingrules.html">bucket
   * naming rules</a>)
   *
   * @param uri a S3 URI
   * @throws IllegalArgumentException if URI contains invalid components (e.g. an invalid bucket
   *         name)
   */
  public S3FileSystemInfo(String key, String endpoint, String bucket, String accessKey,
      String accessSecret) {
    BucketUtils.isValidDnsBucketName(bucket, true);
    this.key = key;
    this.endpoint = endpoint;
    this.bucket = bucket;
    this.accessKey = accessKey;
    this.accessSecret = accessSecret;
  }

  public String key() {
    return key;
  }

  public String endpoint() {
    return endpoint;
  }

  public String bucket() {
    return bucket;
  }

  public String accessKey() {
    return accessKey;
  }

  public String accessSecret() {
    return accessSecret;
  }

  @Override
  public int hashCode() {
    return Objects.hash(accessKey, accessSecret, bucket, endpoint, key);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    S3FileSystemInfo other = (S3FileSystemInfo) obj;
    return Objects.equals(accessKey, other.accessKey)
        && Objects.equals(accessSecret, other.accessSecret) && Objects.equals(bucket, other.bucket)
        && Objects.equals(endpoint, other.endpoint) && Objects.equals(key, other.key);
  }

  @Override
  public String toString() {
    return "S3FileSystemInfo [key=" + key + ", endpoint=" + endpoint + ", bucket=" + bucket
        + ", accessKey=" + (accessKey != null ? "****" : null) + ", accessSecret="
        + (accessSecret != null ? "****" : null) + "]";
  }
}
