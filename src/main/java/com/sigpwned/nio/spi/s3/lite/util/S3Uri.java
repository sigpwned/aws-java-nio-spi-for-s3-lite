package com.sigpwned.nio.spi.s3.lite.util;

import static java.lang.String.format;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Objects;
import com.sigpwned.aws.sdk.lite.s3.internal.BucketUtils;

public class S3Uri {
  /**
   * syntax: s3x://[$KEY:$SECRET@]$ENDPOINT[:$PORT]/$BUCKET/$KEY
   */
  public static final String S3X_SCHEME = "s3x";

  /**
   * syntax: s3://$BUCKET/$KEY
   */
  public static final String S3_SCHEME = "s3";

  public static S3Uri fromString(String s) {
    return fromUri(URI.create(s));
  }

  public static S3Uri fromUri(URI uri) {
    switch (uri.getScheme()) {
      case S3Uri.S3_SCHEME:
        String[] s3PathParts = parseS3Path(uri.getRawPath());
        return new S3Uri(null, uri.getHost(), s3PathParts[0]);
      case S3Uri.S3X_SCHEME:
        String[] s3XPathParts = parseS3XPath(uri.getRawPath());
        return new S3Uri(S3Authority.fromString(uri.getRawAuthority()), s3XPathParts[0],
            s3XPathParts[1]);
      default:
        throw new IllegalArgumentException(
            format("Scheme must be %s or %s", S3Uri.S3_SCHEME, S3Uri.S3X_SCHEME));
    }
  }

  private static String[] parseS3Path(String path) {
    if (path.isEmpty())
      return new String[] {""};
    if (path.startsWith("/"))
      return new String[] {path.substring(1, path.length())};
    // { KEY }
    throw new IllegalArgumentException(format("%s path must be empty or start with /", S3_SCHEME));
  }

  private static String[] parseS3XPath(String path) {
    if (path.isEmpty())
      throw new IllegalArgumentException(format("%s path must not be empty", S3X_SCHEME));
    if (!path.startsWith("/"))
      throw new IllegalArgumentException(format("%s path must start with /", S3X_SCHEME));
    // { BUCKET, KEY }
    return path.substring(1, path.length()).split("/", 2);
  }

  private final S3Authority authority;
  private final String bucket;
  private final String key;

  /**
   * @param authority
   * @param bucket
   * @param key Should not start with a "/"
   */
  public S3Uri(S3Authority authority, String bucket, String key) {
    if (bucket == null)
      throw new NullPointerException();
    if (key == null)
      throw new NullPointerException();
    BucketUtils.isValidDnsBucketName(bucket, true);
    this.authority = authority;
    this.bucket = bucket;
    this.key = key;
  }

  public S3Authority getAuthority() {
    return authority;
  }

  public String getBucket() {
    return bucket;
  }

  public String getKey() {
    return key;
  }

  public String getId() {
    if (getAuthority() != null) {
      String result = getAuthority().getEndpoint();
      if (getAuthority().getUserInfo() != null)
        result = getAuthority().getUserInfo().accessKeyId() + "@" + result;
      return result;
    } else {
      return getBucket();
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(authority, bucket, key);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    S3Uri other = (S3Uri) obj;
    return Objects.equals(authority, other.authority) && Objects.equals(bucket, other.bucket)
        && Objects.equals(key, other.key);
  }

  public URI toUri() {
    try {
      return getAuthority() != null
          ? new URI(S3X_SCHEME, getAuthority().toString(), "/" + getBucket() + "/" + getKey(), null,
              null)
          : new URI(S3_SCHEME, getBucket(), "/" + getKey(), null, null);
    } catch (URISyntaxException e) {
      throw new UncheckedIOException(new IOException("S3Uri conversion failed", e));
    }
  }

  @Override
  public String toString() {
    return toUri().toString();
  }
}
