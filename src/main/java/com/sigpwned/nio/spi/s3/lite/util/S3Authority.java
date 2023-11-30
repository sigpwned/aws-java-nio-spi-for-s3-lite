package com.sigpwned.nio.spi.s3.lite.util;

import java.util.Objects;
import com.sigpwned.aws.sdk.lite.core.auth.credentials.AwsBasicCredentials;

public class S3Authority {
  public static S3Authority fromString(String s) {
    if (s == null)
      throw new NullPointerException();

    AwsBasicCredentials userInfo;
    String[] authorityParts = s.split("@", 2);
    if (authorityParts.length == 2) {
      String authorityUserInfoPart = authorityParts[0];
      String authorityEndpointPart = authorityParts[1];

      String[] userInfoParts = authorityUserInfoPart.split(":", 2);
      if (userInfoParts.length != 2)
        throw new IllegalArgumentException("S3 user info must have exactly two parts");

      String accessKey = UrlEncoding.urldecode(userInfoParts[0]);
      String secretKey = UrlEncoding.urldecode(userInfoParts[1]);

      userInfo = AwsBasicCredentials.of(accessKey, secretKey);

      authorityParts = new String[] {authorityEndpointPart};
    } else {
      userInfo = null;
    }

    String[] endpointParts = authorityParts[0].split(":", 2);

    String host = endpointParts[0];

    Integer port;
    if (endpointParts.length == 2) {
      port = Integer.parseInt(endpointParts[1]);
    } else {
      port = null;
    }

    return of(userInfo, host, port);
  }

  public static S3Authority of(AwsBasicCredentials userInfo, String host, Integer port) {
    return new S3Authority(userInfo, host, port);
  }

  private final AwsBasicCredentials userInfo;
  private final String host;
  private final Integer port;

  public S3Authority(AwsBasicCredentials userInfo, String host, Integer port) {
    if (host == null)
      throw new NullPointerException();
    this.userInfo = userInfo;
    this.host = host;
    this.port = port;
  }

  public AwsBasicCredentials getUserInfo() {
    return userInfo;
  }

  public String getHost() {
    return host;
  }

  public Integer getPort() {
    return port;
  }

  /**
   * @return $HOST[:$PORT]
   */
  public String getEndpoint() {
    String result = getHost();
    if (getPort() != null)
      result = result + ":" + getPort();
    return result;
  }

  @Override
  public int hashCode() {
    return Objects.hash(host, port, userInfo);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    S3Authority other = (S3Authority) obj;
    return Objects.equals(host, other.host) && Objects.equals(port, other.port)
        && Objects.equals(userInfo, other.userInfo);
  }

  @Override
  public String toString() {
    StringBuilder result = new StringBuilder();
    if (getUserInfo() != null) {
      result.append(UrlEncoding.urlencode(getUserInfo().accessKeyId())).append(":")
          .append(UrlEncoding.urlencode(getUserInfo().secretAccessKey())).append("@");
    }
    result.append(getHost());
    if (getPort() != null) {
      result.append(":").append(getPort());
    }
    return result.toString();
  }
}
