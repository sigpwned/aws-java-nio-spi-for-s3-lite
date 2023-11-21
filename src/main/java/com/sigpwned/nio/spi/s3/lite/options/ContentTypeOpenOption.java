package com.sigpwned.nio.spi.s3.lite.options;

import static java.util.Objects.requireNonNull;
import java.nio.file.OpenOption;
import java.util.Objects;

public class ContentTypeOpenOption implements OpenOption {
  public static ContentTypeOpenOption of(String contentType) {
    return new ContentTypeOpenOption(contentType);
  }

  private final String contentType;

  public ContentTypeOpenOption(String contentType) {
    this.contentType = requireNonNull(contentType);
  }

  public String getContentType() {
    return contentType;
  }

  @Override
  public int hashCode() {
    return Objects.hash(contentType);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    ContentTypeOpenOption other = (ContentTypeOpenOption) obj;
    return Objects.equals(contentType, other.contentType);
  }

  @Override
  public String toString() {
    return "ContentTypeOpenOption [contentType=" + contentType + "]";
  }
}
