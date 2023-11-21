package com.sigpwned.nio.spi.s3.lite.options;

import java.nio.file.OpenOption;
import java.util.Objects;

public class FileLengthOpenOption implements OpenOption {
  public static FileLengthOpenOption of(long length) {
    return new FileLengthOpenOption(length);
  }

  private final long length;

  public FileLengthOpenOption(long length) {
    if (length < 0L)
      throw new IllegalArgumentException("file length must not be negative");
    this.length = length;
  }

  public long getLength() {
    return length;
  }

  @Override
  public int hashCode() {
    return Objects.hash(length);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    FileLengthOpenOption other = (FileLengthOpenOption) obj;
    return length == other.length;
  }

  @Override
  public String toString() {
    return "FileLengthOpenOption [length=" + length + "]";
  }
}
