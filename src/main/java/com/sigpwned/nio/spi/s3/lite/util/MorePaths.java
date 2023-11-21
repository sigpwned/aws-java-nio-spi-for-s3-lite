package com.sigpwned.nio.spi.s3.lite.util;

import java.nio.file.Path;
import java.nio.file.ProviderMismatchException;
import com.sigpwned.nio.spi.s3.lite.S3Path;

public final class MorePaths {
  private MorePaths() {}

  public static S3Path toS3Path(Path p) {
    if (p == null)
      return null;
    if (p instanceof S3Path)
      return (S3Path) p;
    throw new ProviderMismatchException();
  }
}
