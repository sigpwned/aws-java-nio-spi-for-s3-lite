/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved. SPDX-License-Identifier:
 * Apache-2.0
 */

package com.sigpwned.nio.spi.s3.lite;

import java.nio.file.attribute.BasicFileAttributeView;
import java.nio.file.attribute.FileTime;
import com.sigpwned.aws.sdk.lite.s3.model.HeadObjectRequest;
import com.sigpwned.aws.sdk.lite.s3.model.HeadObjectResponse;

class S3BasicFileAttributeView implements BasicFileAttributeView {
  private final S3Path path;

  S3BasicFileAttributeView(S3Path path) {
    this.path = path;
  }

  /**
   * Returns the name of the attribute view. Attribute views of this type have the name
   * {@code "basic"}.
   */
  @Override
  public String name() {
    return "basic";
  }

  /**
   * Reads the basic file attributes as a bulk operation.
   *
   * <p>
   * It is implementation specific if all file attributes are read as an atomic operation with
   * respect to other file system operations.
   *
   * @return the file attributes
   */
  @Override
  public S3BasicFileAttributes readAttributes() {
    HeadObjectResponse response = getPath().getFileSystem().getClient().headObject(
        HeadObjectRequest.builder().bucket(getPath().bucketName()).key(getPath().getKey()).build());
    return S3BasicFileAttributes.fromHeadObjectResponse(getPath(), response);
  }

  /**
   * Unsupported operation, write operations are not supported.
   */
  @Override
  public void setTimes(FileTime lastModifiedTime, FileTime lastAccessTime, FileTime createTime) {
    throw new UnsupportedOperationException(
        "write operations are not supported, please submitted a feature request explaining your use case");
  }

  private S3Path getPath() {
    return path;
  }
}
