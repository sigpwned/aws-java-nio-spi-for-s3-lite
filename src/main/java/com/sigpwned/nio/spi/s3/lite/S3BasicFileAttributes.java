/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved. SPDX-License-Identifier:
 * Apache-2.0
 */

package com.sigpwned.nio.spi.s3.lite;

import static java.util.Objects.requireNonNull;
import java.nio.file.Files;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.util.Optional;
import com.sigpwned.aws.sdk.lite.s3.model.HeadObjectResponse;

/**
 * Representation of {@link BasicFileAttributes} for an S3 object
 */
class S3BasicFileAttributes implements BasicFileAttributes {
  private static final FileTime EPOCH = FileTime.from(Instant.EPOCH);

  /* default */ static S3BasicFileAttributes fromHeadObjectResponse(S3Path path,
      HeadObjectResponse r) {
    boolean directory = path.isDirectory();
    if (directory) {
      return new S3BasicFileAttributes(EPOCH, EPOCH, EPOCH, false, true, 0L, null);
    } else {
      FileTime lastModifiedTime =
          Optional.ofNullable(r.lastModified()).map(FileTime::from).orElse(null);
      FileTime lastAccessTime = lastModifiedTime;
      FileTime creationTime = lastModifiedTime;
      Long size = r.contentLength();
      Object fileKey = r.eTag();
      return new S3BasicFileAttributes(lastModifiedTime, lastAccessTime, creationTime, true, false,
          size, fileKey);
    }
  }

  private final FileTime lastModifiedTime;
  private final FileTime lastAccessTime;
  private final FileTime creationTime;
  private final boolean regularFile;
  private final boolean directory;
  private final long size;
  private final Object fileKey;

  public S3BasicFileAttributes(FileTime lastModifiedTime, FileTime lastAccessTime,
      FileTime creationTime, boolean regularFile, boolean directory, long size, Object fileKey) {
    if (size < 0L)
      throw new IllegalArgumentException("size must not be negative");
    this.lastModifiedTime = requireNonNull(lastModifiedTime);
    this.lastAccessTime = requireNonNull(lastAccessTime);
    this.creationTime = requireNonNull(creationTime);
    this.regularFile = regularFile;
    this.directory = directory;
    this.size = size;
    this.fileKey = requireNonNull(fileKey);
  }

  /**
   * Returns the time of last modification.
   *
   * <p>
   * S3 "directories" do not support a time stamp to indicate the time of last modification
   * therefore this method returns a default value representing the epoch (1970-01-01T00:00:00Z) as
   * a proxy
   *
   * @return a {@code FileTime} representing the time the file was last modified.
   * @throws RuntimeException if the S3Clients {@code RetryConditions} configuration was not able to
   *         handle the exception.
   */
  @Override
  public FileTime lastModifiedTime() {
    return lastModifiedTime;
  }

  /**
   * Returns the time of last access.
   * <p>
   * Without enabling S3 server access logging, CloudTrail or similar it is not possible to obtain
   * the access time of an object, therefore the current implementation will return the @{code
   * lastModifiedTime}
   * </p>
   *
   * @return a {@code FileTime} representing the time of last access
   */
  @Override
  public FileTime lastAccessTime() {
    return lastAccessTime;
  }

  /**
   * Returns the creation time. The creation time is the time that the file was created.
   *
   * <p>
   * Any modification of an S3 object results in a new Object so this time will be the same as
   * {@code lastModifiedTime}. A future implementation could consider times for versioned objects.
   *
   * @return a {@code FileTime} representing the time the file was created
   */
  @Override
  public FileTime creationTime() {
    return creationTime;
  }

  /**
   * Tells whether the file is a regular file with opaque content.
   *
   * @return {@code true} if the file is a regular file with opaque content
   */
  @Override
  public boolean isRegularFile() {
    return regularFile;
  }

  /**
   * Tells whether the file is a directory.
   *
   * @return {@code true} if the file is a directory
   */
  @Override
  public boolean isDirectory() {
    return directory;
  }

  /**
   * Tells whether the file is a symbolic link.
   *
   * @return {@code false} always as S3 has no links
   */
  @Override
  public boolean isSymbolicLink() {
    return false;
  }

  /**
   * Tells whether the file is something other than a regular file, directory, or symbolic link.
   * There are only objects in S3 and inferred directories
   *
   * @return {@code false} always
   */
  @Override
  public boolean isOther() {
    return false;
  }

  /**
   * Returns the size of the file (in bytes). The size may differ from the actual size on the file
   * system due to compression, support for sparse files, or other reasons. The size of files that
   * are not {@link #isRegularFile regular} files is implementation specific and therefore
   * unspecified.
   *
   * @return the file size, in bytes
   * @throws RuntimeException if the S3Clients {@code RetryConditions} configuration was not able to
   *         handle the exception.
   */
  @Override
  public long size() {
    return size;
  }

  /**
   * Returns the S3 etag for the object
   *
   * @return the etag for an object, or {@code null} for a "directory"
   * @throws RuntimeException if the S3Clients {@code RetryConditions} configuration was not able to
   *         handle the exception.
   * @see Files#walkFileTree
   */
  @Override
  public Object fileKey() {
    return fileKey;
  }
}
