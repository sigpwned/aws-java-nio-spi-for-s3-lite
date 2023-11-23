/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved. SPDX-License-Identifier:
 * Apache-2.0
 */

package com.sigpwned.nio.spi.s3.lite;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.sigpwned.aws.sdk.lite.s3.model.CommonPrefix;
import com.sigpwned.aws.sdk.lite.s3.model.ListObjectsV2Request;
import com.sigpwned.aws.sdk.lite.s3.model.S3Object;

class S3DirectoryStream implements DirectoryStream<Path> {
  private static final String PATH_SEPARATOR = S3FileSystemProvider.SEPARATOR;

  private final Logger logger = LoggerFactory.getLogger(this.getClass().getName());

  private Iterator<Path> iterator;

  S3DirectoryStream(S3Path path, Filter<? super Path> filter) {
    final S3FileSystem fileSystem = path.getFileSystem();
    final String bucketName = path.bucketName();
    final String key = path.getKey();

    iterator =
        fileSystem.getClient()
            .listObjectsV2Paginator(ListObjectsV2Request
                .builder().bucket(bucketName).prefix(key).delimiter(PATH_SEPARATOR).build())
            .stream()
            .flatMap(r -> Stream.concat(r.commonPrefixes().stream().map(CommonPrefix::prefix),
                r.contents().stream().map(S3Object::key)))
            .map(fileSystem::getPath)
            // Including the parent would cause cycles
            .filter(s3pi -> !isEqualToParent(key, s3pi)).filter(s3pi -> tryAccept(filter, s3pi))
            .map(s3pi -> (Path) s3pi).iterator();

    // noinspection ResultOfMethodCallIgnored
    iterator.hasNext();
  }

  @Override
  public Iterator<Path> iterator() {
    // Only allow one call to iterator, per the docs
    if (iterator == null)
      throw new IllegalStateException();
    Iterator<Path> result = iterator;
    iterator = null;
    return result;
  }

  @Override
  public void close() {}

  private static boolean isEqualToParent(String finalDirName, S3Path p) {
    return p.getKey().equals(finalDirName);
  }

  private boolean tryAccept(DirectoryStream.Filter<? super Path> filter, S3Path path) {
    try {
      return filter.accept(path);
    } catch (IOException e) {
      logger.warn("An IOException was thrown while filtering the path: {}."
          + " Set log level to debug to show stack trace", path);
      logger.debug(e.getMessage(), e);
      return false;
    }
  }
}
