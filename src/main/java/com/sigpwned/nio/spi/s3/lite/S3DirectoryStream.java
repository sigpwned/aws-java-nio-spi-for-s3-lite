/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved. SPDX-License-Identifier:
 * Apache-2.0
 */

package com.sigpwned.nio.spi.s3.lite;

import java.io.IOException;
import java.nio.file.ClosedDirectoryStreamException;
import java.nio.file.DirectoryIteratorException;
import java.nio.file.DirectoryStream;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.Stream;
import com.sigpwned.aws.sdk.lite.s3.model.CommonPrefix;
import com.sigpwned.aws.sdk.lite.s3.model.ListObjectsV2Request;
import com.sigpwned.aws.sdk.lite.s3.model.S3Object;

class S3DirectoryStream implements DirectoryStream<Path> {
  private static final String PATH_SEPARATOR = S3FileSystemProvider.SEPARATOR;

  private Iterator<Path> iterator;
  private boolean open;

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
  public void forEach(Consumer<? super Path> action) {
    if (open == false)
      throw new ClosedDirectoryStreamException();
    DirectoryStream.super.forEach(action);
  }

  @Override
  public Spliterator<Path> spliterator() {
    if (open == false)
      throw new ClosedDirectoryStreamException();
    return DirectoryStream.super.spliterator();
  }

  @Override
  public Iterator<Path> iterator() {
    // Only allow one call to iterator, per the docs
    if (iterator == null)
      throw new IllegalStateException();
    if (open == false)
      throw new ClosedDirectoryStreamException();
    Iterator<Path> result = iterator;
    iterator = null;
    return result;
  }

  @Override
  public void close() {
    iterator = null;
    open = false;
  }

  private static boolean isEqualToParent(String finalDirName, S3Path p) {
    return p.getKey().equals(finalDirName);
  }

  private boolean tryAccept(DirectoryStream.Filter<? super Path> filter, S3Path path) {
    try {
      return filter.accept(path);
    } catch (IOException e) {
      throw new DirectoryIteratorException(e);
    }
  }
}
