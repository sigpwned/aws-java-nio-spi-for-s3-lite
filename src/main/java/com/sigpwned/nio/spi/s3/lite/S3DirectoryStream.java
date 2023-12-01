/*-
 * =================================LICENSE_START==================================
 * AWS Java NIO SPI for S3 Lite
 * ====================================SECTION=====================================
 * Copyright (C) 2023 Andy Boothe
 * ====================================SECTION=====================================
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * ==================================LICENSE_END===================================
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

    open = true;

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
