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

import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;
import java.io.Closeable;
import java.io.IOException;
import java.nio.file.ClosedFileSystemException;
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.WatchService;
import java.nio.file.attribute.UserPrincipalLookupService;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;
import com.sigpwned.aws.sdk.lite.s3.S3Client;

public class S3FileSystem extends FileSystem {
  private final S3FileSystemProvider provider;
  private final S3Client client;
  private final String bucketName;
  private final Set<Closeable> closeables;
  private boolean open;

  public S3FileSystem(S3FileSystemProvider provider, S3Client client, String bucketName) {
    this.provider = requireNonNull(provider);
    this.client = requireNonNull(client);
    this.bucketName = requireNonNull(bucketName);
    this.closeables = Collections.newSetFromMap(new IdentityHashMap<Closeable, Boolean>());
    this.open = true;
  }

  @Override
  public void close() throws IOException {
    open = false;

    Collection<Closeable> cs = new ArrayList<>();
    synchronized (closeables) {
      cs.addAll(closeables);
      closeables.clear();
    }

    Exception problem = null;
    for (Closeable closeable : cs) {
      try {
        closeable.close();
      } catch (Exception e) {
        problem = e;
      }
    }
    if (problem != null) {
      if (problem instanceof IOException) {
        throw (IOException) problem;
      } else if (problem instanceof RuntimeException) {
        throw (RuntimeException) problem;
      } else {
        throw new IOException("Failed to close stream", problem);
      }
    }
  }

  @Override
  public Iterable<FileStore> getFileStores() {
    if (!isOpen())
      throw new ClosedFileSystemException();
    return emptyList();
  }

  @Override
  public S3Path getPath(String first, String... more) {
    if (!isOpen())
      throw new ClosedFileSystemException();
    return S3Path.getPath(this, first, more);
  }

  @Override
  public PathMatcher getPathMatcher(String syntaxAndPattern) {
    if (!isOpen())
      throw new ClosedFileSystemException();
    // TODO this assumes the underlying platform's filesystem is POSIX-like
    return FileSystems.getDefault().getPathMatcher(syntaxAndPattern);
  }

  @Override
  public Iterable<Path> getRootDirectories() {
    if (!isOpen())
      throw new ClosedFileSystemException();
    return Collections.singleton(S3Path.getPath(this, "/"));
  }

  @Override
  public String getSeparator() {
    if (!isOpen())
      throw new ClosedFileSystemException();
    return S3FileSystemProvider.SEPARATOR;
  }

  @Override
  public boolean isOpen() {
    return open;
  }

  @Override
  public boolean isReadOnly() {
    if (!isOpen())
      throw new ClosedFileSystemException();
    return false;
  }

  @Override
  public S3FileSystemProvider provider() {
    if (!isOpen())
      throw new ClosedFileSystemException();
    return provider;
  }

  /**
   * View required by Java NIO
   */
  static final String BASIC_FILE_ATTRIBUTE_VIEW = "basic";

  private static final Set<String> SUPPORTED_FILE_ATTRIBUTE_VIEWS =
      Collections.singleton(BASIC_FILE_ATTRIBUTE_VIEW);

  @Override
  public Set<String> supportedFileAttributeViews() {
    if (!isOpen())
      throw new ClosedFileSystemException();
    return SUPPORTED_FILE_ATTRIBUTE_VIEWS;
  }

  @Override
  public UserPrincipalLookupService getUserPrincipalLookupService() {
    if (!isOpen())
      throw new ClosedFileSystemException();
    throw new UnsupportedOperationException();
  }

  @Override
  public WatchService newWatchService() throws IOException {
    if (!isOpen())
      throw new ClosedFileSystemException();
    throw new UnsupportedOperationException();
  }

  /* default */ void registerCloseable(Closeable closeable) {
    synchronized (closeables) {
      closeables.add(closeable);
    }
  }

  /* default */ void deregisterCloseable(Closeable closeable) {
    synchronized (closeables) {
      closeables.remove(closeable);
    }
  }

  public String getBucketName() {
    return bucketName;
  }

  /* default */ S3Client getClient() {
    return client;
  }
}
