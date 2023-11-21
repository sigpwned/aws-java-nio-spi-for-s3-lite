package com.sigpwned.nio.spi.s3.lite;

import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;
import java.io.Closeable;
import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.WatchService;
import java.nio.file.attribute.UserPrincipalLookupService;
import java.nio.file.spi.FileSystemProvider;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;
import com.sigpwned.aws.sdk.lite.s3.S3Client;

public class S3FileSystem extends FileSystem {
  public static final String SCHEME = "s3";

  public static final String SEPARATOR = "/";

  private final S3FileSystemProvider provider;
  private final S3NioSpiConfiguration configuration;
  private final Set<Closeable> openCloseables;
  private boolean open;

  public S3FileSystem(S3FileSystemProvider provider, S3NioSpiConfiguration configuration) {
    this.provider = requireNonNull(provider);
    this.configuration = requireNonNull(configuration);
    this.openCloseables = Collections.newSetFromMap(new IdentityHashMap<Closeable, Boolean>());
    this.open = true;
  }

  @Override
  public void close() throws IOException {
    // TODO Auto-generated method stub

  }

  @Override
  public Iterable<FileStore> getFileStores() {
    return emptyList();
  }

  @Override
  public S3Path getPath(String first, String... more) {
    return S3Path.getPath(this, first, more);
  }

  @Override
  public PathMatcher getPathMatcher(String syntaxAndPattern) {
    // TODO this assumes the JDK will be on a system where path matching of the default filesystem
    // is Posix like.
    return FileSystems.getDefault().getPathMatcher(syntaxAndPattern);
  }

  @Override
  public Iterable<Path> getRootDirectories() {
    return Collections.singleton(S3Path.getPath(this, "/"));
  }

  @Override
  public String getSeparator() {
    return SEPARATOR;
  }

  @Override
  public boolean isOpen() {
    return open;
  }

  @Override
  public boolean isReadOnly() {
    return false;
  }

  @Override
  public FileSystemProvider provider() {
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
    return SUPPORTED_FILE_ATTRIBUTE_VIEWS;
  }

  @Override
  public UserPrincipalLookupService getUserPrincipalLookupService() {
    throw new UnsupportedOperationException();
  }

  @Override
  public WatchService newWatchService() throws IOException {
    throw new UnsupportedOperationException();
  }

  /* default */ void registerOpenCloseable(Closeable openCloseable) {
    openCloseables.add(openCloseable);
  }

  /* default */ void deregisterOpenCloseable(Closeable openCloseable) {
    openCloseables.remove(openCloseable);
  }

  public S3NioSpiConfiguration configuration() {
    return configuration;
  }

  public String bucketName() {
    return configuration().getBucketName();
  }

  /* default */ S3Client getClient() {
    // TODO I'm pretty sure we'll need to at least detect regions here
    return S3Client.builder().build();
  }
}
