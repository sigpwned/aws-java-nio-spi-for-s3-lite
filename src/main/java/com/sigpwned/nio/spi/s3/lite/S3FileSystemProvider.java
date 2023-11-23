package com.sigpwned.nio.spi.s3.lite;

import static java.nio.file.LinkOption.NOFOLLOW_LINKS;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toMap;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FilterInputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.PushbackInputStream;
import java.net.URI;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.AccessMode;
import java.nio.file.CopyOption;
import java.nio.file.DirectoryStream;
import java.nio.file.DirectoryStream.Filter;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributeView;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.FileAttributeView;
import java.nio.file.spi.FileSystemProvider;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import com.sigpwned.aws.sdk.lite.core.credentials.provider.DefaultAwsCredentialsProviderChain;
import com.sigpwned.aws.sdk.lite.core.io.RequestBody;
import com.sigpwned.aws.sdk.lite.core.util.AwsRegions;
import com.sigpwned.aws.sdk.lite.s3.S3Client;
import com.sigpwned.aws.sdk.lite.s3.S3ClientBuilder;
import com.sigpwned.aws.sdk.lite.s3.exception.AccessDeniedException;
import com.sigpwned.aws.sdk.lite.s3.exception.NoSuchBucketException;
import com.sigpwned.aws.sdk.lite.s3.exception.NoSuchKeyException;
import com.sigpwned.aws.sdk.lite.s3.model.DeleteObjectRequest;
import com.sigpwned.aws.sdk.lite.s3.model.GetObjectRequest;
import com.sigpwned.aws.sdk.lite.s3.model.HeadBucketRequest;
import com.sigpwned.aws.sdk.lite.s3.model.HeadObjectRequest;
import com.sigpwned.aws.sdk.lite.s3.model.ListObjectsV2Request;
import com.sigpwned.aws.sdk.lite.s3.model.ListObjectsV2Response;
import com.sigpwned.aws.sdk.lite.s3.model.PutObjectRequest;
import com.sigpwned.aws.sdk.lite.s3.model.PutObjectResponse;
import com.sigpwned.httpmodel.core.util.MoreByteStreams;
import com.sigpwned.nio.spi.s3.lite.io.IgnoreCloseInputStream;
import com.sigpwned.nio.spi.s3.lite.io.LimitedInputStream;
import com.sigpwned.nio.spi.s3.lite.options.ContentTypeOpenOption;
import com.sigpwned.nio.spi.s3.lite.options.FileLengthOpenOption;
import com.sigpwned.nio.spi.s3.lite.util.Buckets;
import com.sigpwned.nio.spi.s3.lite.util.MorePaths;
import com.sigpwned.nio.spi.s3.lite.util.S3FileSystemInfo;

public class S3FileSystemProvider extends FileSystemProvider {
  private static final AtomicReference<Supplier<S3ClientBuilder<?>>> defaultClientBuilderSupplierReference =
      new AtomicReference<>(() -> {
        return S3Client.builder().credentialsProvider(new DefaultAwsCredentialsProviderChain())
            .region(AwsRegions.US_EAST_1);
      });

  private static final AtomicReference<S3Client> defaultClientReference =
      new AtomicReference<>(defaultClientBuilderSupplierReference.get().get().build());

  /**
   * Test hook
   */
  /* default */ static void setDefaultClientBuilderSupplier(
      Supplier<S3ClientBuilder<?>> clientBuilderSupplier) {
    if (clientBuilderSupplier == null)
      throw new NullPointerException();
    S3Client newDefaultClient = clientBuilderSupplier.get().build();
    defaultClientBuilderSupplierReference.set(clientBuilderSupplier);
    defaultClientReference.set(newDefaultClient);
  }

  private static S3ClientBuilder<?> defaultClientBuilder() {
    return defaultClientBuilderSupplierReference.get().get();
  }

  private static S3Client getDefaultClient() {
    return defaultClientReference.get();
  }

  public static final String SEPARATOR = "/";
  public static final String SCHEME = "s3";

  /**
   * Required by SPI
   */
  public S3FileSystemProvider() {}

  @Override
  public void checkAccess(Path path, AccessMode... modes) throws IOException {
    // warn if AccessModes includes WRITE or EXECUTE
    for (AccessMode mode : modes) {
      if (mode == AccessMode.WRITE || mode == AccessMode.EXECUTE) {
        // TODO Log
        // logger.warn("checkAccess: AccessMode '{}' is currently not checked by
        // S3FileSystemProvider",
        // mode);
      }
    }

    final S3Path s3Path = requireNonNull(MorePaths.toS3Path(path.toRealPath(NOFOLLOW_LINKS)));

    Object result;
    if (s3Path.equals(s3Path.getRoot())) {
      try {
        s3Path.getFileSystem().getClient()
            .headBucket(HeadBucketRequest.builder().bucket(s3Path.bucketName()).build());
      } catch (NoSuchBucketException e) {
        throw new NoSuchFileException(s3Path.toString());
      } catch (AccessDeniedException e) {
        throw new java.nio.file.AccessDeniedException(s3Path.toString());
      }
    } else if (s3Path.isDirectory()) {
      ListObjectsV2Response response;
      try {
        response = s3Path.getFileSystem().getClient()
            .listObjectsV2(ListObjectsV2Request.builder().bucket(s3Path.bucketName())
                .prefix(s3Path.getKey()).delimiter(S3FileSystemProvider.SEPARATOR).build());
      } catch (AccessDeniedException e) {
        throw new java.nio.file.AccessDeniedException(s3Path.toString());
      }
      if (response.hasCommonPrefixes() && !response.commonPrefixes().isEmpty()) {
        // This directory exists, since we have common prefixes.
      } else if (response.hasContents() && !response.contents().isEmpty()) {
        // This directory exists, since we have contents.
      } else {
        // This directory does not exist.
        throw new NoSuchFileException(s3Path.toString());
      }
    } else {
      try {
        s3Path.getFileSystem().getClient().headObject(
            HeadObjectRequest.builder().bucket(s3Path.bucketName()).key(s3Path.getKey()).build());
      } catch (AccessDeniedException e) {
        throw new java.nio.file.AccessDeniedException(s3Path.toString());
      } catch (NoSuchKeyException e) {
        // Obviously, this does not exists.
        throw new NoSuchFileException(s3Path.toString());
      }
    }
  }

  @Override
  public void copy(Path source, Path target, CopyOption... options) throws IOException {
    // If both paths point to the same object, this is a NOP
    if (isSameFile(source, target)) {
      return;
    }

    S3Path s3Source = requireNonNull(MorePaths.toS3Path(source));
    S3Path s3Target = requireNonNull(MorePaths.toS3Path(target));

    if (s3Source.isDirectory()) {
      throw new IllegalArgumentException("cannot copy directory");
    }
    if (s3Target.isDirectory()) {
      s3Target = s3Target.resolve(s3Source.getFileName().getName(0));
    }

    boolean targetExists = exists(s3Target);
    if (targetExists && !asList(options).contains(StandardCopyOption.REPLACE_EXISTING)) {
      throw new FileAlreadyExistsException(s3Target.toString());
    }

    // TODO We really should do this with S3 copy operations
    try (InputStream in = newInputStream(s3Source); OutputStream out = newOutputStream(s3Target)) {
      MoreByteStreams.drain(in, out);
    }
  }

  private boolean exists(S3Path path) {
    try {
      path.getFileSystem().getClient().headObject(
          HeadObjectRequest.builder().bucket(path.bucketName()).key(path.getKey()).build());
      return true;
    } catch (NoSuchBucketException e) {
      return false;
    } catch (NoSuchKeyException e) {
      return false;
    }
  }

  @Override
  public void createDirectory(Path path, FileAttribute<?>... attrs) throws IOException {
    S3Path s3Path = requireNonNull(MorePaths.toS3Path(path));
    if (s3Path.toString().equals("/") || s3Path.toString().isEmpty()) {
      throw new FileAlreadyExistsException("Root directory already exists");
    }

    String s3Key = s3Path.toRealPath(NOFOLLOW_LINKS).getKey();
    if (!s3Key.endsWith(S3FileSystemProvider.SEPARATOR) && !s3Key.isEmpty()) {
      s3Key = s3Key + S3FileSystemProvider.SEPARATOR;
    }

    s3Path.getFileSystem().getClient().putObject(
        PutObjectRequest.builder().bucket(s3Path.bucketName()).key(s3Key).build(),
        RequestBody.empty());
  }

  @Override
  public void delete(Path path) throws IOException {
    S3Path s3Path = requireNonNull(MorePaths.toS3Path(path));
    s3Path.getFileSystem().getClient().deleteObject(
        DeleteObjectRequest.builder().bucket(s3Path.bucketName()).key(s3Path.getKey()).build());
  }

  @Override
  public FileStore getFileStore(Path path) throws IOException {
    // Always null.
    return null;
  }

  @Override
  public FileSystem getFileSystem(URI uri) {
    return getFileSystem(uri, false);
  }

  private Map<String, S3FileSystem> FS_CACHE = new HashMap<>();

  /**
   * Similar to getFileSystem(uri), but it allows to create the file system if not yet created.
   *
   * @param uri URI reference
   * @param create if true, the file system is created if not already done
   * @return The file system
   * @throws IllegalArgumentException If the pre-conditions for the {@code uri} parameter aren't met
   * @throws FileSystemNotFoundException If the file system does not exist
   * @throws SecurityException If a security manager is installed, and it denies an unspecified
   *         permission.
   */
  S3FileSystem getFileSystem(URI uri, boolean create) {
    S3FileSystemInfo info = S3FileSystemInfo.fromUri(uri);
    return FS_CACHE.computeIfAbsent(info.key(), (key) -> {
      if (!create) {
        throw new FileSystemNotFoundException(uri.toString());
      }

      String region = Buckets.getBucketRegion(getDefaultClient(), info.bucket());

      S3Client client = defaultClientBuilder().region(region).build();

      return new S3FileSystem(this, client, info.bucket());
    });
  }

  @Override
  public Path getPath(URI uri) {
    if (uri == null)
      throw new NullPointerException();
    return getFileSystem(uri, true).getPath(uri.getScheme() + ":/" + uri.getPath());
  }

  @Override
  public String getScheme() {
    return S3FileSystemProvider.SCHEME;
  }

  @Override
  public boolean isHidden(Path path) throws IOException {
    return false;
  }

  /**
   * Tests if two paths locate the same file. This method works in exactly the manner specified by
   * the {@link Files#isSameFile} method.
   *
   * @param path one path to the file
   * @param path2 the other path
   * @return {@code true} if, and only if, the two paths locate the same file
   * @throws IOException if an I/O error occurs
   * @throws SecurityException In the case of the default provider, and a security manager is
   *         installed, the {@link SecurityManager#checkRead(String) checkRead} method is invoked to
   *         check read access to both files.
   */
  @Override
  public boolean isSameFile(Path path, Path path2) throws IOException {
    return path.toRealPath(NOFOLLOW_LINKS).equals(path2.toRealPath(NOFOLLOW_LINKS));
  }

  /**
   * Move or rename a file to a target file. This method works in exactly the manner specified by
   * the {@link Files#move} method except that both the source and target paths must be associated
   * with this provider.
   *
   * @param source the path to the file to move
   * @param target the path to the target file
   * @param options options specifying how the move should be done
   */
  @Override
  public void move(Path source, Path target, CopyOption... options) throws IOException {
    copy(source, target, options);
    delete(source);
  }

  @Override
  public InputStream newInputStream(Path path, OpenOption... options) throws IOException {
    S3Path s3Path = requireNonNull(MorePaths.toS3Path(path));
    InputStream in = s3Path.getFileSystem().getClient().getObject(
        GetObjectRequest.builder().bucket(s3Path.bucketName()).key(s3Path.getKey()).build());
    InputStream result = new FilterInputStream(in) {
      @Override
      public void close() throws IOException {
        try {
          super.close();
        } finally {
          s3Path.getFileSystem().deregisterOpenCloseable(this);
        }
      }
    };
    s3Path.getFileSystem().registerOpenCloseable(result);
    return result;
  }

  @Override
  public OutputStream newOutputStream(Path path, OpenOption... options) throws IOException {
    S3Path s3Path = requireNonNull(MorePaths.toS3Path(path));

    final OptionalLong maybeContentLength;
    if (options != null) {
      maybeContentLength = Arrays.stream(options).filter(o -> o instanceof FileLengthOpenOption)
          .map(o -> (FileLengthOpenOption) o).mapToLong(FileLengthOpenOption::getLength)
          .findFirst();
    } else {
      maybeContentLength = OptionalLong.empty();
    }

    final Optional<String> maybeContentType;
    if (options != null) {
      maybeContentType = Arrays.stream(options).filter(o -> o instanceof ContentTypeOpenOption)
          .map(o -> (ContentTypeOpenOption) o).map(ContentTypeOpenOption::getContentType)
          .findFirst();
    } else {
      maybeContentType = Optional.empty();
    }

    PipedInputStream in = new PipedInputStream();
    PipedOutputStream out = new PipedOutputStream();
    in.connect(out);

    Runnable worker;
    if (maybeContentLength.isPresent()) {
      worker = newUnbufferedOutputStream(in, s3Path, maybeContentType.orElse(null),
          maybeContentLength.getAsLong());

    } else {
      worker = newBufferedOutputStream(in, s3Path, maybeContentType.orElse(null));
    }

    new Thread(worker).start();

    OutputStream result = new FilterOutputStream(out) {
      @Override
      public void close() throws IOException {
        try {
          super.close();
        } finally {
          s3Path.getFileSystem().deregisterOpenCloseable(this);
        }
      }
    };

    s3Path.getFileSystem().registerOpenCloseable(result);

    return result;
  }

  private Runnable newUnbufferedOutputStream(InputStream in, S3Path target, String contentType,
      long contentLength) {
    return () -> {
      try {
        int b0 = in.read();
        try (PushbackInputStream pin = new PushbackInputStream(in, 1)) {
          if (b0 != -1)
            pin.unread(b0);
          @SuppressWarnings("unused")
          PutObjectResponse response = target.getFileSystem().getClient().putObject(
              PutObjectRequest.builder().bucket(target.bucketName()).key(target.getKey()).build(),
              new RequestBody(contentLength, contentType, () -> {
                return in;
              }));
          // TODO Should we check anything on PutObjectResponse?
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    };
  }

  private static final int MAX_MEM_BUFFER_SIZE = 5 * 1024 * 1024;

  private Runnable newBufferedOutputStream(InputStream in, S3Path target, String contentType) {
    return () -> {
      try {
        // TODO Let's avoid having up to double mem buffer size due to copy
        ByteArrayOutputStream membuf = new ByteArrayOutputStream();
        try (InputStream xin =
            new IgnoreCloseInputStream(new LimitedInputStream(in, MAX_MEM_BUFFER_SIZE))) {
          MoreByteStreams.drain(xin, membuf);
        }

        // TODO Should we check anything in the PutObject response?
        @SuppressWarnings("unused")
        PutObjectResponse response;
        try {
          RequestBody body;
          if (membuf.size() < MAX_MEM_BUFFER_SIZE) {
            final long contentLength = membuf.size();
            body = new RequestBody(contentLength, contentType,
                () -> new ByteArrayInputStream(membuf.toByteArray()));
          } else {
            // Welp, that's a file buffer.
            File tmp = File.createTempFile("s3.", ".buf");
            try (OutputStream xout = new FileOutputStream(tmp)) {
              xout.write(membuf.toByteArray());
              MoreByteStreams.drain(in, xout);
            }

            // TODO When do we clean up tmp?
            final long contentLength = tmp.length();
            body = new RequestBody(contentLength, contentType, () -> new FileInputStream(tmp));
          }
          response = target.getFileSystem().getClient().putObject(
              PutObjectRequest.builder().bucket(target.bucketName()).key(target.getKey()).build(),
              body);
        } finally {
          in.close();
        }

        // TODO Anything to do with response?
      } catch (Exception e) {
        e.printStackTrace();
      }
    };
  }

  @Override
  public DirectoryStream<Path> newDirectoryStream(Path path, Filter<? super Path> filter)
      throws IOException {
    S3Path s3Path = requireNonNull(MorePaths.toS3Path(path));
    return new S3DirectoryStream(s3Path, filter);
  }

  /**
   * Reads a set of file attributes as a bulk operation. Largely equivalent to
   * {@code readAttributes(Path path, Class<A> type, LinkOption... options)} where the returned
   * object is a map of method names (attributes) to values, filtered on the comma separated
   * {@code attributes}.
   *
   * @param path the path to the file
   * @param attributes the comma separated attributes to read. May be prefixed with "s3:"
   * @param options ignored, S3 has no links
   * @return a map of the attributes returned; may be empty. The map's keys are the attribute names,
   *         its values are the attribute values. Returns an empty map if {@code attributes} is
   *         empty, or if {@code path} is inferred to be a directory.
   * @throws UnsupportedOperationException if the attribute view is not available
   * @throws IllegalArgumentException if no attributes are specified or an unrecognized attributes
   *         is specified
   * @throws SecurityException In the case of the default provider, and a security manager is
   *         installed, its {@link SecurityManager#checkRead(String) checkRead} method denies read
   *         access to the file. If this method is invoked to read security sensitive attributes
   *         then the security manager may be invoked to check for additional permissions.
   */
  @Override
  public Map<String, Object> readAttributes(Path path, String attributes, LinkOption... options) {
    attributes = requireNonNull(attributes);

    if (attributes.trim().isEmpty()) {
      return emptyMap();
    }

    S3BasicFileAttributes attrs = readAttributes(path, S3BasicFileAttributes.class, options);

    if (attrs.isDirectory()) {
      return Collections.emptyMap();
    }

    Map<String, Object> result = new HashMap<>();
    result.put("creationTime", attrs.creationTime());
    result.put("fileKey", attrs.fileKey());
    result.put("isDirectory", attrs.isDirectory());
    result.put("isOther", attrs.isOther());
    result.put("isRegularFile", attrs.isRegularFile());
    result.put("isSymbolicLink", attrs.isSymbolicLink());
    result.put("lastAccessTime", attrs.lastAccessTime());
    result.put("lastModifiedTime", attrs.lastModifiedTime());
    result.put("size", attrs.size());

    Predicate<String> attributesFilter = attributesFilterFor(attributes);

    return unmodifiableMap(result.entrySet().stream().filter(e -> attributesFilter.test(e.getKey()))
        .collect(toMap(Map.Entry::getKey, Map.Entry::getValue)));
  }

  @Override
  @SuppressWarnings("unchecked")
  public <V extends FileAttributeView> V getFileAttributeView(Path path, Class<V> type,
      LinkOption... options) {
    if (type == null)
      throw new NullPointerException();

    S3Path s3Path = requireNonNull(MorePaths.toS3Path(path));

    if (!type.equals(BasicFileAttributeView.class)
        && !type.equals(S3BasicFileAttributeView.class)) {
      throw new IllegalArgumentException("type must be BasicFileAttributeView.class");
    }

    return (V) new S3BasicFileAttributeView(s3Path);
  }

  /**
   * Reads a file's attributes as a bulk operation. This method works in exactly the manner
   * specified by the {@link Files#readAttributes(Path, Class, LinkOption[])} method.
   *
   * @param path the path to the file
   * @param type the {@code Class} of the file attributes required to read. Supported types are
   *        {@code BasicFileAttributes}
   * @param options options indicating how symbolic links are handled
   * @return the file attributes or {@code null} if {@code path} is inferred to be a directory.
   */
  @Override
  @SuppressWarnings("unchecked")
  public <A extends BasicFileAttributes> A readAttributes(Path path, Class<A> type,
      LinkOption... options) {
    if (type == null)
      throw new NullPointerException();

    if (!type.equals(BasicFileAttributes.class) && !type.equals(S3BasicFileAttributes.class)) {
      throw new UnsupportedOperationException("cannot read attributes of type " + type);
    }

    return (A) getFileAttributeView(path, S3BasicFileAttributeView.class, options).readAttributes();
  }

  /**
   * File attributes of S3 objects cannot be set other than by creating a new object
   *
   * @throws UnsupportedOperationException always
   */
  @Override
  public void setAttribute(Path path, String attribute, Object value, LinkOption... options)
      throws UnsupportedOperationException {
    throw new UnsupportedOperationException("s3 file attributes cannot be modified by this class");
  }

  @Override
  public SeekableByteChannel newByteChannel(Path arg0, Set<? extends OpenOption> arg1,
      FileAttribute<?>... arg2) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public FileSystem newFileSystem(URI uri, Map<String, ?> args) throws IOException {
    // TODO Support new bucket creation?
    throw new UnsupportedOperationException();
  }

  private static Predicate<String> attributesFilterFor(String attributes) {
    if (attributes.equals("*") || attributes.equals("basic")) {
      return x -> true;
    }
    final Set<String> attrSet = Arrays.stream(attributes.split(","))
        .map(attr -> attr.replaceAll("^basic:", "")).collect(Collectors.toSet());
    return attrSet::contains;
  }
}
