# aws-java-nio-spi-for-s3-lite

A Lightweight Java NIO.2 `FileSystemProvider` implementation for S3 and Java 8+.

## What is a `FileSystemProvider`?

In Java, `FileSystemProvider` is an [SPI](https://en.wikipedia.org/wiki/Service_provider_interface) users can implement to integrate [file system](https://en.wikipedia.org/wiki/File_system)-like services, like S3, into the Java NIO framework. This implementations allows S3 objects to be treated like local files using the [java.nio.file.Path](https://docs.oracle.com/javase/8/docs/api/java/nio/file/Path.html) class.

## How do I use a `FileSystemProvider`?

As a rule, you don't use it directly. Instead, you simply use the `Path` object to manipulate files, and the underlying framework loads the required `FileSystemProvider` automatically with the [`ServiceLoader`](https://docs.oracle.com/javase/8/docs/api/java/util/ServiceLoader.html).

For example, this code would print the contents of a text file stored in S3 to `System.out`:

    try (BufferedReader r = new BufferedReader(
        new InputStreamReader(
          Files.newInputStream(Paths.get(URI.create("s3://example-bucket-name/path/to/object.txt"))),
          StandardCharsets.UTF_8))) {
      r.lines().forEach(System.out::println);
    }

Assuming this library is added to a project properly, this code will automatically load the `S3FileSystemProvider`, then use AWS credentials embedded in the environment (e.g., environment variables, system properties, etc.) to communicate with the S3 API and retrieve the named object's content as an `InputStream`.

## Why would I use this library?

Note that the only reference to S3 in the above code is in a URI scheme. In particular, this code makes no direct references to any classes in this library, the AWS Java SDK, etc. This transparency is enormously helpful in building implementations that can read data from disk at test time and S3 at production time, and so on. The library allows you to decouple fetching data from processing data and focus on building business logic without worrying about whether the data is coming from local files or S3.

## Why not use `awslabs/aws-java-nio-spi-for-s3`?

The [awslabs/aws-java-nio-spi-for-s3](https://github.com/awslabs/aws-java-nio-spi-for-s3) library is outstanding, and the basis and inspiration for this library. If you're happily using this library, then you should keep using it!

However, that library requires a surprising amount of space in an über JAR. A simple cross-platform application that depends on `software.amazon.nio.s3:aws-java-nio-spi-for-s3:1.2.4` and is packaged with the Maven Shade Plugin with `<minimizeJar>true</minimizeJar>` results in a 20MB compressed / 53MB uncompressed über JAR. Specializing the application to one platform (e.g., macos-aarch64) can get the size down to 10MB compressed / 27MB uncompressed. For containerized applications, this may not be a big deal, but for other deployments like Lambda functions with hard size limits, that's a lot of space to sacrifice just to make S3 access transparent!

This implementation uses several techniques to minimize dependencies and size, including using [a custom, lightweight AWS Java Client](https://github.com/sigpwned/aws-java-sdk-lite), to minimize JAR size. The same cross-platform application compiled with `com.sigpwned:aws-java-nio-spi-for-s3-lite:0.0.0-b0` is 407K compressed / 945K uncompressed.

If executable size matters to your application, then this implementation offers important advantages over `awslabs/aws-java-nio-spi-for-s3`.

## Caveats

This implementation only supports synchronous I/O. All asynchronous I/O operations throw `UnsupportedOperationException`.

This implementation supports all core read and write operations, including move, copy, and delete. However, at the time of this writing, these implementations are not always atomic or as efficient as they can be. Please open issues as needed for improvements, and of course pull requests are always welcome!

## Acknowledgements

This implementation is based off of and designed to be a lightweight version of [awslabs/aws-java-nio-spi-for-s3](https://github.com/awslabs/aws-java-nio-spi-for-s3). Much credit for this project goes to [markjschreiber](https://github.com/markjschreiber), who did all the heavy lifting for showing how an integration like this works!

Otherwise, many thanks to AWS for the outstanding product that is S3!
