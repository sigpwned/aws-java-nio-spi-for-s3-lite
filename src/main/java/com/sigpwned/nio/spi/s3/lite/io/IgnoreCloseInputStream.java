package com.sigpwned.nio.spi.s3.lite.io;

import static java.util.Objects.requireNonNull;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

public class IgnoreCloseInputStream extends FilterInputStream {
  public IgnoreCloseInputStream(InputStream in) {
    super(requireNonNull(in));
  }

  @Override
  public void close() throws IOException {
    // Ignore this call. That's the whole point of this class! :)
  }
}
