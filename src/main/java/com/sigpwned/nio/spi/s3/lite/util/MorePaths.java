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
package com.sigpwned.nio.spi.s3.lite.util;

import java.nio.file.Path;
import java.nio.file.ProviderMismatchException;
import com.sigpwned.nio.spi.s3.lite.S3Path;

public final class MorePaths {
  private MorePaths() {}

  public static S3Path toS3Path(Path p) {
    if (p == null)
      return null;
    if (p instanceof S3Path)
      return (S3Path) p;
    throw new ProviderMismatchException();
  }
}
