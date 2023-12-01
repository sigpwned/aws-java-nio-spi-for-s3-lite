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
package com.sigpwned.nio.spi.s3.lite.options;

import java.nio.file.OpenOption;
import java.util.Objects;

public class FileLengthOpenOption implements OpenOption {
  public static FileLengthOpenOption of(long length) {
    return new FileLengthOpenOption(length);
  }

  private final long length;

  public FileLengthOpenOption(long length) {
    if (length < 0L)
      throw new IllegalArgumentException("file length must not be negative");
    this.length = length;
  }

  public long getLength() {
    return length;
  }

  @Override
  public int hashCode() {
    return Objects.hash(length);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    FileLengthOpenOption other = (FileLengthOpenOption) obj;
    return length == other.length;
  }

  @Override
  public String toString() {
    return "FileLengthOpenOption [length=" + length + "]";
  }
}
