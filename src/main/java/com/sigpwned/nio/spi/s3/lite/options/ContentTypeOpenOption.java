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

import static java.util.Objects.requireNonNull;
import java.nio.file.OpenOption;
import java.util.Objects;

public class ContentTypeOpenOption implements OpenOption {
  public static ContentTypeOpenOption of(String contentType) {
    return new ContentTypeOpenOption(contentType);
  }

  private final String contentType;

  public ContentTypeOpenOption(String contentType) {
    this.contentType = requireNonNull(contentType);
  }

  public String getContentType() {
    return contentType;
  }

  @Override
  public int hashCode() {
    return Objects.hash(contentType);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    ContentTypeOpenOption other = (ContentTypeOpenOption) obj;
    return Objects.equals(contentType, other.contentType);
  }

  @Override
  public String toString() {
    return "ContentTypeOpenOption [contentType=" + contentType + "]";
  }
}
