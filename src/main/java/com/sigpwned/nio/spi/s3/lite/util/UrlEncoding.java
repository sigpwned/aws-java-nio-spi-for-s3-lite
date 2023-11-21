package com.sigpwned.nio.spi.s3.lite.util;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;

public final class UrlEncoding {
  public static String urlencode(String s) {
    try {
      return URLEncoder.encode(s, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new AssertionError("JDK does not support UTF-8", e);
    }
  }

  public static String urldecode(String s) {
    try {
      return URLDecoder.decode(s, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new AssertionError("JDK does not support UTF-8", e);
    }
  }
}
