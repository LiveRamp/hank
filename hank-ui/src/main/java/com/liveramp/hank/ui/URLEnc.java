package com.liveramp.hank.ui;

import java.io.UnsupportedEncodingException;

public final class URLEnc {
  private URLEnc() {
  }

  public static String encode(String s) {
    try {
      return java.net.URLEncoder.encode(s, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }

  public static String decode(String s) {
    try {
      return java.net.URLDecoder.decode(s, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }
}
