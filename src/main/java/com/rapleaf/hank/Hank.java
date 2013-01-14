package com.rapleaf.hank;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.util.jar.Attributes;
import java.util.jar.Manifest;

public final class Hank {
  private static final Logger LOG = Logger.getLogger(Hank.class);

  private Hank() {
  }

  private final static String GIT_COMMIT;

  static {

    InputStream manifestStream = Hank.class.getResourceAsStream("META-INF/MANIFEST.MF");
    try {
      Manifest manifest = new Manifest(manifestStream);
      Attributes attributes = manifest.getMainAttributes();
      String temp = attributes.getValue("Implementation-Build");
      if (temp != null) {
        GIT_COMMIT = temp;
      } else {
        GIT_COMMIT = "Unknown";
      }
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  public static String getGitCommit() {
    return GIT_COMMIT;
  }

  private final static String VERSION;

  static {
    InputStream manifestStream = Hank.class.getResourceAsStream("META-INF/MANIFEST.MF");
    try {
      Manifest manifest = new Manifest(manifestStream);
      Attributes attributes = manifest.getMainAttributes();
      String temp = attributes.getValue("Implementation-Version");
      if (temp != null) {
        VERSION = temp;
      } else {
        VERSION = "Unknown";
      }
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  public static String getVersion() {
    return VERSION;
  }
}
