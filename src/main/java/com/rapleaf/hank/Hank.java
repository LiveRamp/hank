package com.rapleaf.hank;

import java.io.IOException;
import java.io.InputStream;
import java.util.jar.Attributes;
import java.util.jar.Manifest;

public final class Hank {

  private Hank() {
  }

  private static String gitCommit = null;

  public synchronized static String getGitCommit() {
    if (gitCommit == null) {
      InputStream manifestStream = Hank.class.getResourceAsStream("META-INF/MANIFEST.MF");
      try {
        Manifest manifest = new Manifest(manifestStream);
        Attributes attributes = manifest.getMainAttributes();
        String temp = attributes.getValue("Implementation-Build");
        if (temp != null) {
          gitCommit = temp;
        } else {
          gitCommit = "Unknown";
        }
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    }
    return gitCommit;
  }

  private static String version = null;

  public static String getVersion() {
    if (version == null) {

      InputStream manifestStream = Hank.class.getResourceAsStream("META-INF/MANIFEST.MF");
      try {
        Manifest manifest = new Manifest(manifestStream);
        Attributes attributes = manifest.getMainAttributes();
        String temp = attributes.getValue("Implementation-Version");
        if (temp != null) {
          version = temp;
        } else {
          version = "Unknown";
        }
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    }
    return version;
  }
}
