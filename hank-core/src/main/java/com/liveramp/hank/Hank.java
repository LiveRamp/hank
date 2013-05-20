package com.liveramp.hank;

import java.io.IOException;
import java.net.URL;
import java.util.Enumeration;
import java.util.jar.Attributes;
import java.util.jar.Manifest;

public final class Hank {

  private Hank() {
  }

  private static String gitCommit = null;

  public synchronized static String getGitCommit() {
    if (gitCommit == null) {
      gitCommit = getProperty("Implementation-Build");
    }
    return gitCommit;
  }

  private static String version = null;

  public static String getVersion() {
    if (version == null) {
      version = getProperty("Implementation-Version");
    }
    return version;
  }

  public static String getProperty(String property) {
    try {
      Enumeration<URL> manifestStream = Hank.class.getClassLoader().getResources("META-INF/MANIFEST.MF");
      if (!manifestStream.hasMoreElements()) {
        return "No Manifest";
      }

      while (manifestStream.hasMoreElements()) {
        Manifest manifest = new Manifest(manifestStream.nextElement().openStream());
        Attributes attributes = manifest.getMainAttributes();
        String title = attributes.getValue("Implementation-Title");

        if (title != null && isValidTitle(title)) {
          String temp = attributes.getValue(property);
          if (temp != null) {
            return temp;
          } else {
            return "Not in Manifest";
          }
        }
      }
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }

    return "Unknown";
  }

  private static boolean isValidTitle(String title) {
    return title.equals("hank-core")
        || title.equals("hank-client")
        || title.equals("hank-server");
  }
}
