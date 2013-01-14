package com.rapleaf.hank;

import java.io.IOException;
import java.io.InputStream;
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

  public static String getProperty(String prop){
    try {
      Enumeration<URL> manifestStream = Hank.class.getClassLoader().getResources("META-INF/MANIFEST.MF");
      while(manifestStream.hasMoreElements()){
        Manifest manifest = new Manifest(manifestStream.nextElement().openStream());
        Attributes attributes = manifest.getMainAttributes();
        if(attributes.containsKey("Implementation-Title")){
          String title = attributes.getValue("Implementation-Title");
          if(title.equals("hank")){
            String temp = attributes.getValue(prop);
            if (temp != null) {
              return temp;
            }else{
              return "Not in Manifest";
            }
          }
        }
      }
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }

    return "Unknown";
  }
}
