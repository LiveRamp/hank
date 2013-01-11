package com.rapleaf.hank;

import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public final class Hank {
  private static final Logger LOG = Logger.getLogger(Hank.class);

  private Hank() {
  }

  private final static String GIT_COMMIT;

  static {
    String temp = "UNKNOWN";

    InputStream s = Hank.class.getClassLoader().getResourceAsStream("git-commit.txt");
    if (s != null) {
      BufferedReader r = new BufferedReader(new InputStreamReader(s));

      try {
        temp = r.readLine();
        r.close();
      } catch (IOException e) {
        LOG.warn("couldn't load git-commit.txt from the jar.", e);
      }
    }
    GIT_COMMIT = temp;
  }

  public static String getGitCommit() {
    return GIT_COMMIT;
  }
}
