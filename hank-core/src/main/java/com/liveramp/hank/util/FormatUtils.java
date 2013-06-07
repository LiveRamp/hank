package com.liveramp.hank.util;

import java.text.DecimalFormat;

public class FormatUtils {

  public static String formatNumBytes(long numBytes) {
    long kilo = 1 << 10;
    long mega = kilo << 10;
    long giga = mega << 10;
    long tera = giga << 10;
    long peta = tera << 10;
    if (numBytes < kilo) {
      return numBytes + "B";
    } else if (numBytes < mega) {
      return formatDouble((double) numBytes / kilo) + "KB";
    } else if (numBytes < giga) {
      return formatDouble((double) numBytes / mega) + "MB";
    } else if (numBytes < tera) {
      return formatDouble((double) numBytes / giga) + "GB";
    } else if (numBytes < peta) {
      return formatDouble((double) numBytes / tera) + "TB";
    } else {
      return formatDouble((double) numBytes / peta) + "PB";
    }
  }

  public static String formatDouble(double value) {
    return new DecimalFormat("#.##").format(value);
  }

  public static String formatDataThroughput(double bytesThrougput) {
    return formatNumBytes(Math.round(bytesThrougput)) + "/s";
  }

  public static String formatSecondsDuration(long secondsDuration) {
    if (secondsDuration < 0) {
      return "-";
    } else {
      long hours = secondsDuration / 3600;
      long remainder = secondsDuration % 3600;
      long minutes = remainder / 60;
      long seconds = remainder % 60;

      StringBuilder result = new StringBuilder();
      // Hours
      if (secondsDuration >= 3600) {
        result.append(hours);
        result.append("h");
      }
      // Minutes
      if (secondsDuration >= 60) {
        if (minutes < 10) {
          result.append("0");
        }
        result.append(minutes);
        result.append("m");
      }
      // Seconds
      if (seconds < 10) {
        result.append("0");
      }
      result.append(seconds);
      result.append("s");

      return result.toString();
    }
  }
}
