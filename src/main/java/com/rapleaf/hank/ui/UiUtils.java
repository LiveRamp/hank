package com.rapleaf.hank.ui;

import com.rapleaf.hank.coordinator.*;
import com.rapleaf.hank.partition_server.DoublePopulationStatisticsAggregator;
import com.rapleaf.hank.partition_server.FilesystemStatisticsAggregator;
import com.rapleaf.hank.partition_server.RuntimeStatisticsAggregator;
import org.apache.commons.lang.StringUtils;

import java.io.IOException;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.*;

public class UiUtils {

  private UiUtils() {
  }

  private static final int BAR_SIZE = 100;
  private static DateFormat dateFormat = new SimpleDateFormat("d MMM yyyy HH:mm:ss");

  public static String hostStateToClass(HostState state) throws IOException {
    switch (state) {
      case SERVING:
        return "host-serving";
      case UPDATING:
        return "host-updating";
      case IDLE:
        return "host-idle";
      case OFFLINE:
        return "host-offline";
      default:
        throw new RuntimeException("Unknown host state.");
    }
  }

  public static String formatHostListTooltip(Ring ring,
                                             Set<Host> hosts) throws IOException {
    return formatHostListTooltip(ring.getRingGroup().getName() + " ring " + ring.getRingNumber(), hosts);
  }

  public static String formatHostListTooltip(RingGroup ringGroup,
                                             Set<Host> hosts) throws IOException {
    return formatHostListTooltip(ringGroup.getName(), hosts);
  }

  private static String formatHostListTooltip(String title,
                                              Set<Host> hosts) throws IOException {
    if (hosts.size() == 0) {
      return "-";
    } else {
      TreeSet<Host> sortedHosts = new TreeSet<Host>(hosts);
      StringBuilder content = new StringBuilder();
      for (Host host : sortedHosts) {
        content.append("<div class='" + hostStateToClass(host.getState()) + "'>");
        content.append(host.getAddress().toString());
        content.append("</div>");
      }
      return htmlTooltip(Integer.toString(hosts.size()), title, content.toString());
    }
  }

  private static void addBar(StringBuilder content, String label, Double value, Double maximum, String unit) {
    if (value == null) {
      value = 0.0;
    }
    if (maximum == null) {
      maximum = 0.0;
    }
    long size = 0;
    if (maximum != 0) {
      size = Math.round(BAR_SIZE * (Math.log(1 + value) / Math.log(1 + maximum)));
    }
    if (size < 1) {
      size = 1;
    }
    content.append("<tr><td>");
    content.append(label);
    content.append("</td><td>");
    content.append("<div class='tooltipBar' style='width: " + size + "px;'></div>");
    content.append("</td><td>");
    content.append(DoublePopulationStatisticsAggregator.formatDouble(value));
    content.append(unit);
    content.append("</td></tr>");
  }

  private static String htmlTooltip(String text, String title, String content) {
    String uniqueId = "tooltipContent_" + UUID.randomUUID().toString().replaceAll("-", "_");
    return "<div id=\"" + uniqueId + "\" style=\"display: none;\">" + content + "</div>"
        + "<div style=\"cursor: help;\" onmouseover=\"tooltip.show('" + uniqueId
        + "', '" + title + "');\" onmouseout=\"tooltip.hide();\">"
        + text
        + "</div>";
  }

  public static String formatDomainGroupDomainVersionsTable(DomainGroup domainGroup,
                                                            String cssClass,
                                                            boolean linkToDomains)
      throws IOException {
    StringBuilder content = new StringBuilder();
    content.append("<table class='" + cssClass + "'><tr><th>Domain</th><th>Version</th><th>Closed On</th></tr>");
    for (DomainGroupDomainVersion version : domainGroup.getDomainVersionsSorted()) {
      content.append("<tr><td class='centered'>");
      if (linkToDomains) {
        content.append("<a href='/domain.jsp?n=" + URLEnc.encode(version.getDomain().getName()) + "'>");
        content.append(version.getDomain().getName());
        content.append("</a>");
      } else {
        content.append(version.getDomain().getName());
      }
      content.append("</td><td class='centered'>");
      content.append(version.getVersionNumber());
      content.append("</td><td class='centered'>");
      content.append(formatDomainVersionClosedAt(version.getDomain().getVersion(version.getVersionNumber())));
      content.append("</td></tr>");
    }
    content.append("</table>");
    return content.toString();
  }

  public static String formatDomainGroupInfoTooltip(DomainGroup domainGroup, String text) throws IOException {
    String title = domainGroup.getName();
    String content = formatDomainGroupDomainVersionsTable(domainGroup, "domain-group-info", false);
    return htmlTooltip(text, title, content);
  }

  public static String formatDomainVersionClosedAt(DomainVersion domainVersion) throws IOException {
    if (domainVersion == null) {
      return "?";
    }
    Long closedAt = domainVersion.getClosedAt();
    if (closedAt == null) {
      return "-";
    } else {
      return dateFormat.format(new Date(closedAt));
    }
  }

  public static String formatPopulationStatistics(String title,
                                                  DoublePopulationStatisticsAggregator populationStatistics) {
    if (populationStatistics == null) {
      return "-";
    } else {
      double[] deciles = populationStatistics.computeDeciles();
      StringBuilder tooltipContent = new StringBuilder();

      tooltipContent.append("<table>");
      addBar(tooltipContent, "min", populationStatistics.getMinimum(), populationStatistics.getMaximum(), "ms");
      for (int i = 0; i < 9; ++i) {
        addBar(tooltipContent, ((i + 1) * 10) + "%", deciles[i], populationStatistics.getMaximum(), "ms");
      }
      addBar(tooltipContent, "max", populationStatistics.getMaximum(), populationStatistics.getMaximum(), "ms");
      tooltipContent.append("</table>");

      return htmlTooltip(populationStatistics.format(), title, tooltipContent.toString());
    }
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

  public static String formatDataThroughput(double bytesThrougput) {
    return formatNumBytes(Math.round(bytesThrougput)) + "/s";
  }

  public static String formatFilesystemStatistics(FilesystemStatisticsAggregator filesystemStatistics) {
    return
        formatDouble(filesystemStatistics.getUsedPercentage()) + "% used, "
            + UiUtils.formatNumBytes(filesystemStatistics.getUsedSpace())
            + "/" + UiUtils.formatNumBytes(filesystemStatistics.getTotalSpace())
        ;
  }

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

  public static String formatCacheHits(RuntimeStatisticsAggregator runtimeStatisticsAggregator) {
    double l1 = runtimeStatisticsAggregator.getL1CacheHitRate();
    double l2 = runtimeStatisticsAggregator.getL2CacheHitRate();
    if (l1 == 0 && l2 == 0) {
      return "-";
    } else {
      String l1Str = "-";
      String l2Str = "-";
      if (l1 != 0) {
        l1Str = formatDouble(l1 * 100.0) + "%";
      }
      if (l2 != 0) {
        l2Str = formatDouble(l2 * 100.0) + "%";
      }
      return l1Str + " / " + l2Str;
    }
  }

  public static String formatUpdateProgress(UpdateProgressAggregator updateProgressAggregator) {
    return formatUpdateProgress(updateProgressAggregator, -1);
  }

  public static String formatUpdateProgress(UpdateProgressAggregator updateProgressAggregator, long eta) {
    if (updateProgressAggregator == null) {
      return "";
    }
    StringBuilder content = new StringBuilder();
    StringBuilder tooltip = new StringBuilder();
    UpdateProgress updateProgress = updateProgressAggregator.computeUpdateProgress();

    content.append(UiUtils.formatDouble(updateProgress.getUpdateProgress() * 100) + "% up-to-date"
        + " (" + updateProgress.getNumPartitionsUpToDate() + "/" + updateProgress.getNumPartitions() + ")");
    if (eta >= 0) {
      content.append(" ETA: " + UiUtils.formatSecondsDuration(eta));
    }
    content.append("<div class=\'progress-bar\'><div class=\'progress-bar-filler\' style=\'width: "
        + Math.round(updateProgress.getUpdateProgress() * 100) + "%\'></div></div>");

    // Build tooltip
    tooltip.append("<table>");
    for (Map.Entry<Domain, UpdateProgress> entry : updateProgressAggregator.sortedEntrySet()) {
      UpdateProgress domainUpdateProgress = entry.getValue();
      tooltip.append("<tr><td class='centered'>");
      tooltip.append(entry.getKey().getName());
      tooltip.append("</td><td class='centered'>");
      tooltip.append("<div class=\'progress-bar\'><div class=\'progress-bar-filler\' style=\'width: "
          + Math.round(domainUpdateProgress.getUpdateProgress() * 100) + "%\'></div></div>");
      tooltip.append("</td><td class='centered'>");
      tooltip.append(formatDouble(domainUpdateProgress.getUpdateProgress() * 100) + "%");
      tooltip.append("</td></tr>");
    }
    tooltip.append("</table>");
    return htmlTooltip(content.toString(), "Update Progress", tooltip.toString());
  }

  public static String join(List<String> input, String separator) {
    return StringUtils.join(input, separator);
  }
}
