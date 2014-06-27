package com.liveramp.hank.ui;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;

import org.apache.commons.lang.StringUtils;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.liveramp.hank.coordinator.Domain;
import com.liveramp.hank.coordinator.DomainAndVersion;
import com.liveramp.hank.coordinator.DomainGroup;
import com.liveramp.hank.coordinator.DomainVersion;
import com.liveramp.hank.coordinator.Host;
import com.liveramp.hank.coordinator.HostState;
import com.liveramp.hank.coordinator.Ring;
import com.liveramp.hank.coordinator.RingGroup;
import com.liveramp.hank.coordinator.UpdateProgress;
import com.liveramp.hank.coordinator.UpdateProgressAggregator;
import com.liveramp.hank.partition_server.DoublePopulationStatisticsAggregator;
import com.liveramp.hank.partition_server.FilesystemStatisticsAggregator;
import com.liveramp.hank.partition_server.RuntimeStatisticsAggregator;
import com.liveramp.hank.util.FormatUtils;

public class UiUtils {

  private UiUtils() {
  }

  private static final int BAR_SIZE = 100;
  private static DateTimeFormatter dateFormat = DateTimeFormat.forPattern("d MMM yyyy HH:mm:ss");

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
    for (DomainAndVersion version : domainGroup.getDomainVersionsSorted()) {
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
      return dateFormat.print(closedAt);
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

  public static String formatFilesystemStatistics(FilesystemStatisticsAggregator filesystemStatistics) {
    return
        FormatUtils.formatDouble(filesystemStatistics.getUsedPercentage()) + "% used, "
            + FormatUtils.formatNumBytes(filesystemStatistics.getUsedSpace())
            + "/" + FormatUtils.formatNumBytes(filesystemStatistics.getTotalSpace())
        ;
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
        l1Str = FormatUtils.formatDouble(l1 * 100.0) + "%";
      }
      if (l2 != 0) {
        l2Str = FormatUtils.formatDouble(l2 * 100.0) + "%";
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

    content.append(FormatUtils.formatDouble(updateProgress.getUpdateProgress() * 100) + "% up-to-date"
        + " (" + updateProgress.getNumPartitionsUpToDate() + "/" + updateProgress.getNumPartitions() + ")");
    if (eta >= 0) {
      content.append(" ETA: " + FormatUtils.formatSecondsDuration(eta));
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
      tooltip.append(FormatUtils.formatDouble(domainUpdateProgress.getUpdateProgress() * 100) + "%");
      tooltip.append("</td></tr>");
    }
    tooltip.append("</table>");
    return htmlTooltip(content.toString(), "Update Progress", tooltip.toString());
  }

  public static String join(List<String> input, String separator) {
    return StringUtils.join(input, separator);
  }
}
