package com.rapleaf.hank.ui;

import com.rapleaf.hank.coordinator.*;
import com.rapleaf.hank.partition_server.DoublePopulationStatisticsAggregator;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;

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
    content.append(' ');
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

  public static String formatDomainGroupVersionTable(DomainGroupVersion domainGroupVersion,
                                                     String cssClass,
                                                     boolean linkToDomains)
      throws IOException {
    StringBuilder content = new StringBuilder();
    content.append("<table class='" + cssClass + "'><tr><th>Domain</th><th>Version</th><th>Closed On</th></tr>");
    for (DomainGroupVersionDomainVersion version : domainGroupVersion.getDomainVersionsSorted()) {
      content.append("<tr><td>");
      if (linkToDomains) {
        content.append("<a href='/domain.jsp?n=" + URLEnc.encode(version.getDomain().getName()) + "'>");
        content.append(version.getDomain().getName());
        content.append("</a>");
      } else {
        content.append(version.getDomain().getName());
      }
      content.append("</td><td>");
      content.append(version.getVersion().toString());
      content.append("</td><td>");
      content.append(formatDomainVersionClosedAt(version.getDomain().getVersionByNumber(version.getVersion())));
      content.append("</td></tr>");
    }
    content.append("</table>");
    return content.toString();
  }

  public static String formatDomainGroupVersionInfo(DomainGroupVersion domainGroupVersion) {
    return domainGroupVersion.getDomainGroup().getName() + " version " + domainGroupVersion.getVersionNumber()
        + " created on " + formatDomainGroupVersionCreatedAt(domainGroupVersion);
  }

  public static String formatDomainGroupVersionInfoTooltip(DomainGroupVersion domainGroupVersion, String text) throws IOException {
    String title = formatDomainGroupVersionInfo(domainGroupVersion);
    String content = formatDomainGroupVersionTable(domainGroupVersion, "domain-group-info", false);
    return htmlTooltip(text, title, content);
  }

  public static String formatDomainGroupVersionCreatedAt(DomainGroupVersion domainGroupVersion) {
    Long createdAt = domainGroupVersion.getCreatedAt();
    if (createdAt == null) {
      return "-";
    } else {
      return dateFormat.format(new Date(createdAt));
    }
  }

  public static String formatDomainVersionClosedAt(DomainVersion domainVersion) throws IOException {
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
}
