package com.rapleaf.hank.ui;

import com.rapleaf.hank.coordinator.DomainGroupVersion;
import com.rapleaf.hank.coordinator.DomainGroupVersionDomainVersion;
import com.rapleaf.hank.coordinator.DomainVersion;
import com.rapleaf.hank.coordinator.HostState;
import com.rapleaf.hank.partition_server.DoublePopulationStatisticsAggregator;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
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

  public static String formatDomainGroupVersionInfo(DomainGroupVersion domainGroupVersion, String text) throws IOException {
    String title = domainGroupVersion.getDomainGroup().getName() + " version " + domainGroupVersion.getVersionNumber()
        + " created on " + formatDomainGroupVersionCreatedAt(domainGroupVersion);
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
