package com.rapleaf.hank.ui;

import com.rapleaf.hank.coordinator.HostState;
import com.rapleaf.hank.partition_server.DoublePopulationStatisticsAggregator;

import java.io.IOException;
import java.util.UUID;

public class UiUtils {

  private static final int BAR_SIZE = 100;

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
      size = Math.round(BAR_SIZE * (value / maximum));
    }
    if (size < 1) {
      size = 1;
    }
    content.append("<tr><td>");
    content.append(DoublePopulationStatisticsAggregator.formatDouble(value));
    content.append(' ');
    content.append(unit);
    content.append("</td><td>");
    content.append("<div class='tooltipBar' style='width: " + size + "px;'></div>");
    content.append("</td><td>");
    content.append(label);
    content.append("</td></tr>");
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

    String uniqueId = UUID.randomUUID().toString().replaceAll("-", "_");
    return "<script type=\"text/javascript\">"
        + "var tooltipContent_" + uniqueId + " = \"" + tooltipContent.toString() + "\";"
        + "</script>"
        + "<div onmouseover=\"tooltip.show(tooltipContent_" + uniqueId
        + ", '" + title + "');\" onmouseout=\"tooltip.hide();\">"
        + populationStatistics.format()
        + " ms"
        + "</div>";
  }
}
