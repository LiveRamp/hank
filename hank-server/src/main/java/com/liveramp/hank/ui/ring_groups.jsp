  <%@ page language="java" contentType="text/html; charset=ISO-8859-1"
           pageEncoding="ISO-8859-1" %>

    <%@page import="com.liveramp.hank.ui.*" %>
    <%@page import="com.liveramp.hank.util.*" %>
    <%@page import="com.liveramp.hank.coordinator.*" %>
    <%@page import="com.liveramp.hank.partition_server.*" %>
    <%@page import="java.util.*" %>
    <%@page import="java.text.DecimalFormat" %>

      <%
  Coordinator coord = (Coordinator)getServletContext().getAttribute("coordinator");
%>
    <!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">

    <%@page import="java.net.URLEncoder" %><html>
    <head>
    <meta http-equiv="Content-Type" content="text/html; charset=ISO-8859-1">
    <title>Hank: Ring Groups</title>

    <script type="text/javascript">
    function validateCreate() {
    var ringGroupName = document.getElementById('rgName');
    if (ringGroupName.value.match(/^ *$/)) {
    alert("Ring group names must contain some non-space characters. (Leading and trailing spaces are OK.)");
    return false;
    }
    if (ringGroupName.value.match(/^\./)) {
    alert("Ring group names may not start with a '.'");
    return false;
    }
    return true;
    }
    </script>

    <jsp:include page="_head.jsp"/>
    </head>
    <body>

    <script type="text/javascript">
    addAsyncReload(['ALL-RING-GROUPS']);
    </script>

    <jsp:include page="_top_nav.jsp"/>

    <h1>Ring Groups</h1>

    <table id='all-ring-groups' class='table-blue ALL-RING-GROUPS'>
    <tr>
    <th>Ring Group</th>
    <th>Domain Group</th>
    <th></th>
    <th>Hosts</th>
    <th>Serving</th>
    <th>Updating</th>
    <th>Idle</th>
    <th>Offline</th>
    <th>Clients</th>
    <th>Throughput</th>
    <th>Latency</th>
    <th>Hit rate</th>
    <th>Cache Hits</th>
    <th>Up-to-date & Served</th>
    <th>File System</th>
    </tr>
      <%
      for (RingGroup ringGroup : coord.getRingGroupsSorted()) {
    %>
    <tr>
    <td class='centered'>
    <a href="/ring_group.jsp?name=<%= URLEnc.encode(ringGroup.getName()) %>"><%= ringGroup.getName() %></a>
    </td>

      <%
        UpdateProgressAggregator progress = null;
        if (!RingGroups.isUpToDate(ringGroup, ringGroup.getDomainGroup())) {
          progress = RingGroups.computeUpdateProgress(ringGroup, ringGroup.getDomainGroup());
        }
        %>

    <td class='centered'>
      <%= UiUtils.formatDomainGroupInfoTooltip(ringGroup.getDomainGroup(),
        "<a href='/domain_group.jsp?n=" + URLEnc.encode(ringGroup.getDomainGroup().getName()) +
        "'>" + ringGroup.getDomainGroup().getName() + "</a>") %>
    </td>

    <td><%= UiUtils.formatUpdateProgress(progress) %></td>

    <!-- Hosts State -->

      <%
        Set<Host> hostsAll = RingGroups.getHosts(ringGroup);
        Set<Host> hostsServing = RingGroups.getHostsInState(ringGroup, HostState.SERVING);
        Set<Host> hostsUpdating = RingGroups.getHostsInState(ringGroup, HostState.UPDATING);
        Set<Host> hostsIdle = RingGroups.getHostsInState(ringGroup, HostState.IDLE);
        Set<Host> hostsOffline = RingGroups.getHostsInState(ringGroup, HostState.OFFLINE);
        %>

    <td class='host-total'>
      <%= UiUtils.formatHostListTooltip(ringGroup, hostsAll) %>
    </td>
      <% if (hostsServing.size() != 0 && hostsServing.size() == hostsAll.size()) { %>
    <td class='host-serving'>
      <% } else if (hostsServing.size() != 0) { %>
    <td class='host-serving-incomplete'>
      <% } else { %>
    <td class='centered'>
      <% } %>
      <%= UiUtils.formatHostListTooltip(ringGroup, hostsServing) %>
    </td>
      <% if (hostsUpdating.size() != 0) { %>
    <td class='host-updating'><%= UiUtils.formatHostListTooltip(ringGroup, hostsUpdating) %></td>
      <% } else { %>
    <td class='centered'>-</td>
      <% } %>
      <% if (hostsIdle.size() != 0) { %>
    <td class='host-idle'><%= UiUtils.formatHostListTooltip(ringGroup, hostsIdle) %></td>
      <% } else { %>
    <td class='centered'>-</td>
      <% } %>
      <% if (hostsOffline.size() != 0) { %>
    <td class='host-offline'><%= UiUtils.formatHostListTooltip(ringGroup, hostsOffline) %></td>
      <% } else { %>
    <td class='centered'>-</td>
      <% } %>

    <td class='centered'><%= ringGroup.getClients().size() %>

    <!-- Statistics -->

      <%
        Map<Ring, Map<Host, Map<Domain, RuntimeStatisticsAggregator>>> runtimeStatistics =
          RingGroups.computeRuntimeStatistics(coord, ringGroup);
        RuntimeStatisticsAggregator runtimeStatisticsForRingGroup =
          RingGroups.computeRuntimeStatisticsForRingGroup(runtimeStatistics);

        Map<Ring, Map<Host, Map<String, FilesystemStatisticsAggregator>>> filesystemStatistics =
          RingGroups.computeFilesystemStatistics(ringGroup);

        FilesystemStatisticsAggregator filesystemStatisticsForRingGroup =
          RingGroups.computeFilesystemStatisticsForRingGroup(filesystemStatistics);
        %>

    <td class='centered'><%= FormatUtils.formatDouble(runtimeStatisticsForRingGroup.getThroughput()) %>
    qps
    (<%= FormatUtils.formatDataThroughput(runtimeStatisticsForRingGroup.getResponseDataThroughput()) %>)</td>
    <td
    class='centered'><%= UiUtils.formatPopulationStatistics("Server-side latency on " + ringGroup.getName(), runtimeStatisticsForRingGroup.getGetRequestsPopulationStatistics()) %>
    </td>
    <td class='centered'><%= FormatUtils.formatDouble(runtimeStatisticsForRingGroup.getHitRate() * 100) %>% </td>
    <td class='centered'><%= UiUtils.formatCacheHits(runtimeStatisticsForRingGroup) %></td>

    <!-- Serving Status -->

      <%
        ServingStatusAggregator servingStatusAggregator = RingGroups.computeServingStatusAggregator(ringGroup, ringGroup.getDomainGroup());
        ServingStatus servingStatus = servingStatusAggregator.computeServingStatus();
        %>
      <% if (servingStatus.getNumPartitionsServedAndUpToDate() != 0
                 && servingStatus.getNumPartitionsServedAndUpToDate() == servingStatus.getNumPartitions()) { %>
    <td class='centered complete'>
      <% } else { %>
    <td class='centered error'>
      <% } %>
      <%= servingStatus.getNumPartitionsServedAndUpToDate() %> / <%= servingStatus.getNumPartitions() %>
    </td>

    <!-- File system -->
    <td>
      <%= UiUtils.formatFilesystemStatistics(filesystemStatisticsForRingGroup) %>
    <div class='progress-bar'>
    <div class='progress-bar-filler-used'
    style='width:<%= Math.round(filesystemStatisticsForRingGroup.getUsedPercentage()) %>%'></div>
    </div>
    </td>

    </tr>
      <%
    }
    %>
    </table>

    <jsp:include page="_footer.jsp"/>

    </body>
    </html>
