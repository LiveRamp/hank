<%@ page language="java" contentType="text/html; charset=ISO-8859-1"
    pageEncoding="ISO-8859-1"%>

<%@page import="com.rapleaf.hank.ui.*"%>
<%@page import="com.rapleaf.hank.coordinator.*"%>
<%@page import="com.rapleaf.hank.partition_server.*"%>
<%@page import="java.util.*"%>
<%@page import="java.text.DecimalFormat" %>

<%
  Coordinator coord = (Coordinator)getServletContext().getAttribute("coordinator");
%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">

<%@page import="java.net.URLEncoder"%><html>
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

  <jsp:include page="_head.jsp" />
</head>
<body>

  <script type="text/javascript">
    addAsyncReload(['ALL-RING-GROUPS']);
  </script>

  <jsp:include page="_top_nav.jsp" />

  <h1>Ring Groups</h1>

  <table id='all-ring-groups' class='table-blue ALL-RING-GROUPS'>
    <tr>
      <th>Ring Group</th>
      <th>Domain Group</th>
      <th>Target Version</th>
      <th></th>
      <th>Hosts</th>
      <th>Serving</th>
      <th>Updating</th>
      <th>Idle</th>
      <th>Offline</th>
      <th>Throughput</th>
      <th>Latency</th>
      <th>Hit rate</th>
      <th>Up-to-date & Served</th>
      <th>(fully)</th>
    </tr>
    <%
      for (RingGroup ringGroup : coord.getRingGroupsSorted()) {
    %>
      <tr>
        <td><a href="/ring_group.jsp?name=<%= URLEnc.encode(ringGroup.getName()) %>"><%= ringGroup.getName() %></a></td>
        <td><a href="domain_group.jsp?n=<%=URLEnc.encode(ringGroup.getDomainGroup().getName())%>"><%=ringGroup.getDomainGroup().getName()%></a></td>

        <%
        DomainGroupVersion targetDomainGroupVersion = ringGroup.getTargetVersion();
        UpdateProgress progress = null;
        if (targetDomainGroupVersion != null &&
            !RingGroups.isUpToDate(ringGroup, targetDomainGroupVersion)) {
          progress = RingGroups.computeUpdateProgress(ringGroup, targetDomainGroupVersion);
        }
        %>

        <td class='centered'><%= targetDomainGroupVersion != null ?
        UiUtils.formatDomainGroupVersionInfoTooltip(targetDomainGroupVersion,
        "<a href='/domain_group.jsp?n=" + URLEnc.encode(targetDomainGroupVersion.getDomainGroup().getName()) +
        "'>" + targetDomainGroupVersion.getVersionNumber() + "</a>") : "-" %></td>

        <% if (progress != null) { %>
        <td class='centered'>
          <%= new DecimalFormat("#.##").format(progress.getUpdateProgress() * 100) %>%
          (<%= progress.getNumPartitionsUpToDate() %>/<%= progress.getNumPartitions() %>)
          <div class='progress-bar'>
            <div class='progress-bar-filler' style='width: <%= Math.round(progress.getUpdateProgress() * 100) %>%'></div>
          </div>
        </td>
        <% } else { %>
        <td></td>
        <% } %>

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

        <!-- Statistics -->

        <%
        Map<Ring, Map<Host, Map<Domain, RuntimeStatisticsAggregator>>> runtimeStatistics =
          RingGroups.computeRuntimeStatistics(coord, ringGroup);
        RuntimeStatisticsAggregator runtimeStatisticsForRingGroup =
          RingGroups.computeRuntimeStatisticsForRingGroup(runtimeStatistics);
        %>

        <td class='centered'> <%= new DecimalFormat("#.##").format(runtimeStatisticsForRingGroup.getThroughput()) %> qps
        (<%= UiUtils.formatDataThroughput(runtimeStatisticsForRingGroup.getResponseDataThroughput()) %>)</td>
        <td class='centered'><%= UiUtils.formatPopulationStatistics("Server-side latency on " + ringGroup.getName(), runtimeStatisticsForRingGroup.getGetRequestsPopulationStatistics()) %></td>
        <td class='centered'> <%= new DecimalFormat("#.##").format(runtimeStatisticsForRingGroup.getHitRate() * 100) %>% </td>

        <!-- Serving Status -->

        <%
        ServingStatusAggregator servingStatusAggregator = null;
        ServingStatus servingStatus = null;
        ServingStatus uniquePartitionsServingStatus = null;
        if (targetDomainGroupVersion != null) {
          servingStatusAggregator = RingGroups.computeServingStatusAggregator(ringGroup, targetDomainGroupVersion);
          servingStatus = servingStatusAggregator.computeServingStatus();
          uniquePartitionsServingStatus = servingStatusAggregator.computeUniquePartitionsServingStatus(targetDomainGroupVersion);
        }
        %>
        <% if (servingStatusAggregator != null) { %>
          <% if (servingStatus.getNumPartitionsServedAndUpToDate() != 0
                 && servingStatus.getNumPartitionsServedAndUpToDate() == servingStatus.getNumPartitions()) { %>
            <td class='centered complete'>
          <% } else { %>
            <td class='centered error'>
          <% } %>
          <%= servingStatus.getNumPartitionsServedAndUpToDate() %> / <%= servingStatus.getNumPartitions() %>
          </td>
          <% if (uniquePartitionsServingStatus.getNumPartitionsServedAndUpToDate() != 0
                 && uniquePartitionsServingStatus.getNumPartitionsServedAndUpToDate() == uniquePartitionsServingStatus.getNumPartitions()) { %>
            <td class='centered complete'>
          <% } else { %>
            <td class='centered error'>
          <% } %>
          <%= uniquePartitionsServingStatus.getNumPartitionsServedAndUpToDate() %> / <%= uniquePartitionsServingStatus.getNumPartitions() %>
          </td>
        <% } else { %>
          <td></td>
          <td></td>
        <% } %>

      </tr>
      <%
    }
    %>
  </table>

<jsp:include page="_footer.jsp"/>

</body>
</html>
