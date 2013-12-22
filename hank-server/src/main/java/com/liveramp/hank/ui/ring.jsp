  <%@ page language="java" contentType="text/html; charset=ISO-8859-1"
           pageEncoding="ISO-8859-1" %>

    <%@page import="com.liveramp.hank.ui.*" %>
    <%@page import="com.liveramp.hank.util.*" %>
    <%@page import="com.liveramp.hank.coordinator.*" %>
    <%@page import="com.liveramp.hank.partition_server.*" %>
    <%@page import="java.util.*" %>
    <%@page import="java.net.*" %>
    <%@page import="java.text.DecimalFormat" %>

      <%
Coordinator coord = (Coordinator)getServletContext().getAttribute("coordinator");
RingGroup ringGroup = coord.getRingGroup(URLEnc.decode(request.getParameter("g")));
Ring ring = ringGroup.getRing(Integer.parseInt(request.getParameter("n")));
%>

    <!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
    <html>
    <head>
    <meta http-equiv="Content-Type" content="text/html; charset=ISO-8859-1">
    <title>Ring <%=ring.getRingNumber()%>
    in group <%=ringGroup.getName()%></title>
    <jsp:include page="_head.jsp"/>
    </head>
    <body>

    <script type="text/javascript">
    addAsyncReload(['ALL-HOSTS']);
    addAsyncReload(['RING-STATE']);
    addAsyncReload(['DOMAIN-STATISTICS']);
    </script>

    <jsp:include page="_top_nav.jsp"/>

    <h1>
    Ring Group <a href="/ring_group.jsp?name=<%=URLEnc.encode(ringGroup.getName())%>"><%=ringGroup.getName()%></a>
    &gt;
    <span class='currentItem'>Ring <%=ring.getRingNumber()%></span>
    </h1>

      <%
    Map<Host, Map<Domain, RuntimeStatisticsAggregator>> runtimeStatistics =
      Rings.computeRuntimeStatistics(coord, ring);

    RuntimeStatisticsAggregator runtimeStatisticsForRing =
      Rings.computeRuntimeStatisticsForRing(runtimeStatistics);

    Map<Host, Map<String, FilesystemStatisticsAggregator>> filesystemStatistics =
      Rings.computeFilesystemStatistics(ring);

    FilesystemStatisticsAggregator filesystemStatisticsForRing =
      Rings.computeFilesystemStatisticsForRing(filesystemStatistics);

    long updateETA = Rings.computeUpdateETA(ring);

    DomainGroup domainGroup = ringGroup.getDomainGroup();
  %>

    <h2>State</h2>
    <table class='table-blue-compact RING-STATE'>

      <% if (updateETA >= 0) { %>
    <tr>
    <td>Update ETA:</td>
    <td>
      <%= FormatUtils.formatSecondsDuration(updateETA) %>
    </td>
    </tr>
      <% } %>

    <tr>
    <td>Throughput:</td>
    <td>
      <%= FormatUtils.formatDouble(runtimeStatisticsForRing.getThroughput()) %>
    qps
    (<%= FormatUtils.formatDataThroughput(runtimeStatisticsForRing.getResponseDataThroughput()) %>)
    </td>
    </tr>

    <tr>
    <td>Latency:</td>
    <td>
      <%= UiUtils.formatPopulationStatistics("Server-side latency on " + ringGroup.getName() + " Ring " + ring.getRingNumber(), runtimeStatisticsForRing.getGetRequestsPopulationStatistics()) %>
    </td>
    </tr>

    <tr>
    <td>Hit Rate:</td>
    <td>
      <%= FormatUtils.formatDouble(runtimeStatisticsForRing.getHitRate() * 100) %>%
    </td>
    </tr>

    <tr>
    <td>Cache Hits:</td>
    <td>
      <%= UiUtils.formatCacheHits(runtimeStatisticsForRing) %>
    </td>
    </tr>

    <tr>
    <td>Cache Size:</td>
    <td>
      <%= String.format("%,d", runtimeStatisticsForRing.getCacheStatistics().getNumItems())  %> items
      /
      <%= FormatUtils.formatNumBytes(runtimeStatisticsForRing.getCacheStatistics().getNumManagedBytes()) %>
    </td>
    </tr>

    <!-- Serving Status -->

      <%
        ServingStatusAggregator servingStatusAggregator = Rings.computeServingStatusAggregator(ring, domainGroup);
        ServingStatus servingStatus = servingStatusAggregator.computeServingStatus();
        %>

    <tr>
    <td>Up-to-date & Served</td>
      <% if (servingStatus.getNumPartitionsServedAndUpToDate() != 0
                 && servingStatus.getNumPartitionsServedAndUpToDate() == servingStatus.getNumPartitions()) { %>
    <td class='centered complete'>
      <% } else { %>
    <td class='centered error'>
      <% } %>
      <%= servingStatus.getNumPartitionsServedAndUpToDate() %> / <%= servingStatus.getNumPartitions() %>
    </td>
    </tr>

    <tr>
    <td>File System</td>
    <td>
      <%= UiUtils.formatFilesystemStatistics(filesystemStatisticsForRing) %>
    <div class='progress-bar'>
    <div class='progress-bar-filler-used'
    style='width:<%= Math.round(filesystemStatisticsForRing.getUsedPercentage()) %>%'></div>
    </div>
    </td>
    </tr>


    </table>

    <!-- Domain specific Runtime Statistics -->

    <table class='table-blue-compact DOMAIN-STATISTICS'>
    <tr>
    <th>Domain</th>
    <th>Throughput</th>
    <th>Latency</th>
    <th>Hit Rate</th>
    <th>Cache Hits</th>
    <th>Cache Size</th>
    </tr>
      <%
     SortedMap<Domain, RuntimeStatisticsAggregator> runtimeStatisticsForDomains = Rings.computeRuntimeStatisticsForDomains(runtimeStatistics);
     for (SortedMap.Entry<Domain, RuntimeStatisticsAggregator> entry : runtimeStatisticsForDomains.entrySet()) {
       Domain domain = entry.getKey();
       RuntimeStatisticsAggregator runtimeStatisticsForDomain = entry.getValue();
   %>
    <tr>
    <td><a href="/domain.jsp?n=<%= domain.getName() %>"><%= domain.getName() %></a></td>
    <td class='centered'><%= FormatUtils.formatDouble(runtimeStatisticsForDomain.getThroughput()) %>
    qps
    (<%= FormatUtils.formatDataThroughput(runtimeStatisticsForDomain.getResponseDataThroughput()) %>)</td>
    <td
    class='centered'><%= UiUtils.formatPopulationStatistics("Server-side latency for " + domain.getName() + " on " + ringGroup.getName() + " Ring " + ring.getRingNumber(), runtimeStatisticsForDomain.getGetRequestsPopulationStatistics()) %>
    </td>
    <td class='centered'><%= FormatUtils.formatDouble(runtimeStatisticsForDomain.getHitRate() * 100) %>%</td>
    <td class='centered'><%= UiUtils.formatCacheHits(runtimeStatisticsForDomain) %></td>
    <td class='centered'>
      <%= String.format("%,d", runtimeStatisticsForDomain.getCacheStatistics().getNumItems())  %> items
      /
      <%= FormatUtils.formatNumBytes(runtimeStatisticsForDomain.getCacheStatistics().getNumManagedBytes()) %>
    </td>
    </tr>
      <%
    }
  %>
    </table>

    <h2>Command All Hosts</h2>

    <form action="/ring/command_all" method=post>
    <input type=hidden name="rgName" value="<%=ringGroup.getName()%>"/>
    <input type=hidden name="ringNum" value="<%=ring.getRingNumber()%>"/>
    <select name="command">
    <option></option>
      <% for (HostCommand cmd : HostCommand.values()) { %>
    <option value="<%= cmd.name() %>"><%= cmd.name() %></option>
      <% } %>
    </select>
    <input type=submit value="Command"/>
    </form>

    <h2>Hosts</h2>

    <table class='table-blue ALL-HOSTS'>
    <tr>
    <th>Host Address</th>
    <th>State</th>
    <th></th>
    <th>Current Command</th>
    <th>Command Queue</th>
    <th>Throughput</th>
    <th>Latency</th>
    <th>Hit Rate</th>
    <th>Cache Hits</th>
    <th>Up-to-date & Served</th>
    <th>File System</th>
    <th>Flags</th>
    </tr>
      <%
      Collection<Host> hosts = ring.getHostsSorted();
      if (hosts != null) {
        for(Host host : hosts) {
        long hostUpdateETA = Hosts.computeUpdateETA(host);
    %>
    <tr>
    <td><a href="/host.jsp?g=<%= ringGroup.getName() %>&r=<%= ring.getRingNumber() %>&h=<%= URLEnc.encode(host.getAddress().toString()) %>"><%= host.getAddress() %></a></td>
    <td class='<%= UiUtils.hostStateToClass(host.getState()) %>'>
      <%= host.getState() %>
    </td>

      <%
      UpdateProgressAggregator progress = null;
      if (!Rings.isUpToDate(ring, domainGroup)) {
        progress = Hosts.computeUpdateProgress(host, domainGroup);
      }
      %>

    <td><%= UiUtils.formatUpdateProgress(progress, hostUpdateETA) %></td>

    <td class='centered'><%= host.getCurrentCommand() %></td>
    <td><%= host.getCommandQueue() %></td>

    <!-- Runtime Statistics -->
      <%
        RuntimeStatisticsAggregator runtimeStatisticsForHost =
          Rings.computeRuntimeStatisticsForHost(runtimeStatistics, host);

        FilesystemStatisticsAggregator filesystemStatisticsForHost =
          Rings.computeFilesystemStatisticsForHost(filesystemStatistics, host);
      %>

    <td class='centered'><%= FormatUtils.formatDouble(runtimeStatisticsForHost.getThroughput()) %>
    qps
    (<%= FormatUtils.formatDataThroughput(runtimeStatisticsForHost.getResponseDataThroughput()) %>)</td>
    <td
    class='centered'><%= UiUtils.formatPopulationStatistics("Server-side latency on " + host.getAddress(), runtimeStatisticsForHost.getGetRequestsPopulationStatistics()) %>
    </td>
    <td class='centered'><%= FormatUtils.formatDouble(runtimeStatisticsForHost.getHitRate() * 100) %>% </td>
    <td class='centered'><%= UiUtils.formatCacheHits(runtimeStatisticsForHost) %></td>

    <!-- Serving Status -->

      <%
        ServingStatusAggregator hostServingStatusAggregator = Hosts.computeServingStatusAggregator(host, domainGroup);
        ServingStatus hostServingStatus = hostServingStatusAggregator.computeServingStatus();
        %>

      <% if (hostServingStatus.getNumPartitionsServedAndUpToDate() != 0
                 && hostServingStatus.getNumPartitionsServedAndUpToDate() == hostServingStatus.getNumPartitions()) { %>
    <td class='centered complete'>
      <% } else { %>
    <td class='centered error'>
      <% } %>
      <%= hostServingStatus.getNumPartitionsServedAndUpToDate() %> / <%= hostServingStatus.getNumPartitions() %>
    </td>

    <!-- File system -->
    <td>
      <%= UiUtils.formatFilesystemStatistics(filesystemStatisticsForHost) %>
    <div class='progress-bar'>
    <div class='progress-bar-filler-used'
    style='width:<%= Math.round(filesystemStatisticsForHost.getUsedPercentage()) %>%'></div>
    </div>
    </td>

    <!-- Flags -->
    <td>
      <%= Hosts.joinHostFlags(host.getFlags()) %>
    </td>

    </tr>
      <%
        }
      }
    %>
    </table>

    <jsp:include page="_footer.jsp"/>

    </body>
    </html>
