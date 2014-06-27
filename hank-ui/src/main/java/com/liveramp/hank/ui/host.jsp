  <%@ page language="java" contentType="text/html; charset=ISO-8859-1"
           pageEncoding="ISO-8859-1" %>

    <%@page import="com.liveramp.hank.coordinator.*" %>
    <%@page import="com.liveramp.hank.partition_server.*" %>
    <%@page import="com.liveramp.hank.ui.*" %>
    <%@page import="com.liveramp.hank.util.*" %>
    <%@page import="java.util.*" %>
    <%@page import="java.text.DecimalFormat" %>
    <%@page import="org.joda.time.format.DateTimeFormat" %>

  <%
  Coordinator coord = (Coordinator)getServletContext().getAttribute("coordinator");
  RingGroup ringGroup = coord.getRingGroup(request.getParameter("g"));
  Ring ring = ringGroup.getRing(Integer.parseInt(request.getParameter("r")));
  Host host = ring.getHostByAddress(PartitionServerAddress.parse(URLEnc.decode(request.getParameter("h"))));
%>

    <!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
    <html>
    <head>
    <meta http-equiv="Content-Type" content="text/html; charset=ISO-8859-1">
    <title>Host: <%= host.getAddress() %></title>
    <jsp:include page="_head.jsp"/>

    <style type="text/css">
    div.part_assignment_visualization {float:left; padding: 3px}
    </style>
    </head>
    <body>

    <script type="text/javascript">
    addAsyncReload(['HOST-STATE']);
    addAsyncReload(['DOMAIN-STATISTICS']);
    addAsyncReload(['PARTITIONS-STATE']);
    </script>

    <jsp:include page="_top_nav.jsp"/>

    <h1>
    Ring Group <a href="/ring_group.jsp?name=<%=ringGroup.getName() %>"><%= ringGroup.getName() %></a>
    &gt;
    <a href="/ring.jsp?g=<%=ringGroup.getName() %>&n=<%= ring.getRingNumber() %>">Ring <%= ring.getRingNumber() %></a>
    &gt; <span class='currentItem'><%= host.getAddress() %></span>
    </h1>

    <!-- State and Commands -->


      <%
  Map<Domain, RuntimeStatisticsAggregator> runtimeStatistics =
    Hosts.computeRuntimeStatistics(coord, host);

  RuntimeStatisticsAggregator runtimeStatisticsForHost =
    Hosts.computeRuntimeStatisticsForHost(runtimeStatistics);

  Map<String, FilesystemStatisticsAggregator> filesystemStatistics =
    Hosts.computeFilesystemStatistics(host);

  FilesystemStatisticsAggregator filesystemStatisticsForHost =
    Hosts.computeFilesystemStatisticsForHost(filesystemStatistics);

  DomainGroup domainGroup = ringGroup.getDomainGroup();

  long updateETA = Hosts.computeUpdateETA(host);
%>

    <div>
    <h2>State</h2>
    <table class='table-blue-compact HOST-STATE'>
    <tr>
    <td>State:</td>
    <td class='centered<%= UiUtils.hostStateToClass(host.getState()) %>'>
      <%= host.getState() %>
    </td>
    </tr>

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
      <%= FormatUtils.formatDouble(runtimeStatisticsForHost.getThroughput()) %>
    qps
    (<%= FormatUtils.formatDataThroughput(runtimeStatisticsForHost.getResponseDataThroughput()) %>)
    </td>
    </tr>

    <tr>
    <td>Latency:</td>
    <td>
      <%= UiUtils.formatPopulationStatistics("Server-side latency on " + host.getAddress(), runtimeStatisticsForHost.getGetRequestsPopulationStatistics()) %>
    </td>
    </tr>

    <tr>
    <td>Hit Rate:</td>
    <td>
      <%= FormatUtils.formatDouble(runtimeStatisticsForHost.getHitRate() * 100) %>%
    </td>
    </tr>

    <tr>
    <td>Cache Hits:</td>
    <td>
      <%= UiUtils.formatCacheHits(runtimeStatisticsForHost) %>
    </td>
    </tr>

    <tr>
    <td>Cache Size:</td>
    <td>
      <%= String.format("%,d", runtimeStatisticsForHost.getCacheStatistics().getNumItems())  %> items
      /
      <%= FormatUtils.formatNumBytes(runtimeStatisticsForHost.getCacheStatistics().getNumManagedBytes()) %>
    </td>
    </tr>

    <tr>
    <td>Cache Max Size:</td>
    <td>
      <%= String.format("%,d", runtimeStatisticsForHost.getCacheStatistics().getMaxNumItems())  %> items
      /
      <%= FormatUtils.formatNumBytes(runtimeStatisticsForHost.getCacheStatistics().getMaxNumManagedBytes()) %>
    </td>
    </tr>

    <tr>
    <td>Uptime:</td>
    <td>
      <% Long upSince = host.getUpSince(); %>
      <% if (upSince == null) { %>
    undefined
      <% } else { %>
    Started <%= DateTimeFormat.forStyle("SS").print(upSince) %>
    (online for <%= FormatUtils.formatSecondsDuration((System.currentTimeMillis() - upSince) / 1000) %>)
      <% } %>
    </td>
    </tr>

    <tr>
    <td>Current Command:</td>
    <td>

    <form method="post" action="/host/discard_current_command">
    <input type="hidden" name="g" value="<%= ringGroup.getName() %>"/>
    <input type="hidden" name="n" value="<%= ring.getRingNumber() %>"/>
    <input type="hidden" name="h" value="<%= host.getAddress() %>"/>
      <%= host.getCurrentCommand() %>
      <% if (host.getCurrentCommand() != null) { %><input type="submit" value="discard"/><% } %>
    </form>
    </td>
    </tr>

    <tr>
    <td>Command Queue:</td>
    <td>
      <%= host.getCommandQueue() %>
    <form method=post action="/host/clear_command_queue" style='display: inline'>
    <input type="hidden" name="g" value="<%= ringGroup.getName() %>"/>
    <input type="hidden" name="n" value="<%= ring.getRingNumber() %>"/>
    <input type="hidden" name="h" value="<%= host.getAddress() %>"/>
    <input type=submit value="Clear"/>
    </form>
    </td>
    </tr>


      <% for (Map.Entry<String, FilesystemStatisticsAggregator> entry : filesystemStatistics.entrySet()) { %>
    <tr>
    <td><b><%= entry.getKey() %></b></td>
    <td>
      <%= UiUtils.formatFilesystemStatistics(entry.getValue()) %>
    <div class='progress-bar'>
    <div class='progress-bar-filler-used' style='width:<%= Math.round(entry.getValue().getUsedPercentage()) %>%'></div>
    </div>
    </td>
    </tr>
      <% } %>

    <tr>
    <td>Host Flags:</td>
    <td>
      <%= Hosts.joinHostFlags(host.getFlags()) %>
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
    <th>Cache Max Size</th>
    </tr>
      <%
     for (DomainAndVersion dgdv : domainGroup.getDomainVersionsSorted()) {
       Domain domain = dgdv.getDomain();
       RuntimeStatisticsAggregator runtimeStatisticsForDomain =
       Hosts.computeRuntimeStatisticsForDomain(runtimeStatistics, domain);
   %>
    <tr>
    <td><a href="/domain.jsp?n=<%= domain.getName() %>"><%= domain.getName() %></a></td>
    <td class='centered'><%= FormatUtils.formatDouble(runtimeStatisticsForDomain.getThroughput()) %>
    qps
    (<%= FormatUtils.formatDataThroughput(runtimeStatisticsForDomain.getResponseDataThroughput()) %>)
    </td>
    <td
    class='centered'><%= UiUtils.formatPopulationStatistics("Server-side latency for " + domain.getName() + " on " + host.getAddress(), runtimeStatisticsForDomain.getGetRequestsPopulationStatistics()) %>
    </td>
    <td class='centered'><%= FormatUtils.formatDouble(runtimeStatisticsForDomain.getHitRate() * 100) %>%</td>
    <td class='centered'><%= UiUtils.formatCacheHits(runtimeStatisticsForDomain) %></td>
    <td class='centered'>
      <%= String.format("%,d", runtimeStatisticsForDomain.getCacheStatistics().getNumItems())  %> items
      /
      <%= FormatUtils.formatNumBytes(runtimeStatisticsForDomain.getCacheStatistics().getNumManagedBytes()) %>
    </td>
    <td class='centered'>
      <%= String.format("%,d", runtimeStatisticsForDomain.getCacheStatistics().getMaxNumItems())  %> items
      /
      <%= FormatUtils.formatNumBytes(runtimeStatisticsForDomain.getCacheStatistics().getMaxNumManagedBytes()) %>
    </td>


    </tr>
      <%
    }
  %>
    </table>

    <h2>Enqueue Command</h2>

    <form action="/host/enqueue_command" method="post">
    <input type="hidden" name="g" value="<%= ringGroup.getName() %>"/>
    <input type="hidden" name="n" value="<%= ring.getRingNumber() %>"/>
    <input type="hidden" name="h" value="<%= host.getAddress() %>"/>

    <select name="command">
    <option></option>
      <% for (HostCommand cmd : HostCommand.values()) {%>
    <option><%= cmd.name() %></option>
      <% } %>
    </select>
    <input type="submit" value="Enqueue Command"/>
    </form>

    </div>



    <!-- Domains and Partitions -->

    <h2>Domains and Partitions</h2>

    <div style="border: 1px solid #ddd; width: 200px">
    <table style="font-size: 8px">
    <tr>
    <td colspan=2 align=center style="border-bottom: 1px solid #ddd">Legend</td>
    </tr>
    <tr>
    <td class="partition_unassigned" style="width:6px; height: 6px; font-size:0px">&nbsp;</td>
    <td>Not assigned</td>
    </tr>
    <tr>
    <td class="partition_undeployed" style="width:6px; height: 6px; font-size:0px">&nbsp;</td>
    <td>Assigned, no version deployed</td>
    </tr>
    <tr>
    <td class="partition_deletable" style="width:6px; height: 6px; font-size:0px">&nbsp;</td>
    <td>Assigned, deletable</td>
    </tr>
    <tr>
    <td class="partition_updating" style="width:6px; height: 6px; font-size:0px">&nbsp;</td>
    <td>Assigned, some version deployed, update pending</td>
    </tr>
    <tr>
    <td class="partition_updated" style="width:6px; height: 6px; font-size:0px">&nbsp;</td>
    <td>Assigned, target version deployed</td>
    </tr>
    </table>
    </div>
    <div class='PARTITIONS-STATE'>
      <%
    for (HostDomain hdc : host.getAssignedDomainsSorted()) {
      Domain domain = hdc.getDomain();
      DomainAndVersion targetDomainVersion = domainGroup.getDomainVersion(domain);
      int squareDim = (int)Math.floor(Math.sqrt(domain.getNumParts()));
      if (domain == null) {
  %>
    <div>Unknown Domain</div>
      <% } else { %>
    <div class="part_assignment_visualization">
    <div><%= domain.getName() %>
    <br/>
    (<%= hdc.getPartitions().size() %>/<%= domain.getNumParts() %>
    partitions)</div>
    <div>
    <table cellspacing=1 cellpadding=0>
      <%
      for (int i = 0; i < domain.getNumParts(); i++) {
        String className = "partition_unassigned";
        HostDomainPartition hdp = hdc.getPartitionByNumber(i);
        if (hdp != null) {
          if (hdp.isDeletable()) {
            className = "partition_deletable";
          } else if (hdp.getCurrentDomainVersion() == null) {
            className = "partition_undeployed";
          } else if (targetDomainVersion != null &&
                     hdp.getCurrentDomainVersion().equals(targetDomainVersion.getVersionNumber())) {
            className = "partition_updated";
          } else {
            className = "partition_updating";
          }
        }
      %>
      <% if (i % squareDim == 0) { %>
    <tr>
      <% } %>
    <td class="<%= className %>" style="font-size: 0px; width: 4px; height: 4px;" title="<%= i %>">&nbsp;</td>
      <% if (i % squareDim == squareDim - 1) { %>
    </tr>
      <% } %>
      <% } %>
    </table>
    </div>
    </div>
      <% } %>
      <% } %>
    </div>

    <div style="clear:both"></div>

    <jsp:include page="_footer.jsp"/>

    </body>
    </html>
