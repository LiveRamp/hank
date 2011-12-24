<%@ page language="java" contentType="text/html; charset=ISO-8859-1"
    pageEncoding="ISO-8859-1"%>

<%@page import="com.rapleaf.hank.ui.*"%>
<%@page import="com.rapleaf.hank.coordinator.*"%>
<%@page import="com.rapleaf.hank.partition_server.*"%>
<%@page import="java.util.*"%>
<%@page import="java.net.*"%>
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
  <title>Ring <%=ring.getRingNumber()%> in group <%=ringGroup.getName()%></title>

  <jsp:include page="_head.jsp" />
</head>
<body>

  <jsp:include page="_top_nav.jsp" />

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

    DomainGroupVersion currentDomainGroupVersion = ring.getCurrentVersion();
  %>

  <h2>State</h2>
    <table class='table-blue-compact'>
      <tr>
      <td>State:</td>
      <td>
      <%=ring.getState()%>
      </td>
      </tr>

      <tr>
      <td>Current version:</td>
      <td>
      <%= ring.getCurrentVersionNumber() %>
      </td>
      </tr>

      <tr>
      <td>Update Status:</td>
      <td>
      <% if (Rings.isUpdatePending(ring)) { %>
      Updating from <%=ring.getCurrentVersionNumber()%> to <%=ring.getUpdatingToVersionNumber()%>
      <% } else { %>
      Not updating
      <% } %>
      </td>
      </tr>

      <tr>
      <td>Throughput:</td>
      <td>
      <%= new DecimalFormat("#.##").format(runtimeStatisticsForRing.getThroughput()) %> qps
      </td>
      </tr>

      <tr>
      <td>Latency:</td>
      <td>
      <%= UiUtils.formatPopulationStatistics("Latency on " + ringGroup.getName() + " Ring " + ring.getRingNumber(), runtimeStatisticsForRing.getGetRequestsPopulationStatistics()) %>
      </td>
      </tr>

      <tr>
      <td>Hit Rate:</td>
      <td>
      <%= new DecimalFormat("#.##").format(runtimeStatisticsForRing.getHitRate() * 100) %>%
      </td>
      </tr>

      <tr>
      <td>Partition assignment:</td>
      <td><a href="/ring_partitions.jsp?g=<%=URLEnc.encode(ringGroup.getName())%>&n=<%=ring.getRingNumber()%>">manage</a></td>
      </tr>

        <!-- Serving Status -->

        <%
        ServingStatusAggregator servingStatusAggregator = null;
        ServingStatus servingStatus = null;
        ServingStatus uniquePartitionsServingStatus = null;
        DomainGroupVersion mostRecentDomainGroupVersion = Rings.getMostRecentVersion(ring);
        if (mostRecentDomainGroupVersion != null) {
          servingStatusAggregator = Rings.computeServingStatusAggregator(ring, mostRecentDomainGroupVersion);
          servingStatus = servingStatusAggregator.computeServingStatus();
          uniquePartitionsServingStatus = servingStatusAggregator.computeUniquePartitionsServingStatus(mostRecentDomainGroupVersion);
        }
        %>
        <% if (servingStatusAggregator != null) { %>
        <tr>
        <td>Updated & Served</td>
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
        <td>Updated & Served (fully)</td>
          <% if (uniquePartitionsServingStatus.getNumPartitionsServedAndUpToDate() != 0
                 && uniquePartitionsServingStatus.getNumPartitionsServedAndUpToDate() == uniquePartitionsServingStatus.getNumPartitions()) { %>
            <td class='centered complete'>
          <% } else { %>
            <td class='centered error'>
          <% } %>
          <%= uniquePartitionsServingStatus.getNumPartitionsServedAndUpToDate() %> / <%= uniquePartitionsServingStatus.getNumPartitions() %>
          </td>
        </tr>
        <% } %>

    </table>

  <!-- Domain specific Runtime Statistics -->

  <%
    if (currentDomainGroupVersion != null) {
  %>
  <table class='table-blue-compact'>
  <tr>
     <th>Domain</th>
     <th>Throughput</th>
     <th>Latency</th>
     <th>Hit Rate</th>
  </tr>
   <%
     SortedMap<Domain, RuntimeStatisticsAggregator> runtimeStatisticsForDomains = Rings.computeRuntimeStatisticsForDomains(runtimeStatistics);
     for (SortedMap.Entry<Domain, RuntimeStatisticsAggregator> entry : runtimeStatisticsForDomains.entrySet()) {
       Domain domain = entry.getKey();
       RuntimeStatisticsAggregator runtimeStatisticsForDomain = entry.getValue();
   %>
    <tr>
      <td class='centered'><a href="/domain.jsp?n=<%= domain.getName() %>"><%= domain.getName() %></a></td>
      <td class='centered'><%= new DecimalFormat("#.##").format(runtimeStatisticsForDomain.getThroughput()) %> qps</td>
      <td class='centered'><%= UiUtils.formatPopulationStatistics("Latency for " + domain.getName() + " on " + ringGroup.getName() + " Ring " + ring.getRingNumber(), runtimeStatisticsForDomain.getGetRequestsPopulationStatistics()) %></td>
      <td class='centered'><%= new DecimalFormat("#.##").format(runtimeStatisticsForDomain.getHitRate() * 100) %>%</td>
    </tr>
  <%
    }
  %>
  <%
  }
  %>
  </table>

  <h2>Add New Host</h2>

  <form action="/ring/add_host" method=post>
    <input type=hidden name="rgName" value="<%=ringGroup.getName()%>"/>
    <input type=hidden name="ringNum" value="<%=ring.getRingNumber()%>"/>
    <table>
      <tr>
        <td>Host:</td>
        <td><input type=text size=30 name="hostname"/></td>
      </tr>
      <tr>
        <td>Port:</td>
        <td><input type=text size=5 name="port" /></td>
      </tr>
    </table>
    <input type=submit value="Add"/>
  </form>

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

  <table width=800 class='table-blue'>
    <tr>
      <th>Host Address</th>
      <th>State</th>
      <th></th>
      <th></th>
      <th>Current Command</th>
      <th>Command Queue</th>
      <th>Throughput</th>
      <th>Latency</th>
      <th>Hit Rate</th>
      <th>Updated & Served</th>
      <th>Actions</th>
    </tr>
    <%
      Collection<Host> hosts = ring.getHostsSorted();
      if (hosts != null) {
        for(Host host : hosts) {
    %>
    <tr>
      <td><a href="/host.jsp?g=<%= ringGroup.getName() %>&r=<%= ring.getRingNumber() %>&h=<%= URLEnc.encode(host.getAddress().toString()) %>"><%= host.getAddress() %></a></td>
      <td class='<%= UiUtils.hostStateToClass(host.getState()) %>'>
      <%= host.getState() %>
      </td>
      <%
      UpdateProgress progress = null;
      if (Rings.isUpdatePending(ring)) {
        DomainGroupVersion domainGroupVersion = ringGroup.getUpdatingToVersion();
        if (domainGroupVersion != null) {
          progress = Hosts.computeUpdateProgress(host, domainGroupVersion);
        }
      }
      %>
      <% if (progress != null) { %>
      <td>
        <div class='progress-bar'>
          <div class='progress-bar-filler' style='width: <%= Math.round(progress.getUpdateProgress() * 100) %>%'></div>
        </div>
      </td>
      <td>
        <%= new DecimalFormat("#.##").format(progress.getUpdateProgress() * 100) %>% partitions updated
        (<%= progress.getNumPartitionsUpToDate() %>/<%= progress.getNumPartitions() %>)
      </td>
      <% } else { %>
      <td></td>
      <td></td>
      <% } %>
      <td class='centered'><%= host.getCurrentCommand() %></td>
      <td><%= host.getCommandQueue() %></td>

      <!-- Runtime Statistics -->
      <%
        RuntimeStatisticsAggregator runtimeStatisticsForHost =
          Rings.computeRuntimeStatisticsForHost(runtimeStatistics, host);
      %>

      <td class='centered'> <%= new DecimalFormat("#.##").format(runtimeStatisticsForHost.getThroughput()) %> qps </td>
      <td class='centered'><%= UiUtils.formatPopulationStatistics("Latency on " + host.getAddress(), runtimeStatisticsForHost.getGetRequestsPopulationStatistics()) %></td>
      <td class='centered'> <%= new DecimalFormat("#.##").format(runtimeStatisticsForHost.getHitRate() * 100) %>% </td>

        <!-- Serving Status -->

        <%
        ServingStatusAggregator hostServingStatusAggregator = null;
        ServingStatus hostServingStatus = null;
        DomainGroupVersion ringMostRecentDomainGroupVersion = Rings.getMostRecentVersion(ring);
        if (ringMostRecentDomainGroupVersion != null) {
          hostServingStatusAggregator = Hosts.computeServingStatusAggregator(host, ringMostRecentDomainGroupVersion);
          hostServingStatus = hostServingStatusAggregator.computeServingStatus();
        }
        %>
        <% if (hostServingStatusAggregator != null) { %>
          <% if (hostServingStatus.getNumPartitionsServedAndUpToDate() != 0
                 && hostServingStatus.getNumPartitionsServedAndUpToDate() == hostServingStatus.getNumPartitions()) { %>
            <td class='centered complete'>
          <% } else { %>
            <td class='centered error'>
          <% } %>
          <%= hostServingStatus.getNumPartitionsServedAndUpToDate() %> / <%= hostServingStatus.getNumPartitions() %>
          </td>
        <% } else { %>
          <td></td>
        <% } %>

      <!-- Actions -->

      <td>
        <form method=post action="/ring/delete_host" id="remove_form_<%= host.getAddress().getHostName() %>__<%= host.getAddress().getPortNumber() %>">
          <input type=hidden name="g" value="<%= ringGroup.getName() %>"/>
          <input type=hidden name="n" value="<%= ring.getRingNumber() %>"/>
          <input type=hidden name="h" value="<%= host.getAddress() %>"/>
          <a href="javascript:if (confirm('Are you sure you want to delete this host? This action cannot be undone!')) document.forms['remove_form_<%= host.getAddress().getHostName() %>__<%= host.getAddress().getPortNumber() %>'].submit();">remove from ring</a>
        </form>
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
