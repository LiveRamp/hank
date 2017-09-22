  <%@ page language="java" contentType="text/html; charset=ISO-8859-1"
           pageEncoding="ISO-8859-1" %>

    <%@page import="com.liveramp.hank.coordinator.*" %>
    <%@page import="com.liveramp.hank.partition_server.*" %>
    <%@page import="com.liveramp.hank.ring_group_conductor.*" %>
    <%@page import="com.liveramp.hank.generated.*" %>
    <%@page import="com.liveramp.hank.ui.*" %>
    <%@page import="com.liveramp.hank.util.*" %>
    <%@page import="com.liveramp.commons.util.*" %>
    <%@page import="java.util.*" %>
    <%@page import="java.net.*" %>
    <%@page import="java.nio.ByteBuffer" %>
    <%@page import="org.apache.thrift.*" %>
    <%@page import="org.apache.commons.lang.StringEscapeUtils" %>
    <%@page import="java.text.DecimalFormat" %>
    <%@page import="org.joda.time.format.DateTimeFormat" %>

      <%

Coordinator coord = (Coordinator)getServletContext().getAttribute("coordinator");

RingGroup ringGroup = coord.getRingGroup(request.getParameter("name"));

List<ClientMetadata> clients = ringGroup.getClients();

List<ConnectedServerMetadata> servers = ringGroup.getLiveServers();

// Sort clients by host name
Collections.sort(clients, new ClientMetadataComparator());

%>
    <!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">

    <%@page import="org.apache.commons.lang.NotImplementedException" %><html>
    <head>
    <meta http-equiv="Content-Type" content="text/html; charset=ISO-8859-1">
    <title>Ring Group <%=ringGroup.getName()%></title>

    <jsp:include page="_head.jsp"/>
    </head>
    <body>

    <script type="text/javascript">
    addAsyncReload(['ALL-RINGS']);
    addAsyncReload(['RING-GROUP-STATE']);
    addAsyncReload(['DOMAIN-STATISTICS']);
    addAsyncReload(['CLIENTS']);
    addAsyncReload(['CLIENTS-TITLE']);
    </script>

    <jsp:include page="_top_nav.jsp"/>

    <h1>
    Ring Group <span class='currentItem'><%=ringGroup.getName()%></span>
    </h1>

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

    <h2>State</h2>
    <table class='table-blue-compact RING-GROUP-STATE'>

    <tr>
    <td>Domain Group:</td>
    <td>
      <%= UiUtils.formatDomainGroupInfoTooltip(ringGroup.getDomainGroup(),
          "<a href='/domain_group.jsp?n=" + URLEnc.encode(ringGroup.getDomainGroup().getName()) +
          "'>" + ringGroup.getDomainGroup().getName() + "</a>") %>
    </td>
    </tr>

    <tr>
    <td>Ring Group Conductor:</td>
      <% if (ringGroup.isRingGroupConductorOnline()) { %>
      <% if (ringGroup.getRingGroupConductorMode() == RingGroupConductorMode.INACTIVE) { %>
    <td class='inactive centered'>INACTIVE</td>
      <% } else { %>
    <td class='complete centered'><%= ringGroup.getRingGroupConductorMode() %></td>
      <% } %>
      <% } else { %>
    <td class='error centered'>OFFLINE</td>
      <% } %>
    </tr>

    <tr>
    <td>Clients:</td>
    <td>
      <%= clients.size() %>
    </td>
    </tr>

    <tr>
    <td>Throughput:</td>
    <td>
      <%= FormatUtils.formatDouble(runtimeStatisticsForRingGroup.getThroughput()) %>
    qps
    (<%= FormatUtils.formatDataThroughput(runtimeStatisticsForRingGroup.getResponseDataThroughput()) %>)
    </td>
    </tr>

    <tr>
    <td>Latency:</td>
    <td>
      <%= UiUtils.formatPopulationStatistics("Server-side latency on " + ringGroup.getName(), runtimeStatisticsForRingGroup.getGetRequestsPopulationStatistics()) %>
    </td>
    </tr>

    <tr>
    <td>Hit Rate:</td>
    <td>
      <%= FormatUtils.formatDouble(runtimeStatisticsForRingGroup.getHitRate() * 100) %>%
    </td>
    </tr>

    <tr>
    <td>Cache Hit Rate:</td>
    <td>
      <%= UiUtils.formatCacheHits(runtimeStatisticsForRingGroup) %>
    </td>
    </tr>

    <tr>
    <td>Cache Size:</td>
    <td>
      <%= String.format("%,d", runtimeStatisticsForRingGroup.getCacheStatistics().getNumItems())  %> items
      /
      <%= FormatUtils.formatNumBytes(runtimeStatisticsForRingGroup.getCacheStatistics().getNumManagedBytes()) %>
    </td>
    </tr>

    <!-- Serving Status -->

      <%
        ServingStatusAggregator servingStatusAggregator = RingGroups.computeServingStatusAggregator(ringGroup, ringGroup.getDomainGroup());
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
      <%= UiUtils.formatFilesystemStatistics(filesystemStatisticsForRingGroup) %>
    <div class='progress-bar'>
    <div class='progress-bar-filler-used'
    style='width:<%= Math.round(filesystemStatisticsForRingGroup.getUsedPercentage()) %>%'></div>
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
    <th>Version</th>
    <th>Closed On</th>
    </tr>
      <%
         SortedMap<Domain, RuntimeStatisticsAggregator> runtimeStatisticsForDomains =
         RingGroups.computeRuntimeStatisticsForDomains(runtimeStatistics);

         SortedSet<Domain> relevantDomains = new TreeSet<Domain>();
         relevantDomains.addAll(runtimeStatisticsForDomains.keySet());
         for (DomainAndVersion dgdv : ringGroup.getDomainGroup().getDomainVersions()) {
           relevantDomains.add(dgdv.getDomain());
         }

         for (Domain domain : relevantDomains) {
           RuntimeStatisticsAggregator runtimeStatisticsForDomain = runtimeStatisticsForDomains.get(domain);
           DomainVersion targetDomainVersion = null;
           DomainAndVersion dgdv = ringGroup.getDomainGroup().getDomainVersion(domain);
           if (dgdv != null) {
             targetDomainVersion = domain.getVersion(dgdv.getVersionNumber());
           }
       %>
    <tr>
    <td><a href="/domain.jsp?n=<%= domain.getName() %>"><%= domain.getName() %></a></td>
      <% if (runtimeStatisticsForDomain != null) { %>
    <td class='centered'><%= FormatUtils.formatDouble(runtimeStatisticsForDomain.getThroughput()) %>
    qps
    (<%= FormatUtils.formatDataThroughput(runtimeStatisticsForDomain.getResponseDataThroughput()) %>)</td>
    <td
    class='centered'><%= UiUtils.formatPopulationStatistics("Server-side latency for " + domain.getName() + " on " + ringGroup.getName(), runtimeStatisticsForDomain.getGetRequestsPopulationStatistics()) %>
    </td>
    <td class='centered'><%= FormatUtils.formatDouble(runtimeStatisticsForDomain.getHitRate() * 100) %>%</td>
    <td class='centered'><%= UiUtils.formatCacheHits(runtimeStatisticsForDomain) %></td>
    <td class='centered'>
      <%= String.format("%,d", runtimeStatisticsForDomain.getCacheStatistics().getNumItems())  %> items
      /
      <%= FormatUtils.formatNumBytes(runtimeStatisticsForDomain.getCacheStatistics().getNumManagedBytes()) %>
    </td>
      <% } else { %>
    <td class='centered'>-</td>
    <td class='centered'>-</td>
    <td class='centered'>-</td>
    <td class='centered'>-</td>
    <td class='centered'>-</td>
      <% } %>
      <% if (targetDomainVersion != null) { %>
    <td class='centered'><%= targetDomainVersion != null ? targetDomainVersion.getVersionNumber() : "-" %></td>
    <td
    class='centered'><%= targetDomainVersion != null ? UiUtils.formatDomainVersionClosedAt(targetDomainVersion) : "-"  %>
    </td>
      <% } else { %>
    <td class='centered'>-</td>
    <td class='centered'>-</td>
      <% } %>
    </tr>
      <%
         }
       %>
    </table>

    <h2>Actions</h2>

    <table class='table-blue-compact'>

    <!-- Set Ring Group Conductor Mode form -->
    <tr>
    <td>Ring Group Conductor mode:</td>
    <td>
    <form action="/ring_group/set_ring_group_conductor_mode" method=post>
    <input type=hidden name="g" value="<%= ringGroup.getName() %>"/>
    <select name="mode">
    <option value=""></option>
    <option value="INACTIVE">
    INACTIVE: do nothing</option>
    <option value="ACTIVE">
    ACTIVE: conduct updates</option>
    <option value="PROACTIVE">
    PROACTIVE: conduct updates proactively</option>
    <option value="AUTOCONFIGURE">
    AUTOCONFIGURE: autoconfigure</option>
    </select>
    <input type="submit" value="Change mode"/>
    </form>
    </td>
    </tr>

    </table>

    <h2>Rings</h2>

    <table class='table-blue ALL-RINGS'>
    <tr>
    <th>Ring</th>
    <th></th>
    <th>Hosts</th>
    <th>Serving</th>
    <th>Updating</th>
    <th>Idle</th>
    <th>Offline</th>
    <th>Throughput</th>
    <th>Latency</th>
    <th>Hit Rate</th>
    <th>Cache Hits</th>
    <th>Up-to-date & Served</th>
    <th>File System</th>
    </tr>

      <%
      for (Ring ring : ringGroup.getRingsSorted()) {
    %>
    <tr>
    <td><a href="/ring.jsp?g=<%=URLEnc.encode(ringGroup.getName())%>&n=<%=ring.getRingNumber()%>
    ">Ring <%=ring.getRingNumber()%></a></td>
      <%
      UpdateProgressAggregator progress = null;
      long ringUpdateETA = Rings.computeUpdateETA(ring);
      if (!Rings.isUpToDate(ring, ringGroup.getDomainGroup())) {
        progress = Rings.computeUpdateProgress(ring, ringGroup.getDomainGroup());
      }
      %>

    <td><%= UiUtils.formatUpdateProgress(progress, ringUpdateETA) %></td>

      <%
        Set<Host> hostsAll = ring.getHosts();
        Set<Host> hostsServing = Rings.getHostsInState(ring, HostState.SERVING);
        Set<Host> hostsUpdating = Rings.getHostsInState(ring, HostState.UPDATING);
        Set<Host> hostsIdle = Rings.getHostsInState(ring, HostState.IDLE);
        Set<Host> hostsOffline = Rings.getHostsInState(ring, HostState.OFFLINE);
        %>

    <td class='host-total'>
      <%= UiUtils.formatHostListTooltip(ring, hostsAll) %>
    </td>
      <% if (hostsServing.size() != 0 && hostsServing.size() == hostsAll.size()) { %>
    <td class='host-serving'>
      <% } else if (hostsServing.size() != 0) { %>
    <td class='host-serving-incomplete'>
      <% } else { %>
    <td class='centered'>
      <% } %>
      <%= UiUtils.formatHostListTooltip(ring, hostsServing) %>
    </td>
      <% if (hostsUpdating.size() != 0) { %>
    <td class='host-updating'><%= UiUtils.formatHostListTooltip(ring, hostsUpdating) %></td>
      <% } else { %>
    <td class='centered'>-</td>
      <% } %>
      <% if (hostsIdle.size() != 0) { %>
    <td class='host-idle'><%= UiUtils.formatHostListTooltip(ring, hostsIdle) %></td>
      <% } else { %>
    <td class='centered'>-</td>
      <% } %>
      <% if (hostsOffline.size() != 0) { %>
    <td class='host-offline'><%= UiUtils.formatHostListTooltip(ring, hostsOffline) %></td>
      <% } else { %>
    <td class='centered'>-</td>
      <% } %>

    <!-- Runtime Statistics -->
      <%
        RuntimeStatisticsAggregator runtimeStatisticsForRing =
          RingGroups.computeRuntimeStatisticsForRing(runtimeStatistics, ring);

        FilesystemStatisticsAggregator filesystemStatisticsForRing =
          RingGroups.computeFilesystemStatisticsForRing(filesystemStatistics, ring);
      %>

    <td class='centered'><%= FormatUtils.formatDouble(runtimeStatisticsForRing.getThroughput()) %>
    qps
    (<%= FormatUtils.formatDataThroughput(runtimeStatisticsForRing.getResponseDataThroughput()) %>)</td>
    <td
    class='centered'><%= UiUtils.formatPopulationStatistics("Server-side latency on " + ringGroup.getName() + " Ring " + ring.getRingNumber(), runtimeStatisticsForRing.getGetRequestsPopulationStatistics()) %>
    </td>
    <td class='centered'><%= FormatUtils.formatDouble(runtimeStatisticsForRing.getHitRate() * 100) %>% </td>
    <td class='centered'><%= UiUtils.formatCacheHits(runtimeStatisticsForRing) %></td>

    <!-- Serving Status -->

      <%
        ServingStatusAggregator ringServingStatusAggregator = Rings.computeServingStatusAggregator(ring, ringGroup.getDomainGroup());
        ServingStatus ringServingStatus = ringServingStatusAggregator.computeServingStatus();
        %>
      <% if (ringServingStatus.getNumPartitionsServedAndUpToDate() != 0
                 && ringServingStatus.getNumPartitionsServedAndUpToDate() == ringServingStatus.getNumPartitions()) { %>
    <td class='centered complete'>
      <% } else { %>
    <td class='centered error'>
      <% } %>
      <%= ringServingStatus.getNumPartitionsServedAndUpToDate() %> / <%= ringServingStatus.getNumPartitions() %>
    </td>

    <!-- File system -->
    <td>
      <%= UiUtils.formatFilesystemStatistics(filesystemStatisticsForRing) %>
    <div class='progress-bar'>
    <div class='progress-bar-filler-used'
    style='width:<%= Math.round(filesystemStatisticsForRing.getUsedPercentage()) %>%'></div>
    </div>
    </td>

    </tr>
      <%
      }
    %>
    </table>

    <h2>Manual Query</h2>
    <form action="/ring_group.jsp" method=post>
    <input type=hidden name="name" value="<%=ringGroup.getName()%>"/>
    <table>
    <tr>
    <td>Domain:</td>
    <td>
    <select name="d">
    <option value=""></option>
      <%
            for (DomainAndVersion dgdv : ringGroup.getDomainGroup().getDomainVersionsSorted()) {
          %>
    <option<%= request.getParameter("d") != null && URLEnc.decode(request.getParameter("d")).equals(dgdv.getDomain().getName()) ? " selected" : "" %>
    >
      <%= dgdv.getDomain().getName() %>
    </option>
      <% } %>
    </select>
    </td>
    </tr>
    <tr>
    <td>Key format:</td>
    <td>
    <select name="f">
    <option<%= request.getParameter("f") != null && request.getParameter("f").equals("string") ? " selected" : "" %>
    >string</option>
    <option<%= request.getParameter("f") != null && request.getParameter("f").equals("hex") ? " selected" : "" %>
    >hex</option>
    </select>
    </td>
    </tr>
    <tr>
    <td>Key:</td>
    <td>
    <textarea rows=2 cols=30 name="k"><%= request.getParameter("k") == null ? "" : request.getParameter("k") %>
    </textarea>
    </td>
    </tr>
    </table>
    <input type=submit value="Perform Manual Query"/>

      <%
        if (request.getParameter("k") != null) {
          %>
    <h4>Result</h4>
      <%
          SmartClient.Iface client = ((IClientCache)getServletContext().getAttribute("clientCache")).getSmartClient(ringGroup);

          ByteBuffer key = null;
          String dataFormat = request.getParameter("f");
          if (dataFormat.equals("hex")) {
            key = BytesUtils.hexStringToBytes(request.getParameter("k"));
          } else if (dataFormat.equals("string")) {
            key = ByteBuffer.wrap(request.getParameter("k").getBytes("UTF-8"));
          }
          HankResponse hankResponse = client.get(URLEnc.decode(request.getParameter("d")), key);
          if (hankResponse.is_set_xception()) {
            // uh oh!
          %>
    <div style="color:red; font-weight: bold">
      <%= StringEscapeUtils.escapeHtml(hankResponse.toString()) %>
    </div>
      <%
          } else if (hankResponse.isSet(HankResponse._Fields.NOT_FOUND)) {
            %>
    <div style="color:red; font-weight: bold">
    Key was not found!
    </div>
      <%
          } else {
          %>
    <div style="font-weight:bold; color:green">Found</div>

      <%
          ByteBuffer valueBuffer = hankResponse.buffer_for_value();
          String valueString = BytesUtils.bytesToHexString(valueBuffer);
          String[] valueStrings = valueString.split(" ");
          %>

    <table cellspacing=0>
    <tr>
    <td>off</td>
    <td colspan=16 align=center style="border-bottom: 1px solid black"><strong>bytes</strong></td>
    <td width=5 style="border-bottom: 1px solid black">&nbsp;</td>
    <td colspan=16 align=center style="border-bottom: 1px solid black"><strong>string</strong></td>
    </tr>
      <%
          int numBytes = valueStrings.length;
          for (int r = 0; r < (numBytes / 16) + (numBytes % 16 == 0 ? 0 : 1); r++) {
          %>
    <tr>
      <%
             int baseOff = r * 16;
             %>
    <td style="border-right: 1px solid black">0x<%= Integer.toString(baseOff, 16) %></td>
      <%
             for (int off = baseOff; off < baseOff + 16; off++) {
               if (off < numBytes) {
               %><td width=20 align=center><%= valueStrings[off] %></td><%
               } else {
               %>
    <td width=20>&nbsp;</td>
      <%
               }
             }
             %>
    <td style="border-left: 1px solid black; border-right: 1px solid black"></td>
      <%
             for (int off = baseOff; off < baseOff + 16; off++) {
               if (off < numBytes) {
               %><td width=20
    align=center><%= new String(valueBuffer.array(), valueBuffer.position() + off, 1).replaceAll("\\p{Cntrl}", ".") %>
    </td><%
               } else {
               %>
    <td width=20>&nbsp;</td>
      <%
               }
             }
             %>
    </tr>
      <% } %>
    </table>
    <div style="padding-top:1em;">Raw value in hex:
      <%= valueString %>
    </div>

      <% }} %>
    </form>

    <h2 class='HOSTS-TITLE'><%= servers.size() %>
    live servers</h2>

    <table class='table-blue SERVERS'>
      <tr>
        <th>Host</th>
        <th>Uptime</th>
        <th>Connected</th>
        <th>Environment Flags</th>
      </tr>
      <% for (ConnectedServerMetadata server : servers) { %>
      <tr>
        <td><%= server.get_host() %></td>
        <td>
          <%= FormatUtils.formatSecondsDuration((System.currentTimeMillis() - server.get_connected_at()) / 1000) %>
        </td>
        <td>
          <%= DateTimeFormat.forStyle("SS").print(server.get_connected_at()) %>
        </td>
        <td><%= server.get_environment_flags() %></td>
      </tr>
      <% } %>
    </table>

    <h2 class='CLIENTS-TITLE'><%= clients.size() %>
    clients</h2>

    <table class='table-blue CLIENTS'>
    <tr>
    <th>Host</th>
    <th>Uptime</th>
    <th>Connected</th>
    <th>Type</th>
    <th>Version</th>
    </tr>
      <% for (ClientMetadata client : clients) { %>
    <tr>
    <td><%= client.get_host() %></td>
    <td>
      <%= FormatUtils.formatSecondsDuration((System.currentTimeMillis() - client.get_connected_at()) / 1000) %>
    </td>
    <td>
      <%= DateTimeFormat.forStyle("SS").print(client.get_connected_at()) %>
    </td>
    <td><%= client.get_type() %></td>
    <td><%= client.get_version() %></td>
    </tr>
      <% } %>
    </table>

    <jsp:include page="_footer.jsp"/>

    </body>
    </html>
