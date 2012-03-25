<%@ page language="java" contentType="text/html; charset=ISO-8859-1"
    pageEncoding="ISO-8859-1"%>

<%@page import="com.rapleaf.hank.coordinator.*"%>
<%@page import="com.rapleaf.hank.partition_server.*"%>
<%@page import="com.rapleaf.hank.ring_group_conductor.*"%>
<%@page import="com.rapleaf.hank.generated.*"%>
<%@page import="com.rapleaf.hank.ui.*"%>
<%@page import="com.rapleaf.hank.util.*"%>
<%@page import="java.util.*"%>
<%@page import="java.net.*"%>
<%@page import="java.nio.ByteBuffer"%>
<%@page import="org.apache.thrift.*"%>
<%@page import="org.apache.commons.lang.StringEscapeUtils"%>
<%@page import="java.text.DecimalFormat" %>

<%

Coordinator coord = (Coordinator)getServletContext().getAttribute("coordinator");

RingGroup ringGroup = coord.getRingGroup(request.getParameter("name"));

%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">

<%@page import="org.apache.commons.lang.NotImplementedException"%><html>
<head>
  <meta http-equiv="Content-Type" content="text/html; charset=ISO-8859-1">
  <title>Ring Group <%=ringGroup.getName()%></title>

  <jsp:include page="_head.jsp" />
</head>
<body>

  <script type="text/javascript">
    addAsyncReload(['ALL-RINGS']);
    addAsyncReload(['RING-GROUP-STATE']);
    addAsyncReload(['DOMAIN-STATISTICS']);
  </script>

  <jsp:include page="_top_nav.jsp" />

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

    DomainGroupVersion targetDomainGroupVersion = ringGroup.getTargetVersion();
  %>

    <h2>State</h2>
      <table class='table-blue-compact RING-GROUP-STATE'>

        <tr>
        <td>Domain Group:</td>
        <td>
        <a href="/domain_group.jsp?n=<%=URLEnc.encode(ringGroup.getDomainGroup().getName())%>"><%=ringGroup.getDomainGroup().getName()%></a>
        </td>
        </tr>

        <tr>
        <td>Target Version:</td>
        <td>
        Version
        <% if (targetDomainGroupVersion != null) { %>
          <%= targetDomainGroupVersion.getVersionNumber() %>
          (<%= UiUtils.formatDomainGroupVersionCreatedAt(targetDomainGroupVersion) %>)
        <% } else { %>
          unspecified
        <% } %>
        </td>
        </tr>

        <tr>
        <td>Ring Group Conductor:</td>
        <% if (ringGroup.isRingGroupConductorOnline()) { %>
          <% if (ringGroup.getRingGroupConductorMode() == RingGroupConductorMode.INACTIVE) { %>
            <td class='inactive centered'>INACTIVE</td>
          <% } else if (ringGroup.getRingGroupConductorMode() == RingGroupConductorMode.ACTIVE) { %>
            <td class='complete centered'>ACTIVE</td>
          <% } else if (ringGroup.getRingGroupConductorMode() == RingGroupConductorMode.PROACTIVE) { %>
            <td class='complete centered'>PROACTIVE</td>
          <% } else { %>
            <td>unknown</td>
          <% } %>
        <% } else { %>
          <td class='error centered'>OFFLINE</td>
        <% } %>
        </tr>

        <tr>
        <td>Throughput:</td>
        <td>
        <%= UiUtils.formatDouble(runtimeStatisticsForRingGroup.getThroughput()) %> qps
        (<%= UiUtils.formatDataThroughput(runtimeStatisticsForRingGroup.getResponseDataThroughput()) %>)
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
        <%= UiUtils.formatDouble(runtimeStatisticsForRingGroup.getHitRate() * 100) %>%
        </td>
        </tr>

        <tr>
        <td>Cache Hit Rate:</td>
        <td>
        <%= UiUtils.formatCacheHits(runtimeStatisticsForRingGroup) %>
        </td>
        </tr>

        <!-- Serving Status -->

        <%
        ServingStatusAggregator servingStatusAggregator = null;
        ServingStatus servingStatus = null;
        if (targetDomainGroupVersion != null) {
          servingStatusAggregator = RingGroups.computeServingStatusAggregator(ringGroup, targetDomainGroupVersion);
          servingStatus = servingStatusAggregator.computeServingStatus();
        }
        %>

        <% if (servingStatusAggregator != null) { %>
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
        <% } %>

        <tr>
        <td>File System</td>
        <td>
          <%= UiUtils.formatFilesystemStatistics(filesystemStatisticsForRingGroup) %>
          <div class='progress-bar'>
            <div class='progress-bar-filler-used' style='width: <%= Math.round(filesystemStatisticsForRingGroup.getUsedPercentage()) %>%'></div>
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
         <th>Version</th>
         <th>Closed On</th>
       </tr>
       <%
         SortedMap<Domain, RuntimeStatisticsAggregator> runtimeStatisticsForDomains =
         RingGroups.computeRuntimeStatisticsForDomains(runtimeStatistics);

         SortedSet<Domain> relevantDomains = new TreeSet<Domain>();
         relevantDomains.addAll(runtimeStatisticsForDomains.keySet());
         if (targetDomainGroupVersion != null) {
           for (DomainGroupVersionDomainVersion dgvdv : targetDomainGroupVersion.getDomainVersions()) {
           relevantDomains.add(dgvdv.getDomain());
           }
         }

         for (Domain domain : relevantDomains) {
           RuntimeStatisticsAggregator runtimeStatisticsForDomain = runtimeStatisticsForDomains.get(domain);
           DomainVersion targetDomainVersion = null;
           if (targetDomainGroupVersion != null) {
             DomainGroupVersionDomainVersion dgvdv = targetDomainGroupVersion.getDomainVersion(domain);
             if (dgvdv != null) {
               targetDomainVersion = domain.getVersionByNumber(dgvdv.getVersion());
             }
           }
       %>
         <tr>
           <td><a href="/domain.jsp?n=<%= domain.getName() %>"><%= domain.getName() %></a></td>
           <% if (runtimeStatisticsForDomain != null) { %>
             <td class='centered'><%= UiUtils.formatDouble(runtimeStatisticsForDomain.getThroughput()) %> qps
             (<%= UiUtils.formatDataThroughput(runtimeStatisticsForDomain.getResponseDataThroughput()) %>)</td>
             <td class='centered'><%= UiUtils.formatPopulationStatistics("Server-side latency for " + domain.getName() + " on " + ringGroup.getName(), runtimeStatisticsForDomain.getGetRequestsPopulationStatistics()) %></td>
             <td class='centered'><%= UiUtils.formatDouble(runtimeStatisticsForDomain.getHitRate() * 100) %>%</td>
             <td class='centered'><%= UiUtils.formatCacheHits(runtimeStatisticsForDomain) %></td>
           <% } else { %>
             <td class='centered'>-</td>
             <td class='centered'>-</td>
             <td class='centered'>-</td>
             <td class='centered'>-</td>
           <% } %>
           <% if (targetDomainVersion != null) { %>
             <td class='centered'><%= targetDomainVersion != null ? targetDomainVersion.getVersionNumber() : "-" %></td>
             <td class='centered'><%= targetDomainVersion != null ? UiUtils.formatDomainVersionClosedAt(targetDomainVersion) : "-"  %></td>
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
          ACTIVE: use target version</option>
          <option value="PROACTIVE">
          PROACTIVE: use most recent version</option>
        </select>
      <input type="submit" value="Change mode"/>
      </form>
    </td>
    </tr>

    <!-- Set Target Version form -->
    <% if (ringGroup.getRingGroupConductorMode() != RingGroupConductorMode.PROACTIVE) { %>
    <tr>
    <td>Target Version:</td>
    <td>
    <form action="/ring_group/set_target_version" method=post>
      <input type=hidden name="g" value="<%= ringGroup.getName() %>"/>
      <select name="version">
        <option value=""></option>
        <%
        SortedSet<DomainGroupVersion> dgvRev = new TreeSet<DomainGroupVersion>(new ReverseComparator<DomainGroupVersion>());
        dgvRev.addAll(ringGroup.getDomainGroup().getVersions());
        for (DomainGroupVersion domainGroupVersion : dgvRev) { %>
        <option value="<%= domainGroupVersion.getVersionNumber() %>">
          <%= domainGroupVersion.getVersionNumber() %>
          (<%= UiUtils.formatDomainGroupVersionCreatedAt(domainGroupVersion) %>)
        </option>
        <% } %>
      </select>
      <input type="submit" value="Change target"/>
    </form>
    </td>
    </tr>
    <% } %>

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
      <td><a href="/ring.jsp?g=<%=URLEnc.encode(ringGroup.getName())%>&n=<%=ring.getRingNumber()%>">Ring <%=ring.getRingNumber()%></a></td>
      <%
      UpdateProgress progress = null;
      long ringUpdateETA = Rings.computeUpdateETA(ring);
      if (targetDomainGroupVersion != null &&
          !Rings.isUpToDate(ring, targetDomainGroupVersion)) {
        progress = Rings.computeUpdateProgress(ring, targetDomainGroupVersion);
      }
      %>

        <% if (progress != null) { %>
        <td>
          <%= UiUtils.formatDouble(progress.getUpdateProgress() * 100) %>% up-to-date
          (<%= progress.getNumPartitionsUpToDate() %>/<%= progress.getNumPartitions() %>)
          <% if (ringUpdateETA >= 0) { %>
            ETA: <%= UiUtils.formatSecondsDuration(ringUpdateETA) %>
          <% } %>
          <div class='progress-bar'>
            <div class='progress-bar-filler' style='width: <%= Math.round(progress.getUpdateProgress() * 100) %>%'></div>
          </div>
        </td>
        <% } else { %>
        <td></td>
        <% } %>

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

      <td class='centered'><%= UiUtils.formatDouble(runtimeStatisticsForRing.getThroughput()) %> qps
      (<%= UiUtils.formatDataThroughput(runtimeStatisticsForRing.getResponseDataThroughput()) %>)</td>
      <td class='centered'><%= UiUtils.formatPopulationStatistics("Server-side latency on " + ringGroup.getName() + " Ring " + ring.getRingNumber(), runtimeStatisticsForRing.getGetRequestsPopulationStatistics()) %></td>
      <td class='centered'><%= UiUtils.formatDouble(runtimeStatisticsForRing.getHitRate() * 100) %>% </td>
      <td class='centered'><%= UiUtils.formatCacheHits(runtimeStatisticsForRing) %></td>

        <!-- Serving Status -->

        <%
        ServingStatusAggregator ringServingStatusAggregator = null;
        ServingStatus ringServingStatus = null;
        if (targetDomainGroupVersion != null) {
          ringServingStatusAggregator = Rings.computeServingStatusAggregator(ring, targetDomainGroupVersion);
          ringServingStatus = ringServingStatusAggregator.computeServingStatus();
        }
        %>
        <% if (ringServingStatusAggregator != null) { %>
          <% if (ringServingStatus.getNumPartitionsServedAndUpToDate() != 0
                 && ringServingStatus.getNumPartitionsServedAndUpToDate() == ringServingStatus.getNumPartitions()) { %>
            <td class='centered complete'>
          <% } else { %>
            <td class='centered error'>
          <% } %>
          <%= ringServingStatus.getNumPartitionsServedAndUpToDate() %> / <%= ringServingStatus.getNumPartitions() %>
          </td>
        <% } else { %>
          <td></td>
        <% } %>

      <!-- File system -->
      <td>
      <%= UiUtils.formatFilesystemStatistics(filesystemStatisticsForRing) %>
      <div class='progress-bar'>
        <div class='progress-bar-filler-used' style='width: <%= Math.round(filesystemStatisticsForRing.getUsedPercentage()) %>%'></div>
      </div>
      </td>

    </tr>
    <%
      }
    %>
  </table>

    <h2>Manual Query</h2>
    <% if (ringGroup.getTargetVersion() == null) { %>
      Query disabled because target version is empty.
    <% } else { %>
      <form action="/ring_group.jsp" method=post>
        <input type=hidden name="name" value="<%=ringGroup.getName()%>"/>
        <table>
        <tr>
        <td>Domain:</td>
        <td>
        <select name="d">
          <option value=""></option>
          <%
            for (DomainGroupVersionDomainVersion dgvdv : targetDomainGroupVersion.getDomainVersionsSorted()) {
          %>
          <option<%= request.getParameter("d") != null && URLEnc.decode(request.getParameter("d")).equals(dgvdv.getDomain().getName()) ? " selected" : "" %>>
            <%= dgvdv.getDomain().getName() %>
          </option>
          <% } %>
        </select>
        </td>
        </tr>
        <tr>
        <td>Key format:</td>
        <td>
        <select name="f">
          <option<%= request.getParameter("f") != null && request.getParameter("f").equals("string") ? " selected" : "" %>>string</option>
          <option<%= request.getParameter("f") != null && request.getParameter("f").equals("hex") ? " selected" : "" %>>hex</option>
        </select>
        </td>
        </tr>
        <tr>
        <td>Key:</td>
        <td>
        <textarea rows=2 cols=30 name="k"><%= request.getParameter("k") == null ? "" : request.getParameter("k") %></textarea>
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
            key = Bytes.hexStringToBytes(request.getParameter("k"));
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
          String valueString = Bytes.bytesToHexString(valueBuffer);
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
               %><td width=20 align=center><%= valueStrings[off] %></td> <%
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
               %><td width=20 align=center><%= new String(valueBuffer.array(), valueBuffer.position() + off, 1).replaceAll("\\p{Cntrl}", ".") %></td> <%
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

        <% }}} %>
      </form>

<jsp:include page="_footer.jsp"/>

</body>
</html>
