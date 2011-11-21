<%@ page language="java" contentType="text/html; charset=ISO-8859-1"
    pageEncoding="ISO-8859-1"%>

<%@page import="com.rapleaf.hank.coordinator.*"%>
<%@page import="com.rapleaf.hank.partition_server.*"%>
<%@page import="com.rapleaf.hank.ring_group_conductor.*"%>
<%@page import="com.rapleaf.hank.generated.*"%>
<%@page import="com.rapleaf.hank.ui.*"%>
<%@page import="com.rapleaf.hank.util.Bytes"%>
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
  <jsp:include page="_top_nav.jsp" />

  <h1>
  Ring Group <span class='currentItem'><%=ringGroup.getName()%></span>
  </h1>

  <%
    Map<Ring, Map<Host, Map<Domain, RuntimeStatisticsAggregator>>> runtimeStatistics =
      RingGroups.computeRuntimeStatistics(ringGroup);

    RuntimeStatisticsAggregator runtimeStatisticsForRingGroup =
      RingGroups.computeRuntimeStatisticsForRingGroup(runtimeStatistics);

    DomainGroupVersion currentDomainGroupVersion = ringGroup.getCurrentVersion();
  %>

    <h2>State</h2>
      <table class='table-blue-compact'>
        <tr>
        <td>Domain Group:</td>
        <td>
        <a href="/domain_group.jsp?n=<%=URLEnc.encode(ringGroup.getDomainGroup().getName())%>"><%=ringGroup.getDomainGroup().getName()%></a>
        </td>
        </tr>

        <tr>
        <td>Update status:</td>
        <td>
        <% if(RingGroups.isUpdating(ringGroup)) { %>
        Updating from <%=ringGroup.getCurrentVersionNumber()%> to <%=ringGroup.getUpdatingToVersionNumber()%>
        <% } else { %>
        Not updating
        <% } %>
        </td>
        </tr>

        <tr>
        <td>Ring Group Conductor:</td>
        <% if (ringGroup.isRingGroupConductorOnline()) { %>
          <% if (ringGroup.getRingGroupConductorMode() == RingGroupConductorMode.ACTIVE) { %>
            <td class='complete centered'>ACTIVE</td>
          <% } else if (ringGroup.getRingGroupConductorMode() == RingGroupConductorMode.INACTIVE) { %>
            <td class='inactive centered'>INACTIVE</td>
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
        <%= new DecimalFormat("#.##").format(runtimeStatisticsForRingGroup.getThroughput()) %> qps
        </td>
        </tr>

        <tr>
        <td>Hit Rate:</td>
        <td>
        <%= new DecimalFormat("#.##").format(runtimeStatisticsForRingGroup.getHitRate() * 100) %>%
        </td>
        </tr>

        <!-- Serving Status -->

        <%
        ServingStatusAggregator servingStatusAggregator = null;
        ServingStatus servingStatus = null;
        ServingStatus uniquePartitionsServingStatus = null;
        DomainGroupVersion mostRecentDomainGroupVersion = RingGroups.getMostRecentVersion(ringGroup);
        if (mostRecentDomainGroupVersion != null) {
          servingStatusAggregator = RingGroups.computeServingStatusAggregator(ringGroup, mostRecentDomainGroupVersion);
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
        <td>Updated & Served (uniques)</td>
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
         <th>Hit Rate</th>
       </tr>
       <%
         SortedMap<Domain, RuntimeStatisticsAggregator> runtimeStatisticsForDomains = RingGroups.computeRuntimeStatisticsForDomains(runtimeStatistics);
         for (SortedMap.Entry<Domain, RuntimeStatisticsAggregator> entry : runtimeStatisticsForDomains.entrySet()) {
           Domain domain = entry.getKey();
           RuntimeStatisticsAggregator runtimeStatisticsForDomain = entry.getValue();
       %>
         <tr>
           <td class='centered'><a href="/domain.jsp?n=<%= domain.getName() %>"><%= domain.getName() %></a></td>
           <td class='centered'><%= new DecimalFormat("#.##").format(runtimeStatisticsForDomain.getThroughput()) %> qps</td>
           <td class='centered'><%= new DecimalFormat("#.##").format(runtimeStatisticsForDomain.getHitRate() * 100) %>%</td>
         </tr>
       <%
         }
       %>
       <%
       }
       %>
      </table>

    <h2>Actions</h2>

    <!-- Set Ring Group Conductor Mode form -->
    <form action="/ring_group/set_ring_group_conductor_mode" method=post>
    <input type=hidden name="g" value="<%= ringGroup.getName() %>"/>
    <% if (ringGroup.isRingGroupConductorOnline()) { %>
      <% if (ringGroup.getRingGroupConductorMode() == RingGroupConductorMode.ACTIVE) { %>
    <input type=hidden name="mode" value="INACTIVE"/>
    <input type=submit value="Deactivate Ring Group Conductor"/>
      <% } else { %>
    <input type=hidden name="mode" value="ACTIVE"/>
    <input type=submit value="Activate Ring Group Conductor"/>
      <% } %>
    <% } %>
    </form>

    <!-- Delete Ring Group form -->
    <form action="/ring_group/delete_ring_group" method=post>
    <input type=hidden name="g" value="<%= ringGroup.getName() %>"/>
    <input type=submit value="Delete Ring Group"
    onclick="return confirm('Are you sure you want to delete the ring group <%= ringGroup.getName() %>? This action cannot be undone.');"/>
    </form>

  <h2>Rings</h2>
  <a href="/ring_group/add_ring?g=<%=URLEnc.encode(ringGroup.getName())%>">Add a new ring</a>
  <table class='table-blue'>
    <tr>
      <th>Ring</th>
      <th>State</th>
      <th></th>
      <th></th>
      <th>Version</th>
      <th>Updating to version</th>
      <th>Hosts</th>
      <th>Serving</th>
      <th>Updating</th>
      <th>Idle</th>
      <th>Offline</th>
      <th>Throughput</th>
      <th>Hit Rate</th>
      <th>Updated & Served</th>
      <th>(uniques)</th>
      <th></th>
    </tr>

    <%
      for (Ring ring : ringGroup.getRingsSorted()) {
    %>
    <tr>
      <td><a href="/ring.jsp?g=<%=URLEnc.encode(ringGroup.getName())%>&n=<%=ring.getRingNumber()%>">Ring <%=ring.getRingNumber()%></a></td>
      <td class='centered'><%=ring.getState()%></td>
      <%
      UpdateProgress progress = null;
      if (Rings.isUpdatePending(ring)) {
        DomainGroupVersion domainGroupVersion = ring.getUpdatingToVersion();
        if (domainGroupVersion != null) {
          progress = Rings.computeUpdateProgress(ring, domainGroupVersion);
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

      <td class='centered'><%= ring.getCurrentVersionNumber() != null ? ring.getCurrentVersionNumber() : "-" %></td>
      <td class='centered'><%= ring.getUpdatingToVersionNumber() != null ? ring.getUpdatingToVersionNumber() : "-" %></td>

      <%
      int hostsTotal = ring.getHosts().size();
      int hostsServing = Rings.getHostsInState(ring, HostState.SERVING).size();
      int hostsUpdating = Rings.getHostsInState(ring, HostState.UPDATING).size();
      int hostsIdle = Rings.getHostsInState(ring, HostState.IDLE).size();
      int hostsOffline = Rings.getHostsInState(ring, HostState.OFFLINE).size();
      %>

        <td class='host-total'><%= hostsTotal %></td>
        <% if (hostsServing != 0 && hostsServing == hostsTotal) { %>
          <td class='host-serving'>
        <% } else if (hostsServing != 0) { %>
          <td class='host-serving-incomplete'>
        <% } else { %>
          <td class='centered'>
        <% } %>
        <%= hostsServing != 0 ? Integer.toString(hostsServing) : "-" %></td>
        <% if (hostsUpdating != 0) { %>
          <td class='host-updating'><%= hostsUpdating %></td>
        <% } else { %>
          <td class='centered'>-</td>
        <% } %>
        <% if (hostsIdle != 0) { %>
          <td class='host-idle'><%= hostsIdle %></td>
        <% } else { %>
          <td class='centered'>-</td>
        <% } %>
        <% if (hostsOffline != 0) { %>
          <td class='host-offline'><%= hostsOffline %></td>
        <% } else { %>
          <td class='centered'>-</td>
        <% } %>

      <!-- Runtime Statistics -->
      <%
        RuntimeStatisticsAggregator runtimeStatisticsForRing =
          RingGroups.computeRuntimeStatisticsForRing(runtimeStatistics, ring);
      %>

      <td class='centered'> <%= new DecimalFormat("#.##").format(runtimeStatisticsForRing.getThroughput()) %> qps </td>
      <td class='centered'> <%= new DecimalFormat("#.##").format(runtimeStatisticsForRing.getHitRate() * 100) %>% </td>

        <!-- Serving Status -->

        <%
        ServingStatusAggregator ringServingStatusAggregator = null;
        ServingStatus ringServingStatus = null;
        ServingStatus ringUniquePartitionsServingStatus = null;
        DomainGroupVersion ringMostRecentDomainGroupVersion = Rings.getMostRecentVersion(ring);
        if (ringMostRecentDomainGroupVersion != null) {
          ringServingStatusAggregator = Rings.computeServingStatusAggregator(ring, ringMostRecentDomainGroupVersion);
          ringServingStatus = ringServingStatusAggregator.computeServingStatus();
          ringUniquePartitionsServingStatus = ringServingStatusAggregator.computeUniquePartitionsServingStatus(ringMostRecentDomainGroupVersion);
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
          <% if (ringUniquePartitionsServingStatus.getNumPartitionsServedAndUpToDate() != 0
                 && ringUniquePartitionsServingStatus.getNumPartitionsServedAndUpToDate() == ringUniquePartitionsServingStatus.getNumPartitions()) { %>
            <td class='centered complete'>
          <% } else { %>
            <td class='centered error'>
          <% } %>
          <%= ringUniquePartitionsServingStatus.getNumPartitionsServedAndUpToDate() %> / <%= ringUniquePartitionsServingStatus.getNumPartitions() %>
          </td>
        <% } else { %>
          <td></td>
          <td></td>
        <% } %>

      <!-- Actions -->

      <td>
        <form action="/ring_group/delete_ring" method="post">
          <input type="hidden" name="g" value="<%= ringGroup.getName() %>"/>
          <input type="hidden" name="n" value="<%= ring.getRingNumber() %>"/>
          <input type="submit" value="delete" onclick="return confirm('Are you sure you want to delete this ring? This action cannot be undone!');" />
        </form>
      </td>
    </tr>
    <%
      }
    %>
  </table>

    <h2>Query</h2>
    <% if (ringGroup.getCurrentVersion() == null) { %>
      Query disabled because no domain group version is currently deployed.
    <% } else { %>
      <form action="/ring_group.jsp" method=post>
        <input type=hidden name="name" value="<%=ringGroup.getName()%>"/>

        Domain
        <br/>
        <select name="d">
          <%
            for (DomainGroupVersionDomainVersion dgvdv : currentDomainGroupVersion.getDomainVersionsSorted()) {
          %>
          <option<%= request.getParameter("d") != null && URLEnc.decode(request.getParameter("d")).equals(dgvdv.getDomain().getName()) ? " selected" : "" %>>
            <%= dgvdv.getDomain().getName() %>
          </option>
          <% } %>
        </select>
        <br/>
        Key
        <br/>
        <textarea rows=2 cols=30 name="k"><%= request.getParameter("k") == null ? "" : request.getParameter("k") %></textarea>
        <br/>
        in <select name="f">
          <option<%= request.getParameter("f") != null && request.getParameter("f").equals("string") ? " selected" : "" %>>string</option>
          <option<%= request.getParameter("f") != null && request.getParameter("f").equals("hex") ? " selected" : "" %>>hex</option>
        </select>
        <br/>
        <input type=submit value="Get"/>

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
