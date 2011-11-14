<%@ page language="java" contentType="text/html; charset=ISO-8859-1"
    pageEncoding="ISO-8859-1"%>

<%@page import="com.rapleaf.hank.coordinator.*"%>
<%@page import="com.rapleaf.hank.partition_server.*"%>
<%@page import="com.rapleaf.hank.ui.*"%>
<%@page import="java.util.*"%>
<%@page import="java.text.DecimalFormat" %>

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
  <jsp:include page="_head.jsp" />

  <style type="text/css">
    td.unassigned {background-color: #ddd}
    td.undeployed {background-color: #f00}
    td.updating {background-color: #00f}
    td.updated {background-color: #0f0}

    div.part_assignment_visualization {float:left; padding: 3px}
  </style>
</head>
<body>

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
    Hosts.computeRuntimeStatistics(host);

  RuntimeStatisticsAggregator runtimeStatisticsForHost =
    Hosts.computeRuntimeStatisticsForHost(runtimeStatistics);

  DomainGroupVersion currentDomainGroupVersion = ring.getCurrentVersion();
%>

<div>
  <h2>State</h2>
    <table class='table-blue-compact'>
      <tr>
      <td>State:</td>
      <td>
      <div class='centered <%= UiUtils.hostStateToClass(host.getState()) %>'><%= host.getState() %></div>
      </td>
      </tr>

      <tr>
      <td>Throughput:</td>
      <td>
      <%= new DecimalFormat("#.##").format(runtimeStatisticsForHost.getThroughput()) %> qps
      </td>
      </tr>

      <tr>
      <td>Hit Rate:</td>
      <td>
      <%= new DecimalFormat("#.##").format(runtimeStatisticsForHost.getHitRate() * 100) %>%
      </td>
      </tr>

      <tr>
      <td>Current Command:</td>
      <td>

      <form method="post" action="/host/discard_current_command">
      <input type="hidden" name="g" value="<%= ringGroup.getName() %>"/>
      <input type="hidden" name="n" value="<%= ring.getRingNumber() %>"/>
      <input type="hidden" name="h" value="<%= host.getAddress() %>"/>
      <%= host.getCurrentCommand() %> <% if (host.getCurrentCommand() != null) { %><input type="submit" value="discard"/><% } %>
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
     for (DomainGroupVersionDomainVersion dgvdv : currentDomainGroupVersion.getDomainVersionsSorted()) {
       Domain domain = dgvdv.getDomain();
       RuntimeStatisticsAggregator runtimeStatisticsForDomain =
       Hosts.computeRuntimeStatisticsForDomain(runtimeStatistics, domain);
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

  <h2>Enqueue Command</h2>

  <form action="/host/enqueue_command" method="post">
    <input type="hidden" name="g" value="<%= ringGroup.getName() %>"/>
    <input type="hidden" name="n" value="<%= ring.getRingNumber() %>"/>
    <input type="hidden" name="h" value="<%= host.getAddress() %>"/>

    <select name="command">
    <% for (HostCommand cmd : HostCommand.values()) {%>
      <option><%= cmd.name() %></option>
    <% } %>
    </select>
    <input type="submit" value="Enqueue Command"/>
  </form>
</div>

<!-- Domains and Partitions -->

<div>
  <h2>Domains and Partitions</h2>
  <form method="post" action="/host/add_domain_part">
    <input type="hidden" name="g" value="<%= ringGroup.getName() %>"/>
    <input type="hidden" name="n" value="<%= ring.getRingNumber() %>"/>
    <input type="hidden" name="h" value="<%= host.getAddress() %>"/>

    Add a domain partition:<br/>
    <select name="domainId">
      <%
        if (ringGroup.getCurrentVersion() != null) {
          for (DomainGroupVersionDomainVersion dgvdv : ringGroup.getCurrentVersion().getDomainVersions()) {
          %>
          <option value="<%=dgvdv.getDomain().getName()%>"><%=dgvdv.getDomain().getName()%></option>
          <%
          }
        }
      %>
    </select>
    Part:<input type=text size=4 name="partNum" />
    Initial Version:<input type=text size=4 name="initialVersion"/>
    <input type=submit value="Add"/>
  </form>

  <div style="border: 1px solid #ddd; width: 200px">
    <table style="font-size: 8px">
      <tr>
        <td colspan=2 align=center style="border-bottom: 1px solid #ddd">Legend</td>
      </tr>
      <tr>
        <td class="unassigned" style="width:6px; height: 6px; font-size:0px">&nbsp;</td>
        <td>Not assigned</td>
      </tr>
      <tr>
        <td class="undeployed" style="width:6px; height: 6px; font-size:0px">&nbsp;</td>
        <td>Assigned, no version deployed</td>
      </tr>
      <tr>
        <td class="updating" style="width:6px; height: 6px; font-size:0px">&nbsp;</td>
        <td>Assigned, some version deployed, update pending</td>
      </tr>
      <tr>
        <td class="updated" style="width:6px; height: 6px; font-size:0px">&nbsp;</td>
        <td>Assigned, latest version deployed</td>
      </tr>
    </table>
  </div>
  <%
    for (HostDomain hdc : host.getAssignedDomainsSorted()) {
      Domain domain = hdc.getDomain();
      int squareDim = (int)Math.floor(Math.sqrt(domain.getNumParts()));
      if (domain == null) {
  %>
    <div>Unknown Domain</div>
  <% } else { %>
  <div class="part_assignment_visualization">
    <div><%= domain.getName() %></div>
    <div>
      <table cellspacing=1 cellpadding=0>
      <%
      for (int i = 0; i < domain.getNumParts(); i++) {
        String className = "unassigned";
        HostDomainPartition hdp = hdc.getPartitionByNumber(i);
        if (hdp != null) {
          if (hdp.getCurrentDomainGroupVersion() == null) {
            className = "undeployed";
          } else if (hdp.getUpdatingToDomainGroupVersion() != null) {
            className = "updating";
          } else {
            className = "updated";
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


  <div style="clear:both"></div>

  <table class="table-blue">
    <tr><th>domain</th><th>part #</th><th>cur ver #</th><th>upd ver #</th><th>toggle deletable</th></tr>
  <%
    for (HostDomain hdc : host.getAssignedDomainsSorted()) {
      Domain domain = hdc.getDomain();
      for (HostDomainPartition hdpc : hdc.getPartitionsSorted()) {
    %>
    <tr>
      <td>
      <% if (domain == null) { %>
        Unknown Domain
      <%  } else { %>
        <%= domain.getName()%>
      </td>
      <td><%= hdpc.getPartitionNumber() %></td>
      <td><%= hdpc.getCurrentDomainGroupVersion() %></td>
      <td><%= hdpc.getUpdatingToDomainGroupVersion() %></td>
      <td>
        <form action= "<%= hdpc.isDeletable() ? "/host/undelete_partition" : "/host/delete_partition" %>" method="post">
          <input type="hidden" name="g" value="<%= ringGroup.getName() %>"/>
          <input type="hidden" name="n" value="<%= ring.getRingNumber() %>"/>
          <input type="hidden" name="h" value="<%= host.getAddress() %>"/>
          <input type="hidden" name="d" value="<%= hdc.getDomain().getName() %>"/>
          <input type="hidden" name="p" value="<%= hdpc.getPartitionNumber() %>"/>
          <input type="submit" value="<%= hdpc.isDeletable() ? "Undelete" : "Delete" %>"
          <% if (!hdpc.isDeletable()) { %>
            onclick="return confirm('Are you sure you want to mark this partition for deletion?');"
          <% } %>
          />
        </form>
      </td>
    </tr>
    <% } %>
    <% } %>
  <% } %>
  </table>
</div>

<jsp:include page="_footer.jsp"/>

</body>
</html>
