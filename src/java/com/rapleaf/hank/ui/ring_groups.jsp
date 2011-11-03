<%@ page language="java" contentType="text/html; charset=ISO-8859-1"
    pageEncoding="ISO-8859-1"%>

<%@page import="com.rapleaf.hank.ui.*"%>
<%@page import="com.rapleaf.hank.coordinator.*"%>
<%@page import="java.util.*"%>
<%@page import="java.text.DecimalFormat" %>

<%!public List<RingGroup> ringGroups(Coordinator coord) {
  List<RingGroup> rgcs = new ArrayList<RingGroup>(coord.getRingGroups());
  Collections.sort(rgcs, new RingGroupConfigComparator());
  return rgcs;
}%>
<%
  Coordinator coord = (Coordinator)getServletContext().getAttribute("coordinator");
%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">

<%@page import="java.net.URLEncoder"%><html>
<head>
  <meta http-equiv="Content-Type" content="text/html; charset=ISO-8859-1">
  <title>Hank: Ring Groups</title>

  <jsp:include page="_head.jsp" />
</head>
<body>

  <jsp:include page="_top_nav.jsp" />

  <h1>Ring Groups</h1>

  <h2>Create New Ring Group</h2>

  <form action="/ring_group/create" method=post>
    <select name="dgName">
      <%
        for (DomainGroup dgc : coord.getDomainGroups()) {
      %>
      <option><%=dgc.getName()%></option>
      <%
        }
      %>
    </select>
    <input type=text size=30 name="rgName"/> <input type=submit value="Create"/>
  </form>

  <h2>All Ring Groups</h2>

  <table class='table-blue'>
    <tr>
      <th>Ring Group</th>
      <th>Domain Group</th>
      <th>State</th>
      <th></th>
      <th></th>
      <th>Current Version</th>
      <th>Updating to Version</th>
      <th>Hosts</th>
      <th>Serving</th>
      <th>Updating</th>
      <th>Idle</th>
      <th>Offline</th>
      <th>Partitions served</th>
      <th>Unique partitions served</th>
    </tr>
    <%
      for (RingGroup ringGroup : ringGroups(coord)) {
    %>
      <tr>
        <td><a href="/ring_group.jsp?name=<%= URLEnc.encode(ringGroup.getName()) %>"><%= ringGroup.getName() %></a></td>
        <td><a href="domain_group.jsp?n=<%=URLEnc.encode(ringGroup.getDomainGroup().getName())%>"><%=ringGroup.getDomainGroup().getName()%></a></td>
        <td><%= RingGroups.isUpdating(ringGroup) ? "UPDATING" : "UP" %></td>

        <%
        UpdateProgress progress = null;
        if (RingGroups.isUpdating(ringGroup)) {
          DomainGroupVersion domainGroupVersion = ringGroup.getDomainGroup().getVersionByNumber(ringGroup.getUpdatingToVersion());
          if (domainGroupVersion != null) {
            progress = RingGroups.computeUpdateProgress(ringGroup, domainGroupVersion);
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
          <%= new DecimalFormat("#.##").format(progress.getUpdateProgress() * 100) %>% updated
          (<%= progress.getNumPartitionsUpToDate() %>/<%= progress.getNumPartitions() %>)
        </td>
        <% } else { %>
        <td></td>
        <td></td>
        <% } %>


        <td class='centered'><%= ringGroup.getCurrentVersion() %></td>
        <td class='centered'><%= ringGroup.getUpdatingToVersion() %></td>

        <%
        int hostsTotal = RingGroups.getNumHosts(ringGroup);
        int hostsServing = RingGroups.getHostsInState(ringGroup, HostState.SERVING).size();
        int hostsUpdating = RingGroups.getHostsInState(ringGroup, HostState.UPDATING).size();
        int hostsIdle = RingGroups.getHostsInState(ringGroup, HostState.IDLE).size();
        int hostsOffline = RingGroups.getHostsInState(ringGroup, HostState.OFFLINE).size();
        %>

        <td class='host-total'><%= hostsTotal %></td>
        <% if (hostsServing != 0 && hostsServing == hostsTotal) { %>
          <td class='host-serving complete'>
        <% } else if (hostsServing != 0) { %>
          <td class='host-serving'>
        <% } else { %>
          <td>
        <% } %>
        <%= hostsServing != 0 ? Integer.toString(hostsServing) : "" %></td>
        <% if (hostsUpdating != 0) { %>
          <td class='host-updating'><%= hostsUpdating %></td>
        <% } else { %>
          <td></td>
        <% } %>
        <% if (hostsIdle != 0) { %>
          <td class='host-idle'><%= hostsIdle %></td>
        <% } else { %>
          <td></td>
        <% } %>
        <% if (hostsOffline != 0) { %>
          <td class='host-offline'><%= hostsOffline %></td>
        <% } else { %>
          <td></td>
        <% } %>

        <%
        ServingStatusAggregator servingStatusAggregator = null;
        ServingStatus servingStatus = null;
        ServingStatus uniquePartitionsServingStatus = null;
        if (ringGroup.getCurrentVersion() != null) {
          DomainGroupVersion currentDomainGroupVersion = ringGroup.getDomainGroup().getVersionByNumber(ringGroup.getCurrentVersion());
          servingStatusAggregator = RingGroups.computeServingStatusAggregator(ringGroup, currentDomainGroupVersion);
          servingStatus = servingStatusAggregator.computeServingStatus();
          uniquePartitionsServingStatus = servingStatusAggregator.computeUniquePartitionsServingStatus(currentDomainGroupVersion);
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
