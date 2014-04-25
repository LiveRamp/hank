<%@ page language="java" contentType="text/html; charset=ISO-8859-1"
    pageEncoding="ISO-8859-1"%>

<%@page import="com.liveramp.hank.ui.*"%>
<%@page import="com.liveramp.hank.coordinator.*"%>
<%@page import="java.util.*"%>
<%@page import="java.net.URLEncoder"%>

<%
  Coordinator coord = (Coordinator) getServletContext().getAttribute("coordinator");
  RingGroup ringGroup = coord.getRingGroup(request.getParameter("g"));
  Ring ring = ringGroup.getRing(Integer.parseInt(request.getParameter("r")));
  Host host = ring.getHostByAddress(PartitionServerAddress.parse(URLEnc.decode(request.getParameter("h"))));
%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">

<html>
<head>
  <meta http-equiv="Content-Type" content="text/html; charset=ISO-8859-1">
  <title>Hank: Ring Groups</title>

  <jsp:include page="_head.jsp" />
</head>
<body>

  <jsp:include page="_top_nav.jsp" />

<h1>
  Manage Ring Group <a href="/admin_ring_group.jsp?g=<%=ringGroup.getName() %>"><%= ringGroup.getName() %></a>
  &gt;
  <a href="/admin_ring.jsp?g=<%=ringGroup.getName() %>&n=<%= ring.getRingNumber() %>">Ring <%= ring.getRingNumber() %></a>
  &gt; <span class='currentItem'><%= host.getAddress() %></span>
</h1>

  <h2>Update Configuration</h2>

  <form action="/host/update" method="post">
  <input type=hidden name="g" value="<%= ringGroup.getName() %>"/>
  <input type=hidden name="n" value="<%= ring.getRingNumber() %>"/>
  <input type=hidden name="h" value="<%= host.getAddress() %>"/>
  <table>
    <tr>
      <td>
        Host Flags:
      </td>
      <td>
        <input type=text name="hostFlags" size=50 value="<%= Hosts.joinHostFlags(host.getFlags()) %>"/>
      </td>
    </tr>
    <tr>
      <td>
        Host Address:
      </td>
      <td>
        <input type=text name="hostAddress" size=50 value="<%= host.getAddress().toString() %>"/>
      </td>
    </tr>
    <tr>
      <td></td>
      <td>
        <input type=submit value="Save updated configuration"/>
      </td>
    </tr>
  </table>
  </form>

  <h2>Delete Host</h2>

    <form method="post" action="/ring/delete_host">
      <input type=hidden name="g" value="<%= ringGroup.getName() %>"/>
      <input type=hidden name="n" value="<%= ring.getRingNumber() %>"/>
      <input type=hidden name="h" value="<%= host.getAddress() %>"/>
      <input type="submit" value="Delete Host <%= host.getAddress() %>" onclick="return confirm('Are you sure you want to delete this host? This action cannot be undone!')"/>
    </form>

  <h2>Manage Assigned Partitions</h2>

  <table class="table-blue">
    <tr><th>Domain</th><th>Partition Number</th><th>Current Version</th><th>Toggle Deletable</th></tr>
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
      <td><%= hdpc.getCurrentDomainVersion() %></td>
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

  <jsp:include page="_footer.jsp"/>

</body>
</html>
