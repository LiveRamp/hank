<%@ page language="java" contentType="text/html; charset=ISO-8859-1"
    pageEncoding="ISO-8859-1"%>

<%@page import="com.rapleaf.hank.coordinator.*"%>
<%@page import="java.util.*"%>
<%@page import="java.net.*"%>
<%!

public List<RingConfig> sortedRcs(Collection<RingConfig> rcs) {
  List<RingConfig> sortedList = new ArrayList<RingConfig>(rcs);
  Collections.sort(sortedList, new RingConfigComparator());
  return sortedList;
}
%>
<%

Coordinator coord = (Coordinator)getServletContext().getAttribute("coordinator");

RingGroupConfig ringGroup = coord.getRingGroupConfig(request.getParameter("name"));

%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=ISO-8859-1">
<title>Ring Group <%= ringGroup.getName() %></title>
</head>
<body>

<a href="index.jsp">Home</a>
<a href="domains.jsp">Domains</a>
<a href="domain_groups.jsp">Domain Groups</a>
<a href="ring_groups.jsp">Ring Groups</a>

<h1>Ring Group <%= ringGroup.getName() %></h1>
<div>
<a href="">link to domain group</a>
</div>
<div>
overall status blob
</div>

<h3>Rings</h3>
<table width=500>
  <tr>
    <td><strong>#</strong></td>
    <td><strong>Status</strong></td>
    <td><strong>Cur. Ver.</strong></td>
    <td><strong>Next Ver.</strong></td>
    <td><strong># hosts</strong></td>
    <td></td>
  </tr>
  <% for (RingConfig ring : sortedRcs(ringGroup.getRingConfigs())) { %>
  <tr>
    <td><%= ring.getRingNumber() %></td>
    <td><%= ring.getState() %></td>
    <td><%= ring.getVersionNumber() %></td>
    <td><%= ring.getUpdatingToVersionNumber() %></td>
    <td><%= ring.getHosts().size() %></td>
    <td><a href="/ring.jsp?g=<%= URLEncoder.encode(ringGroup.getName()) %>&n=<%= ring.getRingNumber() %>">details</a></td>
  </tr>
  <% } %>
</table>
<a href="/ring_group/add_ring?g=<%= URLEncoder.encode(ringGroup.getName()) %>">Add a new ring group</a>
</body>
</html>