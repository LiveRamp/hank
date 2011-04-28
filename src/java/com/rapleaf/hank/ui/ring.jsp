<%@ page language="java" contentType="text/html; charset=ISO-8859-1"
    pageEncoding="ISO-8859-1"%>
    

<%@page import="com.rapleaf.hank.coordinator.*"%>
<%@page import="java.util.*"%>
<%@page import="java.net.*"%>

<%!
public List<HostConfig> sortedHcs(Collection<HostConfig> rcs) {
  List<HostConfig> sortedList = new ArrayList<HostConfig>(rcs);
  Collections.sort(sortedList, new HostConfigComparator());
  return sortedList;
}
%>

<%

Coordinator coord = (Coordinator)getServletContext().getAttribute("coordinator");

RingGroupConfig ringGroup = coord.getRingGroupConfig(request.getParameter("g"));

RingConfig ring = ringGroup.getRingConfig(Integer.parseInt(request.getParameter("n")));
%>

<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=ISO-8859-1">
<title>Ring <%= ring.getRingNumber() %> in group <%= ringGroup.getName() %></title>
</head>
<body>

<a href="index.jsp">Home</a>
<a href="domains.jsp">Domains</a>
<a href="domain_groups.jsp">Domain Groups</a>
<a href="ring_groups.jsp">Ring Groups</a>

<h1>Ring <%= ring.getRingNumber() %> in group <a href="/ring_group.jsp?name=<%= URLEncoder.encode(ringGroup.getName()) %>"><%= ringGroup.getName() %></a></h1>

<div>
status blob
</div>

<h3>Hosts</h3>

<form action="/ring/add_host" method=post>
  Add a new host: <br/>
  <input type=hidden name="rgName" value="<%= ringGroup.getName() %>"/>
  <input type=hidden name="ringNum" value="<%= ring.getRingNumber() %>"/>
  Host:
  <input type=text size=30 name="hostname"/>
  <br/>
  Port:
  <input type=text size=5 name="port" />
  <br/>
  <input type=submit value="Add"/>
</form>

<table width=500>
  <tr>
    <td><strong>Address</strong></td>
    <td><strong>Status</strong></td>
    <td><strong>Cur. Cmd.</strong></td>
    <td><strong>Queue</strong></td>
  </tr>
  <% for(HostConfig host : sortedHcs(ring.getHosts())) { %>
  <tr>
    <td><%= host.getAddress() %></td>
    <td><%= host.getState() %></td>
    <td><%= host.getCurrentCommand() %></td>
    <td><%= host.getCommandQueue() %></td>
  </tr>
  <% } %>
</table>

</body>
</html>