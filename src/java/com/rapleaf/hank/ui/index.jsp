<%@ page language="java" contentType="text/html; charset=ISO-8859-1"
    pageEncoding="ISO-8859-1"%>
    
<%@page import="com.rapleaf.hank.coordinator.*"%>

<%
Coordinator coord = (Coordinator)getServletContext().getAttribute("coordinator");
%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=ISO-8859-1">
<title>Hank Management UI</title>
</head>
<body>

<h1>Domains</h1>

<table border=1>
  <tr>
    <td>Name</td>
    <td>Storage Engine</td>
    <td>Version</td>
  </tr>
  <%
  for (DomainConfig domainConfig : coord.getDomainConfigs()) {
    %>
    <tr>
      <td><%= domainConfig.getName() %></td>
      <td><%= domainConfig.getStorageEngine().getClass().getSimpleName() %></td>
      <td><%= domainConfig.getVersion() %></td>
    </tr>
    <%
  }
  %>
</table>

<h1>Domain Groups</h1>
<table border=1>
  <tr>
    <td>Name</td>
  </tr>
  <%
  for (DomainGroupConfig domainConfig : coord.getDomainGroupConfigs()) {
    %>
    <tr>
      <td><%= domainConfig.getName() %></td>
    </tr>
    <%
  }
  %>
</table>



<h1>Ring Groups</h1>
<table border=1>
  <tr>
    <td>Name</td>
  </tr>
  <%
  for (RingGroupConfig ringGroupConfig : coord.getRingGroups()) {
    %>
    <tr>
      <td><%= ringGroupConfig.getName() %></td>
    </tr>
    <%
  }
  %>
</table>

</body>
</html>