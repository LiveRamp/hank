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
<title>Hank: Domain Groups</title>
</head>
<body>

<a href="index.jsp">Home</a>
<a href="domains.jsp">Domains</a>
<a href="domain_groups.jsp">Domain Groups</a>
<a href="ring_groups.jsp">Ring Groups</a>

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


</body>
</html>