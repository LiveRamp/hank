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
  <title>Hank: Domains</title>
  
  <jsp:include page="_head.jsp" />
</head>
<body>

  <jsp:include page="_top_nav.jsp" />


  <h1>Domains</h1>

  <a href="new_domain.jsp">Create a new domain</a>
  <table class='table-blue'>
    <tr>
      <th>Name</th>
      <th>Partitioner</th>
      <th>Num Partitions</th>
      <th>Storage Engine</th>
      <th>Version</th>
    </tr>
    <%
    for (DomainConfig domainConfig : coord.getDomainConfigs()) {
      %>
      <tr>
        <td><%= domainConfig.getName() %></td>
        <td><%= domainConfig.getPartitioner().getClass().getSimpleName() %></td>
        <td class='centered'><%= domainConfig.getNumParts() %></td>
        <td><%= domainConfig.getStorageEngineFactoryClass().getName() %></td>
        <td class='centered'><%= domainConfig.getVersion() %></td>
      </tr>
      <%
    }
    %>
  </table>

</body>
</html>