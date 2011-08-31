<%@ page language="java" contentType="text/html; charset=ISO-8859-1"
    pageEncoding="ISO-8859-1"%>

<%@page import="com.rapleaf.hank.coordinator.*"%>
<%@page import="com.rapleaf.hank.ui.*"%>
<%@page import="java.net.*"%>
<%@page import="java.util.*"%>
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
      <th>ID</th>
      <th>Partitioner</th>
      <th>Num Partitions</th>
      <th>Storage Engine</th>
    </tr>
    <%
      for (Domain domain : new TreeSet<Domain>(coord.getDomains())) {
    %>
      <tr>
        <td><a href="/domain.jsp?n=<%= URLEnc.encode(domain.getName()) %>"><%= domain.getName() %></a></td>
        <td><%= domain.getId() %></td>
        <td><%= domain.getPartitioner().getClass().getSimpleName() %></td>
        <td class='centered'><%= domain.getNumParts() %></td>
        <td><%= domain.getStorageEngineFactoryClass().getName() %></td>
      </tr>
      <%
    }
    %>
  </table>

</body>
</html>
