<%@ page language="java" contentType="text/html; charset=ISO-8859-1"
    pageEncoding="ISO-8859-1"%>

<%@page import="com.liveramp.hank.coordinator.*"%>
<%@page import="com.liveramp.hank.ui.*"%>
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

  <table class='table-blue'>
    <tr>
      <th>Domain</th>
      <th>Required Host Flags</th>
      <th>ID</th>
      <th>Partitioner</th>
      <th>Number of Partitions</th>
      <th>Storage Engine</th>
    </tr>
    <%
      for (Domain domain : coord.getDomainsSorted()) {
    %>
      <tr>
        <td><a href="/domain.jsp?n=<%= URLEnc.encode(domain.getName()) %>"><%= domain.getName() %></a></td>
        <td><%= Hosts.joinHostFlags(domain.getRequiredHostFlags()) %></td>
        <td><%= domain.getId() %></td>
        <td><%= domain.getPartitionerClassName() %></td>
        <td class='centered'><%= domain.getNumParts() %></td>
        <td><%= domain.getStorageEngineFactoryClassName() %></td>
      </tr>
      <%
    }
    %>
  </table>

<jsp:include page="_footer.jsp"/>

</body>
</html>
