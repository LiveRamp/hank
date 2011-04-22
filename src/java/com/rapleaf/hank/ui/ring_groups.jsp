<%@ page language="java" contentType="text/html; charset=ISO-8859-1"
    pageEncoding="ISO-8859-1"%>

<%@page import="com.rapleaf.hank.coordinator.*"%>

<%
Coordinator coord = (Coordinator)getServletContext().getAttribute("coordinator");
%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">

<%@page import="java.net.URLEncoder"%><html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=ISO-8859-1">
<title>Hank: Ring Groups</title>
</head>
<body>

<a href="index.jsp">Home</a>
<a href="domains.jsp">Domains</a>
<a href="domain_groups.jsp">Domain Groups</a>
<a href="ring_groups.jsp">Ring Groups</a>

<h1>Ring Groups</h1>
<table>
  <tr>
    <td><strong>Name</strong></td>
    <td><strong>Domain Group</strong></td>
    <td><strong>Status</strong></td>
  </tr>
  <%
  for (RingGroupConfig ringGroupConfig : coord.getRingGroups()) {
    %>
    <tr>
      <td><%= ringGroupConfig.getName() %></td>
      <td><a href="domain_group.jsp?n=<%= URLEncoder.encode(ringGroupConfig.getDomainGroupConfig().getName()) %>"><%= ringGroupConfig.getDomainGroupConfig().getName() %></a></td>
      <td><%= ringGroupConfig.isUpdating() ? "UPDATING" : "UP" %></td>
    </tr>
    <tr>
      <td colspan=3>
        <table>
          <tr><td colspan=2>Ring #</td><td>Status</td><td>Ver #</td><td>Upd #</td></tr>
          <% for (RingConfig ringConfig : ringGroupConfig.getRingConfigs()) { %>
          <tr>
            <td width=10>&nbsp;</td>
            <td>ring-<%= ringConfig.getRingNumber() %></td>
            <td align=center><%= ringConfig.getState() %></td>
            <td><%= ringConfig.getVersionNumber() %></td>
            <td><%= ringConfig.getUpdatingToVersionNumber() %></td>
          </tr>
          <tr>
            <td width=10>&nbsp;</td>
            <td colspan=4>
              <table>
                <tr><td colspan=2>host</td><td>status</td></tr>
                <% for (HostConfig hostConfig : ringConfig.getHosts()) { %>
                <tr>
                  <td width=10>&nbsp;</td>
                  <td><%= hostConfig.getAddress() %></td>
                  <td><%= hostConfig.getState() %></td>
                </tr>
                <% } %>
              </table>
            </td>
          </tr>
          <% } %>
        </table>
      </td>
    </tr>
    <%
  }
  %>
</table>

</body>
</html>