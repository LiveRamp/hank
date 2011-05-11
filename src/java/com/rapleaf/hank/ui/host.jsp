<%@ page language="java" contentType="text/html; charset=ISO-8859-1"
    pageEncoding="ISO-8859-1"%>
<%@page import="com.rapleaf.hank.coordinator.*"%>
<%@page import="java.util.*"%>
<%@page import="java.net.*"%>

<%

Coordinator coord = (Coordinator)getServletContext().getAttribute("coordinator");

RingGroupConfig ringGroup = coord.getRingGroupConfig(request.getParameter("g"));

RingConfig ring = ringGroup.getRingConfig(Integer.parseInt(request.getParameter("r")));
HostConfig host = ring.getHostConfigByAddress(PartDaemonAddress.parse(URLDecoder.decode(request.getParameter("h"))));
%>

<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=ISO-8859-1">
<title>Host: <%= host.getAddress() %></title>
</head>
<body>

<h3>
  <a href="/ring_group.jsp?name=<%=ringGroup.getName() %>"><%= ringGroup.getName() %></a>
  > 
  <a href="/ring.jsp?g=<%=ringGroup.getName() %>&n=<%= ring.getRingNumber() %>">ring <%= ring.getRingNumber() %></a>
  > <%= host.getAddress() %>
</h3>

<div>
  <h4>Status</h4>
  Currently <%= host.getState() %> <%= host.isOnline() ? "(online)" : "" %><br/>
  Current command: <%= host.getCurrentCommand() %> <br/>
  Queued commands: <%= host.getCommandQueue() %>
</div>

<div>
  <h4>Domains + Parts</h4>
  <form>
    Add a domain/part:<br/>
    <select>
      <% for (DomainConfig domainConfig : ringGroup.getDomainGroupConfig().getDomainConfigs()) { %>
      <option><%= domainConfig.getName() %></option>
      <% } %>
    </select>
    Part:<input type=text/>
    Initial Version:<input type=text/>
    <input type=submit value="Add"/>
  </form>

  <table>
    <tr><td>domain</td><td>part #</td><td>cur ver #</td><td>upd ver #</td></tr>
  <% for (HostDomainConfig hdc : host.getAssignedDomains()) { %>
    <tr>
      <td><%= ringGroup.getDomainGroupConfig().getDomainConfig(hdc.getDomainId()).getName() %></td>
    </tr>
    <% for (HostDomainPartitionConfig hdpc : hdc.getPartitions()) { %>
    <tr>
      <td></td>
      <td><%= hdpc.getPartNum() %></td>
      <td><%= hdpc.getCurrentDomainGroupVersion() %></td>
      <td><%= hdpc.getUpdatingToDomainGroupVersion() %></td>
    </tr>
    <% } %>
  <% } %>
  </table>
</div>

</body>
</html>