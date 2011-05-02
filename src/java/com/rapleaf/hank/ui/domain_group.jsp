<%@ page language="java" contentType="text/html; charset=ISO-8859-1"
    pageEncoding="ISO-8859-1"%>

<%@page import="com.rapleaf.hank.coordinator.*"%>
<%@page import="java.util.*"%>
<%@page import="java.net.*"%>

<%
Coordinator coord = (Coordinator)getServletContext().getAttribute("coordinator");

DomainGroupConfig domainGroupConfig = coord.getDomainGroupConfig(URLDecoder.decode(request.getParameter("n")));
%>


<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=ISO-8859-1">
<jsp:include page="_head.jsp" />
<title>Domain Group <%= domainGroupConfig.getName() %></title>
</head>
<body>

<jsp:include page="_top_nav.jsp" />

<h1>Domain Group <%= domainGroupConfig.getName() %></h1>

<h2>Domains + Ids</h2>
<table width=300>
  <tr>
    <td><strong>Name</strong></td>
    <td><strong>ID</strong></td>
  </tr>
  <% for (DomainConfig domainConfig : domainGroupConfig.getDomainConfigs()) { %>
  <tr>
    <td><a href="/domain.jsp?n=<%= URLEncoder.encode(domainConfig.getName()) %>"><%= domainConfig.getName() %></a></td>
    <td><%= domainGroupConfig.getDomainId(domainConfig.getName()) %></td>
  </tr>
  <% } %>
</table>
<form action="/domain_group/add_domain" method=post>
  <input type=hidden name="n" value="<%= domainGroupConfig.getName() %>"/>

  Add domain:
  <br/>
  <select>
  <% Set<DomainConfig> s = coord.getDomainConfigs(); %>
  <% s.removeAll(domainGroupConfig.getDomainConfigs()); %>
  <% for (DomainConfig domainConfig : s) { %>
    <option><%= domainConfig.getName() %></option>
  <% } %>
  </select>
  <input type=submit value="Add"/>
</form>


<h2>Versions</h2>

<ul>
  <% for (DomainGroupConfigVersion dgcv : domainGroupConfig.getVersions()) { %>
  <li>
    v<%= dgcv.getVersionNumber() %>:
    <ul>
      <% for (DomainConfigVersion dcv : dgcv.getDomainConfigVersions()) { %>
      <li><%=dcv.getDomainConfig().getName() %> @ v<%=dcv.getVersionNumber() %></li>
      <% } %>
    </ul>
  </li>
  <% } %>
</ul>
</body>
</html>