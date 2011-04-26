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
<form action="/domain_group/create" method=post>
Add a new domain group:<br/>
<input type=text name="name" size=50/> <input type=submit value="Create"/>
</form>
<table>
  <%
  for (DomainGroupConfig domainConfig : coord.getDomainGroupConfigs()) {
    %>
    <tr>
      <td colspan=2><%= domainConfig.getName() %></td>
    </tr>
    <tr>
      <td width=10>&nbsp;</td>
      <td>
	      <table>
	        <% for (DomainGroupConfigVersion version : domainConfig.getVersions()) { %>
	        <tr>
	          <td colspan=2>v<%= version.getVersionNumber() %></td>
	        </tr>
	        <tr>
            <td width=10>&nbsp;</td>
	          <td>
	            <table>
	              <% for (DomainConfigVersion dcv : version.getDomainConfigVersions()) { %>
	              <tr>
	                <td><%= dcv.getDomainConfig().getName() %> @v<%= dcv.getVersionNumber() %></td>
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