<%@ page language="java" contentType="text/html; charset=ISO-8859-1"
    pageEncoding="ISO-8859-1"%>

<%@page import="com.rapleaf.hank.coordinator.*"%>
<%@page import="com.rapleaf.hank.ui.*"%>
<%@page import="com.rapleaf.hank.util.*"%>
<%@page import="java.util.*"%>

<%
  Coordinator coord = (Coordinator)getServletContext().getAttribute("coordinator");

DomainGroup domainGroupConfig = coord.getDomainGroupConfig(URLEnc.decode(request.getParameter("n")));
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
<table width=300 class='table-blue'>
  <tr>
    <th>Name</th>
    <th>ID</th>
  </tr>
  <%
    for (Domain domain : domainGroupConfig.getDomains()) {
  %>
  <tr>
    <td><a href="/domain.jsp?n=<%=URLEnc.encode(domain.getName())%>"><%=domain.getName()%></a></td>
    <td><%=domainGroupConfig.getDomainId(domain.getName())%></td>
  </tr>
  <%
    }
  %>
</table>

<%
  Set<Domain> s = coord.getDomains();
%>
<%
  s.removeAll(domainGroupConfig.getDomains());
%>

<%
  if (!s.isEmpty()) {
%>
<form action="/domain_group/add_domain" method=post>
  <input type=hidden name="n" value="<%=domainGroupConfig.getName()%>"/>

  Add domain:
  <br/>
  <select name="d">
  <%
    for (Domain domain : s) {
  %>
    <option><%=domain.getName()%></option>
  <%
    }
  %>
  </select>
  <input type=submit value="Add"/>
</form>
<%
  }
%>

<h2>Versions</h2>

<form method="post" action="/domain_group/add_version">
  <input type=hidden name="n" value="<%=domainGroupConfig.getName()%>"/>

  Add a new version:<br/>

  <table class='table-blue'>
    <tr>
      <th>Domain</th>
      <th>Version (default: most recent)</th>
    </tr>
  <%
    for (Domain domain : domainGroupConfig.getDomains()) {
    if (!domain.getVersions().isEmpty()) {
  %>
    <tr>
      <td>
        <%= domain.getName() %>
      </td>
      <td>
        <select name="<%=domain.getName() %>_version">
          <%
            SortedSet<DomainVersion> revSorted = new TreeSet<DomainVersion>(new ReverseComparator<DomainVersion>());
                revSorted.addAll(domain.getVersions());
          %>
          <%
            boolean first = true;
                for (DomainVersion ver : revSorted) {
          %>
          <option<%= first ? " selected" : "" %>><%= ver.getVersionNumber() %></option>
          <%
          first = false;
          }
          %>
        </select>
      </td>
    </tr>
  <%
    }
  }
  %>

  </table>
  <input type=submit value="Add"/>
</form>


<ul>
  <%
    for (DomainGroupVersion dgcv : domainGroupConfig.getVersions()) {
  %>
  <li>
    v<%= dgcv.getVersionNumber() %>:
    <ul>
      <%
        for (DomainGroupVersionDomainVersion dcv : dgcv.getDomainVersions()) {
      %>
      <li><%=dcv.getDomain().getName()%> @ v<%=dcv.getVersionNumber() %></li>
      <% } %>
    </ul>
  </li>
  <% } %>
</ul>
</body>
</html>
