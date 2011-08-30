<%@ page language="java" contentType="text/html; charset=ISO-8859-1"
    pageEncoding="ISO-8859-1"%>

<%@page import="com.rapleaf.hank.coordinator.*"%>
<%@page import="com.rapleaf.hank.ui.*"%>
<%@page import="com.rapleaf.hank.util.*"%>
<%@page import="java.util.*"%>

<%
  Coordinator coord = (Coordinator)getServletContext().getAttribute("coordinator");

DomainGroup domainGroup = coord.getDomainGroup(URLEnc.decode(request.getParameter("n")));
%>


<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=ISO-8859-1">
<jsp:include page="_head.jsp" />
<title>Domain Group <%= domainGroup.getName() %></title>
</head>
<body>

<jsp:include page="_top_nav.jsp" />

<h1>Domain Group <%= domainGroup.getName() %></h1>

<h2>Domains + Ids</h2>
<table width=300 class='table-blue'>
  <tr>
    <th width=100%>Name</th>
    <th>ID</th>
    <th></th>
  </tr>
  <%
    for (Domain domain : new TreeSet<Domain>(domainGroup.getDomains())) {
  %>
  <tr>
    <td><a href="/domain.jsp?n=<%=URLEnc.encode(domain.getName())%>"><%=domain.getName()%></a></td>
    <td><%=domainGroup.getDomainId(domain.getName())%></td>
    <td>
      <% if (domainGroup.isDomainRemovable(domain)) { %>
      <form action="/domain_group/unassign" method=post>
        <input type=hidden name="n" value="<%= domainGroup.getName() %>"/>
        <input type=hidden name="domain" value="<%= domain.getName() %>"/>
        <input type=submit value="Remove from domain group"
          onclick="return confirm('Are you sure you want to remove domain <%= domain.getName() %> from this domain group? This action cannot be undone automatically.');"/>
      </form>
      <% } %>
    </td>
  </tr>
  <%
    }
  %>
</table>

<%
  Set<Domain> s = new TreeSet<Domain>(coord.getDomains());
  s.removeAll(domainGroup.getDomains());

  if (!s.isEmpty()) {
%>
<form action="/domain_group/add_domain" method=post>
  <input type=hidden name="n" value="<%=domainGroup.getName()%>"/>

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

<h2>Actions</h2>
<form method=post action="/domain_group/delete">
  <input type=hidden name="name" value="<%= domainGroup.getName() %>"/>
  <input type=submit value="Delete this domain group"
    onclick="return confirm('Are you sure you want to delete this domain group? This action cannot be undone.');"/>
</form>

<h2>Versions</h2>

<form method="post" action="/domain_group/add_version">
  <input type=hidden name="n" value="<%=domainGroup.getName()%>"/>

  Add a new version:<br/>

  <table class='table-blue'>
    <tr>
      <th>Domain</th>
      <th>Version (default: most recent)</th>
    </tr>
  <%
    for (Domain domain : new TreeSet<Domain>(domainGroup.getDomains())) {
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
          boolean first = true;
          for (DomainVersion ver : revSorted) {
            if (ver.isDefunct()) {
              continue;
            }
          %>
          <option<%= first ? " selected" : "" %>><%= ver.getVersionNumber() %></option>
          <%
          first = false;
          }
          %>
          <option value="unassign">unassign</option>
        </select>
      </td>
    </tr>
  <%
    }
  }
  %>

  </table>
  <input type=submit value="Add"/> <br/>
  <span style="color: red; font-weight:bold">This will likely trigger a data deploy!</span>
</form>


<ul>
  <%
    SortedSet<DomainGroupVersion> dgvRev = new TreeSet<DomainGroupVersion>(new ReverseComparator<DomainGroupVersion>());
    dgvRev.addAll(domainGroup.getVersions());
    for (DomainGroupVersion dgcv : dgvRev) {
  %>
  <li>
    v<%= dgcv.getVersionNumber() %>:
    <ul>
      <%
        for (DomainGroupVersionDomainVersion dcv : new TreeSet<DomainGroupVersionDomainVersion>(dgcv.getDomainVersions())) {
      %>
      <li>
        <%=dcv.getDomain().getName()%>
        <% if (dcv.getVersionOrAction().isAction()) { %>
        (unassigned)
        <% } else { %>
        @ v<%= dcv.getVersionOrAction().getVersion() %>
        <% } %>
      </li>
      <% } %>
    </ul>
  </li>
  <% } %>
</ul>
</body>
</html>
