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
    for (Domain domain : new TreeSet<Domain>(coord.getDomains())) {
  %>
    <tr>
      <td>
        <%= domain.getName() %>
      </td>
      <td>

          <%
          SortedSet<DomainVersion> revSorted = new TreeSet<DomainVersion>(new ReverseComparator<DomainVersion>());
          revSorted.addAll(domain.getVersions());
          boolean first = true;
          for (DomainVersion ver : revSorted) {
            if (ver.isDefunct()) {
              continue;
            }
          %>

        <input type="text" name="<%=domain.getName() %>_version" />
        <input type="checkbox" name="<%=domain.getName() %>_included" />

        <select name="<%=domain.getName() %>_version">
          <option value="none">unassign</option>
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
        </select>



      </td>
    </tr>
  <%
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
