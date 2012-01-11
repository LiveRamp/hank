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
<script type="text/javascript">
function toggleDisabled(id) {
  element = document.getElementById(id);
  if (element.disabled == true) {
    element.disabled = false;
  } else {
    element.disabled = true;
  }
}
function toggleClass(id, a, b) {
  element = document.getElementById(id);
  if (element.className == a) {
    element.className = b;
  } else {
    element.className = a;
  }
}
</script>
<style type="text/css">
tr.included td {
}
tr.not_included td {
  opacity: 0.8;
  background-color: #c0c0c0;
}
</style>
</head>
<body>

<jsp:include page="_top_nav.jsp" />

<h1>Domain Group <span class='currentItem'><%= domainGroup.getName() %></span></h1>

<h2>Actions</h2>
<form method=post action="/domain_group/delete">
  <input type=hidden name="name" value="<%= domainGroup.getName() %>"/>
  <input type=submit value="Delete this domain group"
    onclick="return confirm('Are you sure you want to delete this domain group? This action cannot be undone.');"/>
</form>

<h2>Create New Version</h2>

<form method="post" action="/domain_group/add_version">
  <input type=hidden name="n" value="<%=domainGroup.getName()%>"/>

  <table class='table-blue'>
    <tr>
      <th>Domain</th>
      <th>Include</th>
      <th>Version (default is most recent)</th>
      <th>Current Version</th>
    </tr>
  <%
    DomainGroupVersion latestDomainGroupVersion = DomainGroups.getLatestVersion(domainGroup);
    for (Domain domain : coord.getDomainsSorted()) {
  %>
    <%
      DomainVersion latestVersion = Domains.getLatestVersionNotOpenNotDefunct(domain);
      DomainGroupVersionDomainVersion latestDgvdv = null;
      if (latestDomainGroupVersion != null) {
        latestDgvdv = latestDomainGroupVersion.getDomainVersion(domain);
      }
      boolean included = latestDgvdv != null;
    %>
    <tr id="<%= domain.getId() %>_tr" <%= included ? "class='included'" : "class='not_included'" %> >
      <td>
        <a href="/domain.jsp?n=<%= domain.getName() %>"><%= domain.getName() %></a>
      </td>

        <% if (latestVersion != null) { %>
        <td>
        <input type="checkbox"
               name="<%=domain.getName() %>_included"
               onclick="toggleDisabled('<%= domain.getId() %>_version'); toggleClass('<%= domain.getId() %>_tr', 'included', 'not_included')"
               <%= included ? "checked='checked'" : "" %> />
        </td>
        <td>
          <select size=3 id="<%= domain.getId() %>_version"
             name="<%=domain.getName() %>_version"
             <%= included ? "" : "disabled='disabled'" %> >
             <%
              TreeSet<DomainVersion> revSortedVersions = new TreeSet<DomainVersion>(new ReverseComparator<DomainVersion>());
              revSortedVersions.addAll(domain.getVersions());
              boolean first = true;
             %>
             <% for (DomainVersion v : revSortedVersions) {
                  if (v.getClosedAt() == null || v.isDefunct()) {continue;}
             %>
             <option
              <% if (first) {
                  first = false;
              %>
              selected
              <% } %>
              value=<%=v.getVersionNumber() %>><%= v.getVersionNumber() %>
              (<%= UiUtils.formatDomainVersionClosedAt(v) %>)</option>
             <% } %>
          </select>
        </td>
        <td>
          <%= included ? latestDgvdv.getVersion() : "-" %>
        </td>

        <% } else { %>
          <td>No valid version available.</td><td></td><td></td>
        <% } %>

    </tr>
  <%
  }
  %>

  </table>
  <input type=submit value="Create"/>
  <span style="color: red; font-weight:bold"> (This will likely trigger a data deploy!)</span>
</form>

<h2>Existing Versions</h2>

<table>
  <%
    SortedSet<DomainGroupVersion> dgvRev = new TreeSet<DomainGroupVersion>(new ReverseComparator<DomainGroupVersion>());
    dgvRev.addAll(domainGroup.getVersions());
    for (DomainGroupVersion dgcv : dgvRev) {
  %>
  <tr>
    <td>v<%= dgcv.getVersionNumber() %></td>
    <td>created <%= UiUtils.formatDomainGroupVersionCreatedAt(dgcv) %></td>
  </tr>
  <tr>
    <td colspan=2 style="padding-left: 10px">
      <%= UiUtils.formatDomainGroupVersionTable(dgcv, "table-blue", true) %>
    </td>
  </tr>
  <% } %>
</table>

<jsp:include page="_footer.jsp"/>

</body>
</html>
