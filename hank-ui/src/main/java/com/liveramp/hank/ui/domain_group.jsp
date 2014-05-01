<%@ page language="java" contentType="text/html; charset=ISO-8859-1"
    pageEncoding="ISO-8859-1"%>

<%@page import="com.liveramp.hank.coordinator.*"%>
<%@page import="com.liveramp.hank.ui.*"%>
<%@page import="com.liveramp.hank.util.*"%>
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

<h2>Current Domain Versions</h2>

<%= UiUtils.formatDomainGroupDomainVersionsTable(domainGroup, "table-blue-compact", true) %>

<h2>Update Domain Versions</h2>

<form method="post" action="/domain_group/update_domain_versions">
  <input type=hidden name="name" value="<%=domainGroup.getName()%>"/>

  <table class='table-blue-compact'>
    <tr>
      <th>Domain</th>
      <th>Include</th>
      <th>Version</th>
      <th>Current Version</th>
      <th>Latest Version</th>
    </tr>
  <%
    // Separate included domains and the other domains for display
    List<Domain> includedDomains = new ArrayList<Domain>();
    List<Domain> notIncludedDomains = new ArrayList<Domain>();
    for (Domain domain : coord.getDomainsSorted()) {
      if (domainGroup.getDomainVersion(domain) != null) {
        includedDomains.add(domain);
      } else {
        notIncludedDomains.add(domain);
      }
    }
    List<Domain> allDomains = new ArrayList<Domain>(includedDomains.size() + notIncludedDomains.size());
    allDomains.addAll(includedDomains);
    allDomains.addAll(notIncludedDomains);

    for (Domain domain : allDomains) {
  %>
    <%
      DomainAndVersion currentDomainVersion = domainGroup.getDomainVersion(domain);
      boolean included = currentDomainVersion != null;
    %>
    <tr id="<%= domain.getId() %>_tr" <%= included ? "class='included'" : "class='not_included'" %> >
      <td class='centered'>
        <a href="/domain.jsp?n=<%= domain.getName() %>"><%= domain.getName() %></a>
      </td>

        <td class='centered'>
        <input type="checkbox"
               name="<%=domain.getName() %>_included"
               onclick="toggleDisabled('<%= domain.getId() %>_version'); toggleClass('<%= domain.getId() %>_tr', 'included', 'not_included')"
               <%= included ? "checked='checked'" : "" %> />
        </td>
        <td class='centered'>
          <select size=3 id="<%= domain.getId() %>_version"
             name="<%=domain.getName() %>_version"
             <%= included ? "" : "disabled='disabled'" %> >
             <%
              DomainVersion latestVersion = null;
              TreeSet<DomainVersion> revSortedVersions = new TreeSet<DomainVersion>(new ReverseComparator<DomainVersion>());
              revSortedVersions.addAll(domain.getVersions());
             %>
             <%
              for (DomainVersion v : revSortedVersions) {
                if (latestVersion == null) {
                  latestVersion = v;
                }
             %>
             <option
              <% if (included && currentDomainVersion.getVersionNumber() == v.getVersionNumber()) { %>
              selected
              <% } else if (v.getClosedAt() == null || v.isDefunct()) { %>
              disabled
              <% } %>
              value=<%=v.getVersionNumber() %>><%= v.getVersionNumber() %>
              (<%= UiUtils.formatDomainVersionClosedAt(v) %>)</option>
             <% } %>
          </select>
        </td>
        <td class='centered'>
          <%= included ? currentDomainVersion.getVersionNumber() : "-" %>
        </td>
        <td class='centered'>
          <%= latestVersion != null ? latestVersion.getVersionNumber() : "-" %>
        </td>

    </tr>
  <%
  }
  %>
  </table>
  <input type=submit value="Update Domain Versions"/>
</form>

<jsp:include page="_footer.jsp"/>

</body>
</html>
