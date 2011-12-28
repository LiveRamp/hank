<%@ page language="java" contentType="text/html; charset=ISO-8859-1"
    pageEncoding="ISO-8859-1"%>

<%@page import="com.rapleaf.hank.coordinator.*"%>
<%@page import="com.rapleaf.hank.ui.*"%>
<%@page import="com.rapleaf.hank.util.*"%>
<%@page import="java.util.*"%>
<%@page import="org.yaml.snakeyaml.*"%>
<%@page import="org.apache.commons.io.*"%>

<%
  Coordinator coord = (Coordinator)getServletContext().getAttribute("coordinator");

  Domain domain = coord.getDomain(URLEnc.decode(request.getParameter("n")));
%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">

<%@page import="org.yaml.snakeyaml.Yaml"%><html>
<head>
  <jsp:include page="_head.jsp" />
  <meta http-equiv="Content-Type" content="text/html; charset=ISO-8859-1">
  <title>Domain: <%= domain.getName() %></title>
</head>
<body>

<jsp:include page="_top_nav.jsp"/>

<h1>Domain <span class='currentItem'><%= domain.getName() %></span></h1>

<table>
  <tr>
    <td>ID:</td>
    <td><%= domain.getId() %></td>
  </tr>
  <tr>
    <td>
      Number of partitions:
    </td>
    <td>
      <%= domain.getNumParts() %>
    </td>
  </tr>
  <tr>
    <td>
      Partitioner:
    </td>
    <td>
      <%= domain.getPartitioner().getClass().getName() %>
    </td>
  </tr>
  <tr>
    <td>
      Storage engine factory:
    </td>
    <td>
      <%= domain.getStorageEngineFactoryClass().getName() %>
    </td>
  </tr>
  <tr valign=top>
    <td>
      Storage engine factory options:
    </td>
    <td>
      <form action="/domain/update" method=post>
        <input type=hidden name="name" value="<%= domain.getName() %>"/>
        <%
        DumperOptions opts = new DumperOptions();
        opts.setExplicitStart(true);
        opts.setDefaultScalarStyle(DumperOptions.ScalarStyle.PLAIN);
        opts.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
        %>
        <textarea rows="10" cols="80" id="storageEngineOptions" name="storageEngineOptions"><%= domain.getStorageEngineOptions() == null
          ? ""
          : new Yaml(opts).dump(domain.getStorageEngineOptions())
          %>
        </textarea>
        <br/>
        <input type=submit value="Save modified storage engine options"/>
      </form>
    </td>
  </tr>
</table>

<h2>Actions</h2>
<form action="/domain/delete" method=post>
  <% if (request.getParameter("used_in_dg") != null) { %>
  <p style="font-weight: bold; color: red">
    Could not delete domain - still assigned to Domain Group <a href="/domain_group.jsp?n=<%= request.getParameter("used_in_dg") %>"><%= request.getParameter("used_in_dg") %></a>. Unassign it first.
  </p>
  <% } %>
  <input type=hidden name="name" value="<%= domain.getName() %>"/>
  <input type=submit value="Delete this domain"
   onclick="return confirm('Are you sure you want to delete the domain <%= domain.getName() %>? This action cannot be undone.');"/>
</form>

<h2>Versions</h2>

<div>
  Total of <%= FileUtils.byteCountToDisplaySize(Domains.getTotalNumBytes(domain)) %> in <%= domain.getVersions().size() %> versions.
</div>

<table class='table-blue'>
  <tr>
    <th>Version Number</th>
    <th>Properties</th>
    <th>State</th>
    <th>Size</th>
    <th>Number of Records</th>
    <th></th>
  </tr>
  <%
    SortedSet<DomainVersion> revSorted = new TreeSet<DomainVersion>(new ReverseComparator<DomainVersion>());
    revSorted.addAll(domain.getVersions());
  %>
  <%
    for (DomainVersion version : revSorted) {
  %>
  <tr class="<%= version.isDefunct() ? "defunct-version" : "" %>">
    <td><%= version.getVersionNumber() %></td>
    <td><%= version.getProperties() %></td>
    <td>
      <% if (DomainVersions.isClosed(version)) { %>
      Closed
      <%   if (version.isDefunct()) { %>
      DEFUNCT
      <%   } %>
      <%= new Date(version.getClosedAt())%>
      <% } else { %>
      Open - <%= version.getPartitionProperties().size() %>/<%= domain.getNumParts() %> complete
      <% } %>
    </td>
    <td><%= FileUtils.byteCountToDisplaySize(DomainVersions.getTotalNumBytes(version)) %></td>
    <td><%= String.format("%,d", DomainVersions.getTotalNumRecords(version)) %></td>
    <td>
      <% if (DomainVersions.isClosed(version) && !version.isDefunct()) { %>
      <form action="/domain/defunctify" method="post">
        <input type=hidden name="n" value="<%= domain.getName() %>" />
        <input type=hidden name="ver" value="<%= version.getVersionNumber() %>" />
        <input type=submit value="Mark defunct"
          onclick="return confirm('Are you sure you want to mark version <%= version.getVersionNumber() %> defunct? Subsequent data deploys will skip this version.');"/>
      </form>
      <% } %>

      <% if (DomainVersions.isClosed(version) && version.isDefunct()) { %>
      <form action="/domain/undefunctify" method="post">
        <input type=hidden name="n" value="<%= domain.getName() %>" />
        <input type=hidden name="ver" value="<%= version.getVersionNumber() %>" />
        <input type=submit value="Unmark defunct"
          onclick="return confirm('Are you sure you want to mark version <%= version.getVersionNumber() %> NOT defunct? Subsequent data deploys will use this version.');"/>
      </form>
      <% } %>

      <% if (!DomainVersions.isClosed(version)) { %>
      <form action="/domain/close" method="post">
        <input type=hidden name="n" value="<%= domain.getName() %>" />
        <input type=hidden name="ver" value="<%= version.getVersionNumber() %>" />
        <input type=submit value="Close"
          onclick="return confirm('Are you sure you want to close version <%= version.getVersionNumber() %>? This can have adverse effects if done prematurely.');" />
      </form>
      <% } %>

      <% if (DomainVersions.isClosed(version) && !version.isDefunct()) { %>
      <form action="/domain/cleanup" method="post">
        <input type=hidden name="n" value="<%= domain.getName() %>" />
        <input type=hidden name="ver" value="<%= version.getVersionNumber() %>" />
        <input type=submit value="Delete from remote storage"
          onclick="return confirm('Are you sure you want to delete version <%= version.getVersionNumber() %> from remote storage? This action cannot be undone!');"/>
      </form>
      <% } %>
    </td>
  </tr>
  <% } %>
</table>

<jsp:include page="_footer.jsp"/>

</body>
</html>
