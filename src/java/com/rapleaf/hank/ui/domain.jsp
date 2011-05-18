<%@ page language="java" contentType="text/html; charset=ISO-8859-1"
    pageEncoding="ISO-8859-1"%>

<%@page import="com.rapleaf.hank.coordinator.*"%>
<%@page import="com.rapleaf.hank.ui.*"%>
<%@page import="com.rapleaf.hank.util.*"%>
<%@page import="java.util.*"%>
<%@page import="org.yaml.snakeyaml.*"%>

<%
Coordinator coord = (Coordinator)getServletContext().getAttribute("coordinator");

DomainConfig domainConfig = coord.getDomainConfig(URLEnc.decode(request.getParameter("n")));
%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">

<%@page import="org.yaml.snakeyaml.Yaml"%><html>
<head>
  <jsp:include page="_head.jsp" />
  <meta http-equiv="Content-Type" content="text/html; charset=ISO-8859-1">
  <title>Domain: <%= domainConfig.getName() %></title>
</head>
<body>

<jsp:include page="_top_nav.jsp"/>

<h1>Domain <%= domainConfig.getName() %></h1>

<table>
  <tr>
    <td>
      Number of partitions:
    </td>
    <td>
      <%= domainConfig.getNumParts() %>
    </td>
  </tr>
  <tr>
    <td>
      Partitioner:
    </td>
    <td>
      <%= domainConfig.getPartitioner().getClass().getName() %>
    </td>
  </tr>
  <tr>
    <td>
      Storage engine factory:
    </td>
    <td>
      <%= domainConfig.getStorageEngineFactoryClass().getName() %>
    </td>
  </tr>
  <tr valign=top>
    <td>
      Storage engine factory options:
    </td>
    <td>
      <% 
      DumperOptions opts = new DumperOptions();
      opts.setExplicitStart(true);
      opts.setDefaultScalarStyle(DumperOptions.ScalarStyle.PLAIN);
      opts.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
      %>
      <textarea rows="10" cols="80" disabled=true><%= domainConfig.getStorageEngineOptions() == null 
          ? ""
          : new Yaml(opts).dump(domainConfig.getStorageEngineOptions()) 
          %>
      </textarea>
    </td>
  </tr>
</table>

<h3>Versions</h3>

<% if (domainConfig.getOpenVersionNumber() == null) { %>
No open version.
<% } else { %>
Version #<%= domainConfig.getOpenVersionNumber() %> is currently open.
<% } %>

<table class='table-blue'>
  <tr>
    <th>#</th>
    <th>closed at</th>
  </tr>
  <%
  SortedSet<DomainVersionConfig> revSorted = new TreeSet<DomainVersionConfig>(new ReverseComparator<DomainVersionConfig>());
  revSorted.addAll(domainConfig.getVersions());
  %>
  <% for (DomainVersionConfig version : revSorted) { %>
  <tr>
    <td><%= version.getVersionNumber() %></td>
    <td><%= new Date(version.getClosedAt()) %></td>
  </tr>
  <% } %>
</table>

</body>
</html>