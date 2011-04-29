<%@ page language="java" contentType="text/html; charset=ISO-8859-1"
    pageEncoding="ISO-8859-1"%>

<%@page import="com.rapleaf.hank.coordinator.*"%>
<%@page import="java.util.*"%>
<%@page import="java.net.*"%>
<%!

public List<RingConfig> sortedRcs(Collection<RingConfig> rcs) {
  List<RingConfig> sortedList = new ArrayList<RingConfig>(rcs);
  Collections.sort(sortedList, new RingConfigComparator());
  return sortedList;
}
%>
<%

Coordinator coord = (Coordinator)getServletContext().getAttribute("coordinator");

RingGroupConfig ringGroup = coord.getRingGroupConfig(request.getParameter("name"));

%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
  <meta http-equiv="Content-Type" content="text/html; charset=ISO-8859-1">
  <title>Ring Group <%= ringGroup.getName() %></title>

  <jsp:include page="_head.jsp" />
</head>
<body>
  
  <jsp:include page="_top_nav.jsp" />
  

  <h1>Ring Group <%= ringGroup.getName() %></h1>
  
  <div class='box-section'>
  	<h3>Status</h3>
  	<div class='box-section-content'>
  	  overall status blob
    </div>
  </div>
  
  <div class='box-section'>
  	<h3>Configuration</h3>
  	<div class='box-section-content'>
      <b>Domain Group:</b> <a href="">(domain group name)</a>
    </div>
  </div>


  <div class='box-section'>
  <h3>Rings</h3>
  <div class='box-section-content'>
  <a href="/ring_group/add_ring?g=<%= URLEncoder.encode(ringGroup.getName()) %>">Add a new ring group</a>
  <table class='table-blue'>
    <tr>
      <th>#</th>
      <th>Status</th>
      <th>Cur. Ver.</th>
      <th>Next Ver.</th>
      <th># hosts</th>
      <th></th>
    </tr>
    <% for (RingConfig ring : sortedRcs(ringGroup.getRingConfigs())) { %>
    <tr>
      <td><%= ring.getRingNumber() %></td>
      <td><%= ring.getState() %></td>
      <td><%= ring.getVersionNumber() %></td>
      <td><%= ring.getUpdatingToVersionNumber() %></td>
      <td><%= ring.getHosts().size() %></td>
      <td><a href="/ring.jsp?g=<%= URLEncoder.encode(ringGroup.getName()) %>&n=<%= ring.getRingNumber() %>">details</a></td>
    </tr>
    <% } %>
  </table>
  </div>
  </div>
  
</body>
</html>