<%@ page language="java" contentType="text/html; charset=ISO-8859-1"
    pageEncoding="ISO-8859-1"%>

<%@page import="com.rapleaf.hank.ui.*"%>
<%@page import="com.rapleaf.hank.coordinator.*"%>
<%@page import="java.util.*"%>

<%!public List<RingGroup> ringGroups(Coordinator coord) {
  List<RingGroup> rgcs = new ArrayList<RingGroup>(coord.getRingGroups());
  Collections.sort(rgcs, new RingGroupConfigComparator());
  return rgcs;
}%>
<%
  Coordinator coord = (Coordinator)getServletContext().getAttribute("coordinator");
%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">

<%@page import="java.net.URLEncoder"%><html>
<head>
  <meta http-equiv="Content-Type" content="text/html; charset=ISO-8859-1">
  <title>Hank: Ring Groups</title>

  <jsp:include page="_head.jsp" />
</head>
<body>

  <jsp:include page="_top_nav.jsp" />


  <h1>Ring Groups</h1>
  <form action="/ring_group/create" method=post>
    Add a new ring group: <br/>
    <select name="dgName">
      <%
        for (DomainGroup dgc : coord.getDomainGroups()) {
      %>
      <option><%=dgc.getName()%></option>
      <%
        }
      %>
    </select>
    <input type=text size=30 name="rgName"/> <input type=submit value="Create"/>
  </form>

  <table class='table-blue'>
    <tr>
      <th>Name</th>
      <th>Domain Group</th>
      <th>Status</th>
      <th>Utilities</th>
    </tr>
    <%
      for (RingGroup ringGroup : ringGroups(coord)) {
    %>
      <tr>
        <td><a href="/ring_group.jsp?name=<%= URLEnc.encode(ringGroup.getName()) %>"><%= ringGroup.getName() %></a></td>
        <td><a href="domain_group.jsp?n=<%=URLEnc.encode(ringGroup.getDomainGroup().getName())%>"><%=ringGroup.getDomainGroup().getName()%></a></td>
        <td><%= ringGroup.isUpdating() ? "UPDATING" : "UP" %></td>.getDomainGroup().getName())%>"><%=ringGroup.getDomainGroup().getName()%></a></td>
        <td>
          <form action="/domain_group/delete_domain_group" method=post>
            <input type=hidden name="name" value="<%= ringGroup.getDomainGroup().getName() %>"/>
            <input type=submit value="delete"
             onclick="return confirm('Are you sure you want to delete the domain group <%= ringGroup.getDomainGroup().getName() %>? This action cannot be undone.');"/>
          </form>
        </td>
      </tr>
      <%
    }
    %>
  </table>

</body>
</html>