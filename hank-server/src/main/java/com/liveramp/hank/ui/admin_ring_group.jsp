<%@ page language="java" contentType="text/html; charset=ISO-8859-1"
    pageEncoding="ISO-8859-1"%>

<%@page import="com.liveramp.hank.ui.*"%>
<%@page import="com.liveramp.hank.coordinator.*"%>
<%@page import="java.util.*"%>
<%@page import="java.net.URLEncoder"%>

<%
  Coordinator coord = (Coordinator) getServletContext().getAttribute("coordinator");
  RingGroup ringGroup = coord.getRingGroup(request.getParameter("g"));
%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">

<html>
<head>
  <meta http-equiv="Content-Type" content="text/html; charset=ISO-8859-1">
  <title>Hank: Ring Groups</title>

  <jsp:include page="_head.jsp" />
</head>
<body>

  <jsp:include page="_top_nav.jsp" />

  <h1>
  Manage <span class='currentItem'><%= ringGroup.getName() %></span>
  </h1>

  <a href="/admin.jsp">Back to administration panel</a>

  <h2>Manage Rings</h2>

  <table>
  <% for (Ring ring : ringGroup.getRingsSorted()) { %>
    <tr><td><a href="/admin_ring.jsp?g=<%= ringGroup.getName() %>&n=<%= ring.getRingNumber() %>">Ring <%= ring.getRingNumber() %></a></td></tr>
  <% } %>
  </table>

  <h2>Add New Ring</h2>

  <form action="/ring_group/add_ring">
    <input type="hidden" name="g" value="<%= URLEnc.encode(ringGroup.getName()) %>"/>
    <input type="submit" value="Add New Ring">
  </form>

  <h2>Delete Ring Group</h2>

  <form action="/ring_group/delete_ring_group" method=post>
  <input type="hidden" name="g" value="<%= ringGroup.getName() %>"/>
  <input type=submit value="Delete Ring Group <%= ringGroup.getName() %>"
  onclick="return confirm('Are you sure you want to delete this ring group? This action cannot be undone.');"/>
  </form>

  <jsp:include page="_footer.jsp"/>

</body>
</html>
