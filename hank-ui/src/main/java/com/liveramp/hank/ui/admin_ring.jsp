<%@ page language="java" contentType="text/html; charset=ISO-8859-1"
    pageEncoding="ISO-8859-1"%>

<%@page import="com.liveramp.hank.ui.*"%>
<%@page import="com.liveramp.hank.coordinator.*"%>
<%@page import="java.util.*"%>
<%@page import="java.net.URLEncoder"%>

<%
  Coordinator coord = (Coordinator) getServletContext().getAttribute("coordinator");
RingGroup ringGroup = coord.getRingGroup(URLEnc.decode(request.getParameter("g")));
Ring ring = ringGroup.getRing(Integer.parseInt(request.getParameter("n")));
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
  Manage Ring Group <a href="/admin_ring_group.jsp?g=<%=URLEnc.encode(ringGroup.getName())%>"><%=ringGroup.getName()%></a>
  &gt;
  <span class='currentItem'>Ring <%=ring.getRingNumber()%></span>
  </h1>

  <h2>Manage Hosts</h2>

  <table>
  <% for (Host host : ring.getHostsSorted()) { %>
    <tr><td><a href="/admin_host.jsp?g=<%= ringGroup.getName() %>&r=<%= ring.getRingNumber() %>&h=<%= host.getAddress() %>"><%= host.getAddress() %></a></td></tr>
  <% } %>
  </table>

  <h2>Add New Host</h2>

  <form action="/ring/add_host" method=post>
    <input type=hidden name="rgName" value="<%=ringGroup.getName()%>"/>
    <input type=hidden name="ringNum" value="<%=ring.getRingNumber()%>"/>
    <table>
      <tr>
        <td>Host:</td>
        <td><input type=text size=30 name="hostname"/></td>
      </tr>
      <tr>
        <td>Port:</td>
        <td><input type=text size=5 name="port" /></td>
      </tr>
      <tr>
        <td>Host Flags:</td>
        <td><input type=text size=50 name="hostFlags" /></td>
      </tr>
    </table>
    <input type=submit value="Add New Host"/>
  </form>

  <h2>Delete Ring</h2>

  <form action="/ring_group/delete_ring" method="post">
    <input type="hidden" name="g" value="<%= ringGroup.getName() %>"/>
    <input type="hidden" name="n" value="<%= ring.getRingNumber() %>"/>
    <input type="submit" value="Delete Ring <%= ring.getRingNumber() %>" onclick="return confirm('Are you sure you want to delete this ring? This action cannot be undone!');" />
  </form>

  <jsp:include page="_footer.jsp"/>

</body>
</html>
