<%@ page language="java" contentType="text/html; charset=ISO-8859-1"
    pageEncoding="ISO-8859-1"%>
    

<%@page import="com.rapleaf.hank.coordinator.*"%>
<%@page import="java.util.*"%>
<%@page import="java.net.*"%>

<%!
public List<HostConfig> sortedHcs(Collection<HostConfig> rcs) {
  List<HostConfig> sortedList = new ArrayList<HostConfig>(rcs);
  Collections.sort(sortedList, new HostConfigComparator());
  return sortedList;
}
%>

<%
Coordinator coord = (Coordinator)getServletContext().getAttribute("coordinator");

RingGroupConfig ringGroup = coord.getRingGroupConfig(request.getParameter("g"));

RingConfig ring = ringGroup.getRingConfig(Integer.parseInt(request.getParameter("n")));
%>

<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
  <meta http-equiv="Content-Type" content="text/html; charset=ISO-8859-1">
  <title>Ring <%= ring.getRingNumber() %> in group <%= ringGroup.getName() %></title>

  <jsp:include page="_head.jsp" />
</head>
<body>

  <jsp:include page="_top_nav.jsp" />

  <h1>Ring <%= ring.getRingNumber() %> in group <a href="/ring_group.jsp?name=<%= URLEncoder.encode(ringGroup.getName()) %>"><%= ringGroup.getName() %></a></h1>

  <div>
    Ring status: <%= ring.getState() %> <br/>
    <% if (ring.isUpdatePending()) { %>
    Currently updating from <%= ring.getVersionNumber() %> to <%= ring.getUpdatingToVersionNumber() %>
    <% } else { %>
    Currently up to date.
    <% } %>
  </div>

  <h3>Hosts</h3>

  <form action="/ring/add_host" method=post>
    Add a new host: <br/>
    <input type=hidden name="rgName" value="<%= ringGroup.getName() %>"/>
    <input type=hidden name="ringNum" value="<%= ring.getRingNumber() %>"/>
    Host:
    <input type=text size=30 name="hostname"/>
    <br/>
    Port:
    <input type=text size=5 name="port" />
    <br/>
    <input type=submit value="Add"/>
  </form>

  <table width=500>
    <tr>
      <td><strong>Address</strong></td>
      <td><strong>Status</strong></td>
      <td><strong>Cur. Cmd.</strong></td>
      <td><strong>Queue</strong></td>
    </tr>
    <% for(HostConfig host : sortedHcs(ring.getHosts())) { %>
    <tr>
      <td><a href="/host.jsp?g=<%= ringGroup.getName() %>&r=<%= ring.getRingNumber() %>&h=<%= URLEncoder.encode(host.getAddress().toString()) %>"><%= host.getAddress() %></a></td>
      <td><%= host.getState() %></td>
      <td><%= host.getCurrentCommand() %></td>
      <td><%= host.getCommandQueue() %></td>
      <td>
        <form method=post action="/ring/delete_host" id="remove_form_<%= host.getAddress().getHostName() %>__<%= host.getAddress().getPortNumber() %>">
          <input type=hidden name="g" value="<%= ringGroup.getName() %>"/>
          <input type=hidden name="n" value="<%= ring.getRingNumber() %>"/>
          <input type=hidden name="h" value="<%= host.getAddress() %>"/>
          <a href="javascript:document.forms['remove_form_<%= host.getAddress().getHostName() %>__<%= host.getAddress().getPortNumber() %>'].submit();">remove from ring</a>
        </form>
      </td>
    </tr>
    <% } %>
  </table>

</body>
</html>