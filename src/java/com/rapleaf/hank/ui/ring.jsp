<%@ page language="java" contentType="text/html; charset=ISO-8859-1"
    pageEncoding="ISO-8859-1"%>


<%@page import="com.rapleaf.hank.ui.*"%>
<%@page import="com.rapleaf.hank.coordinator.*"%>
<%@page import="java.util.*"%>
<%@page import="java.net.*"%>
<%@page import="java.text.DecimalFormat" %>

<%!public List<Host> sortedHosts(Collection<Host> hosts) {
  if (hosts == null) {
    return null;
  }
  List<Host> sortedList = new ArrayList<Host>(hosts);
  Collections.sort(sortedList);
  return sortedList;
}%>

<%
Coordinator coord = (Coordinator)getServletContext().getAttribute("coordinator");

RingGroup ringGroup = coord.getRingGroup(URLEnc.decode(request.getParameter("g")));

Ring ring = ringGroup.getRing(Integer.parseInt(request.getParameter("n")));
%>

<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
  <meta http-equiv="Content-Type" content="text/html; charset=ISO-8859-1">
  <title>Ring <%=ring.getRingNumber()%> in group <%=ringGroup.getName()%></title>

  <jsp:include page="_head.jsp" />
</head>
<body>

  <jsp:include page="_top_nav.jsp" />

  <h1>Ring <%=ring.getRingNumber()%> in group <a href="/ring_group.jsp?name=<%=URLEnc.encode(ringGroup.getName())%>"><%=ringGroup.getName()%></a></h1>

  <div>
    Ring status: <%=ring.getState()%> <br/>
    <%
      if (Rings.isUpdatePending(ring)) {
    %>
    Currently updating from <%=ring.getVersionNumber()%> to <%=ring.getUpdatingToVersionNumber()%>
    <%
      } else {
    %>
    Currently up to date.
    <%
      }
    %>
  </div>

  <h3>Hosts</h3>

  <form action="/ring/add_host" method=post>
    Add a new host: <br/>
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
    </table>
    <input type=submit value="Add"/>
  </form>

  <form action="/ring/command_all" method=post>
    Command all hosts: <br/>
    <input type=hidden name="rgName" value="<%=ringGroup.getName()%>"/>
    <input type=hidden name="ringNum" value="<%=ring.getRingNumber()%>"/>
    <select name="command">
      <option></option>
      <% for (HostCommand cmd : HostCommand.values()) { %>
      <option value="<%= cmd.name() %>"><%= cmd.name() %></option>
      <% } %>
    </select>
    <input type=submit value="Command"/>
  </form>

  <table width=800 class='table-blue'>
    <tr>
      <th><strong>Address</strong></th>
      <th><strong>Status</strong></th>
      <th></th>
      <th></th>
      <th><strong>Cur. Cmd.</strong></th>
      <th><strong>Queue</strong></th>
      <th><strong>Actions</strong></th>
    </tr>
    <%
      Collection<Host> hosts = sortedHosts(ring.getHosts());
      if (hosts != null) {
        for(Host host : hosts) {
    %>
    <tr>
      <td><a href="/host.jsp?g=<%= ringGroup.getName() %>&r=<%= ring.getRingNumber() %>&h=<%= URLEnc.encode(host.getAddress().toString()) %>"><%= host.getAddress() %></a></td>
      <td>
      <%= host.getState() %>
      </td>
      <%
      UpdateProgress progress = null;
      if (Rings.isUpdatePending(ring)) {
        DomainGroupVersion domainGroupVersion = ringGroup.getDomainGroup().getVersionByNumber(ring.getUpdatingToVersionNumber());
        if (domainGroupVersion != null) {
          progress = Hosts.computeUpdateProgress(host, domainGroupVersion);
        }
      }
      %>
      <% if (progress != null) { %>
      <td>
        <div class='progress-bar'>
          <div class='progress-bar-filler' style='width: <%= Math.round(progress.getUpdateProgress() * 100) %>%'></div>
        </div>
      </td>
      <td>
        <%= new DecimalFormat("#.##").format(progress.getUpdateProgress() * 100) %>% updated
        (<%= progress.getNumPartitionsUpToDate() %>/<%= progress.getNumPartitions() %>)
      </td>
      <% } else { %>
      <td></td>
      <td></td>
      <% } %>
      <td><%= host.getCurrentCommand() %></td>
      <td><%= host.getCommandQueue() %></td>
      <td>
        <form method=post action="/ring/delete_host" id="remove_form_<%= host.getAddress().getHostName() %>__<%= host.getAddress().getPortNumber() %>">
          <input type=hidden name="g" value="<%= ringGroup.getName() %>"/>
          <input type=hidden name="n" value="<%= ring.getRingNumber() %>"/>
          <input type=hidden name="h" value="<%= host.getAddress() %>"/>
          <a href="javascript:if (confirm('Are you sure you want to delete this host? This action cannot be undone!')) document.forms['remove_form_<%= host.getAddress().getHostName() %>__<%= host.getAddress().getPortNumber() %>'].submit();">remove from ring</a>
        </form>
      </td>
    </tr>
    <%
        }
      }
    %>
  </table>

  <h3>Utilities</h3>

  <a href="/ring_partitions.jsp?g=<%=URLEnc.encode(ringGroup.getName())%>&n=<%=ring.getRingNumber()%>">Partition Assignment</a>

</body>
</html>
