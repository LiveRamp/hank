<%@ page language="java" contentType="text/html; charset=ISO-8859-1"
    pageEncoding="ISO-8859-1"%>


<%@page import="com.rapleaf.hank.ui.*"%>
<%@page import="com.rapleaf.hank.coordinator.*"%>
<%@page import="java.util.*"%>
<%@page import="java.net.*"%>

<%
Coordinator coord = (Coordinator)getServletContext().getAttribute("coordinator");

RingGroup ringGroup = coord.getRingGroup(request.getParameter("g"));

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

  <h1>
  Ring Group <a href="/ring_group.jsp?name=<%=URLEnc.encode(ringGroup.getName())%>"><%=ringGroup.getName()%></a>
  &gt;
  <a href="/ring.jsp?g=<%=URLEnc.encode(ringGroup.getName())%>&n=<%=ring.getRingNumber()%>">Ring <%=ring.getRingNumber()%></a>
  &gt;
  <span class='currentItem'>Partitions Assignment</span>
  </h1>

  <h2>Partition Assignment</h2>

  <%
  int total = 0;
  if (ring.getCurrentVersion() != null) {
    DomainGroupVersion currentVersion = ring.getCurrentVersion();
    if (currentVersion != null && currentVersion.getDomainVersions() != null) {

      for (DomainGroupVersionDomainVersion dc : currentVersion.getDomainVersions()) {
        total += Rings.getUnassignedPartitions(ring, dc.getDomain()).size();
      }
  %>

  <% if (total > 0) { %>
  There are <%=total%> unassigned partitions in <%= currentVersion.getDomainVersions().size() %> domains.
  <% } %>
  <form action="/ring/redistribute_partitions_for_ring" method=post>
    <input type="hidden" name="g" value="<%= ringGroup.getName() %>"/>
    <input type="hidden" name="n" value="<%= ring.getRingNumber() %>"/>
    <input type="submit" value="Assign all unassigned partitions and balance <%= ringGroup.getName() %>"/>
  </form>

  <h2>Assignment visualization</h2>
  <%
  for (DomainGroupVersionDomainVersion d : currentVersion.getDomainVersions()) {
    Set<Integer> unassignedParts = Rings.getUnassignedPartitions(ring, d.getDomain());

    int squareDim = (int)Math.floor(Math.sqrt(d.getDomain().getNumParts()));
  %>
    <div style="float:left; padding:3px">
      <div><%= d.getDomain().getName() %></div>
      <table cellspacing=1 cellpadding=0>
        <% for (int i = 0; i < d.getDomain().getNumParts(); i++) { %>
          <% if (i % squareDim == 0) { %>
          <tr>
          <% } %>

          <td title="<%= i %>" style="font-size: 1pt; width: 3px; height: 3px; background-color: <%= unassignedParts.contains(i) ? "red" : "green" %>">&nbsp;</td>

          <% if (i % squareDim == squareDim - 1) { %>
          </tr>
          <% } %>
        <% } %>
      </table>
    </div>
  <% } %>

  <% } %>
  <% } %>

<jsp:include page="_footer.jsp"/>

</body>
</html>
