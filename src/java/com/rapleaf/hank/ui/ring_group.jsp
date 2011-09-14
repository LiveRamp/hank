<%@ page language="java" contentType="text/html; charset=ISO-8859-1"
    pageEncoding="ISO-8859-1"%>

<%@page import="com.rapleaf.hank.coordinator.*"%>
<%@page import="com.rapleaf.hank.generated.*"%>
<%@page import="com.rapleaf.hank.ui.*"%>
<%@page import="com.rapleaf.hank.util.Bytes"%>
<%@page import="java.util.*"%>
<%@page import="java.net.*"%>
<%@page import="java.nio.ByteBuffer"%>
<%@page import="org.apache.commons.lang.StringEscapeUtils"%>
<%!
public List<Ring> sortedRcs(Collection<Ring> rcs) {
  List<Ring> sortedList = new ArrayList<Ring>(rcs);
  Collections.sort(sortedList);
  return sortedList;
}
%>
<%

Coordinator coord = (Coordinator)getServletContext().getAttribute("coordinator");

RingGroup ringGroup = coord.getRingGroup(request.getParameter("name"));

%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">

<%@page import="org.apache.commons.lang.NotImplementedException"%><html>
<head>
  <meta http-equiv="Content-Type" content="text/html; charset=ISO-8859-1">
  <title>Ring Group <%=ringGroup.getName()%></title>

  <jsp:include page="_head.jsp" />
</head>
<body>
  <jsp:include page="_top_nav.jsp" />

  <h1>Ring Group <%=ringGroup.getName()%></h1>

  <div class='box-section'>
    <h3>Status</h3>
    <div class='box-section-content'>
      <div>
        <%
          if(ringGroup.isUpdating()) {
        %>
        An update from version <%=ringGroup.getCurrentVersion()%> to <%=ringGroup.getUpdatingToVersion()%> is in progress.
        <%
          } else {
        %>
        No updates in progress.
        <%
          }
        %>
      </div>
      <div>
        Ring Group Conductor is <%=ringGroup.isRingGroupConductorOnline() ? "online" : "offline"%>
      </div>
    </div>
  </div>

  <div class='box-section'>
    <h3>Configuration</h3>
    <div class='box-section-content'>
      <b>Domain Group:</b> <a href="/domain_group.jsp?n=<%=URLEnc.encode(ringGroup.getDomainGroup().getName())%>"><%=ringGroup.getDomainGroup().getName()%></a>
    </div>
  </div>


<h3>Actions</h3>
  <form action="/ring_group/delete_ring_group" method=post>
    <input type=hidden name="g" value="<%= ringGroup.getName() %>"/>
    <input type=submit value="delete"
      onclick="return confirm('Are you sure you want to delete the ring group <%= ringGroup.getName() %>? This action cannot be undone.');"/>
  </form>


  <div class='box-section'>
  <h3>Rings</h3>
  <div class='box-section-content'>
  <a href="/ring_group/add_ring?g=<%=URLEnc.encode(ringGroup.getName())%>">Add a new ring</a>
  <table class='table-blue'>
    <tr>
      <th>#</th>
      <th>Status</th>
      <th>Cur. Ver.</th>
      <th>Next Ver.</th>
      <th># hosts</th>
      <th></th>
    </tr>
    <%
      for (Ring ring : sortedRcs(ringGroup.getRings())) {
    %>
    <tr>
      <td><%=ring.getRingNumber()%></td>
      <td><%=ring.getState()%></td>
      <td><%=ring.getVersionNumber()%></td>
      <td><%=ring.getUpdatingToVersionNumber()%></td>
      <td><%=ring.getHosts().size()%></td>
      <td>
        <a href="/ring.jsp?g=<%=URLEnc.encode(ringGroup.getName())%>&n=<%=ring.getRingNumber()%>">details</a>
        <form action="/ring_group/delete_ring" method="post">
          <input type="hidden" name="g" value="<%= ringGroup.getName() %>"/>
          <input type="hidden" name="n" value="<%= ring.getRingNumber() %>"/>
          <input type="submit" value="delete" onclick="return confirm('Are you sure you want to delete this ring? This action cannot be undone!');" />
        </form>
      </td>
    </tr>
    <%
      }
    %>
  </table>
  </div>
  </div>

  <div class='box-section'>
    <h3>Query</h3>
    <% if (ringGroup.getCurrentVersion() == null) { %>
      Query disabled because no domain group version is currently deployed!
    <% } else { %>
    <div class='box-section-content'>
      <form action="/ring_group.jsp" method=post>
        <input type=hidden name="name" value="<%=ringGroup.getName()%>"/>

        Domain
        <br/>
        <select name="d">
          <%
            for (DomainGroupVersionDomainVersion dgvdv : ringGroup.getDomainGroup().getVersionByNumber(ringGroup.getCurrentVersion()).getDomainVersions()) {
          %>
          <option<%= request.getParameter("d") != null && URLEnc.decode(request.getParameter("d")).equals(dgvdv.getDomain().getName()) ? " selected" : "" %>>
            <%= dgvdv.getDomain().getName() %>
          </option>
          <% } %>
        </select>
        <br/>
        Key
        <br/>
        <textarea rows=2 cols=30 name="k"><%= request.getParameter("k") == null ? "" : request.getParameter("k") %></textarea>
        <br/>
        in <select name="f">
          <option<%= request.getParameter("f") != null && request.getParameter("f").equals("string") ? " selected" : "" %>>string</option>
          <option<%= request.getParameter("f") != null && request.getParameter("f").equals("hex") ? " selected" : "" %>>hex</option>
        </select>
        <br/>
        <input type=submit value="Get"/>

        <%
        if (request.getParameter("k") != null) {
          %>
        <h4>Result</h4>
          <%
          SmartClient.Iface client = ((IClientCache)getServletContext().getAttribute("clientCache")).getSmartClient(ringGroup);

          ByteBuffer key = null;
          String dataFormat = request.getParameter("f");
          if (dataFormat.equals("hex")) {
            key = Bytes.hexStringToBytes(request.getParameter("k"));
          } else if (dataFormat.equals("string")) {
            key = ByteBuffer.wrap(request.getParameter("k").getBytes("UTF-8"));
          }
          HankResponse hankResponse = client.get(URLEnc.decode(request.getParameter("d")), key);
          if (hankResponse.isSet(HankResponse._Fields.XCEPTION)) {
            // uh oh!
          %>
          <div style="color:red; font-weight: bold">
          <%= StringEscapeUtils.escapeHtml(hankResponse.toString()) %>
          </div>
          <%
          } else if (hankResponse.isSet(HankResponse._Fields.NOT_FOUND)) {
            %>
            <div style="color:red; font-weight: bold">
            Key was not found!
            </div>
            <%
          } else {
          %>
          <div style="font-weight:bold; color:green">Found</div>

          <%
          ByteBuffer valueBuffer = hankResponse.buffer_for_value();
          String valueString = Bytes.bytesToHexString(valueBuffer);
          String[] valueStrings = valueString.split(" ");
          %>

          <table cellspacing=0>
            <tr>
              <td>off</td>
              <td colspan=16 align=center style="border-bottom: 1px solid black"><strong>bytes</strong></td>
              <td width=5 style="border-bottom: 1px solid black">&nbsp;</td>
              <td colspan=16 align=center style="border-bottom: 1px solid black"><strong>string</strong></td>
            </tr>
          <%
          int numBytes = valueStrings.length;
          for (int r = 0; r < (numBytes / 16) + (numBytes % 16 == 0 ? 0 : 1); r++) {
          %>
            <tr>
             <%
             int baseOff = r * 16;
             %>
             <td style="border-right: 1px solid black">0x<%= Integer.toString(baseOff, 16) %></td>
             <%
             for (int off = baseOff; off < baseOff + 16; off++) {
               if (off < numBytes) {
               %><td width=20 align=center><%= valueStrings[off] %></td> <%
               } else {
               %>
               <td width=20>&nbsp;</td>
               <%
               }
             }
             %>
             <td style="border-left: 1px solid black; border-right: 1px solid black"></td>
             <%
             for (int off = baseOff; off < baseOff + 16; off++) {
               if (off < numBytes) {
               %><td width=20 align=center><%= new String(valueBuffer.array(), valueBuffer.position() + off, 1).replaceAll("\\p{Cntrl}", ".") %></td> <%
               } else {
               %>
               <td width=20>&nbsp;</td>
               <%
               }
             }
             %>
            </tr>
          <% } %>
          </table>
          <div style="padding-top:1em;">Raw value in hex:
            <%= valueString %>
          </div>

        <% }}} %>
      </form>
    </div>
  </div>

</body>
</html>
