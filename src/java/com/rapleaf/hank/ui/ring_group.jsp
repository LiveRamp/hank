<%@ page language="java" contentType="text/html; charset=ISO-8859-1"
    pageEncoding="ISO-8859-1"%>

<%@page import="com.rapleaf.hank.coordinator.*"%>
<%@page import="com.rapleaf.hank.generated.*"%>
<%@page import="com.rapleaf.hank.ui.*"%>
<%@page import="java.util.*"%>
<%@page import="java.net.*"%>
<%@page import="java.nio.ByteBuffer"%>
<%@page import="org.apache.commons.lang.StringEscapeUtils"%>
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

<%@page import="org.apache.commons.lang.NotImplementedException"%><html>
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
      <b>Domain Group:</b> <a href="/domain_group.jsp?n=<%= URLEncoder.encode(ringGroup.getDomainGroupConfig().getName()) %>"><%= ringGroup.getDomainGroupConfig().getName() %></a>
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

  <div class='box-section'>
    <h3>Query</h3>
    <div class='box-section-content'>
      <form action="/ring_group.jsp" method=post>
        <input type=hidden name="name" value="<%= ringGroup.getName() %>"/>

        Domain
        <br/>
        <select name="d">
          <% for (DomainConfig domainConfig : ringGroup.getDomainGroupConfig().getDomainConfigs()) { %>
          <option<%= request.getParameter("d") != null && URLDecoder.decode(request.getParameter("d")).equals(domainConfig.getName()) ? " selected" : "" %>><%= domainConfig.getName() %></option>
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
            String hexString = request.getParameter("k");
            byte[] bytes = new byte[hexString.length()/2];
            for (int i = 0; i < hexString.length(); i+=2) {
              bytes[i/2] = (byte)Integer.valueOf(hexString.substring(i,i+1), 16).intValue();
            }
            key = ByteBuffer.wrap(bytes);
          } else if (dataFormat.equals("string")) {
            key = ByteBuffer.wrap(request.getParameter("k").getBytes("UTF-8"));
          }
          HankResponse hankResponse = client.get(URLDecoder.decode(request.getParameter("d")), key);
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

          <table cellspacing=0>
            <tr>
              <td>off</td>
              <td colspan=16 align=center style="border-bottom: 1px solid black"><strong>bytes</strong></td>
              <td width=5 style="border-bottom: 1px solid black">&nbsp;</td>
              <td colspan=16 align=center style="border-bottom: 1px solid black"><strong>string</strong></td>
            </tr>
          <%
          ByteBuffer value = hankResponse.buffer_for_value();

          for (int r = 0; r < (value.remaining() / 16) + 1; r++) {
          %>
            <tr>
             <%
             int baseOff = r * 16;
             %>
             <td style="border-right: 1px solid black">0x<%= Integer.toString(baseOff, 16) %></td>
             <%
             for (int off = baseOff; off < baseOff + 16; off++) {
               if (off < value.limit() - value.position()) {
               %><td width=20 align=center><%= Integer.toString(value.array()[value.position() + off] | 0x100 & 0x1ff, 16).substring(1) %></td> <%
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
               if (off < value.limit() - value.position()) {
               %><td width=20 align=center><%= new String(value.array(), value.position() + off, 1).replaceAll("\\p{Cntrl}", ".") %></td> <%
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
          
        <% }} %>
      </form>
    </div>
  </div>

</body>
</html>