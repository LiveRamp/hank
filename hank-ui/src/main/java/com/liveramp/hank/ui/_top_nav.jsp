<%@ page import="com.liveramp.hank.coordinator.Coordinator" %>

<%
Coordinator coord = (Coordinator)getServletContext().getAttribute("coordinator");
%>
<div id='top-nav'>
  <a href='index.jsp'><div style='display: inline-block;'>Hank</div></a>
  <a href="domains.jsp"><div style='display: inline-block;'>Domains</div></a>
  <a href="domain_groups.jsp"><div style='display: inline-block;'>Domain Groups</div></a>
  <a href="ring_groups.jsp"><div style='display: inline-block;'>Ring Groups</div></a>

  <div class="coordinator-status">
    <%--technically this could be any implementation, but it's going to be more confusing to the end
    user to say "Coordinator".  can change this when there is a different production Coordinator impl--%>
    ZooKeeper
      <span class="data-<%=coord.getDataState()%>"><b><%=coord.getDataState()%></b></span>
  </div>

</div>


<div id='content'>
