<%@ page language="java" contentType="text/html; charset=ISO-8859-1"
    pageEncoding="ISO-8859-1" %>

<%@ page import="com.liveramp.hank.coordinator.*" %>
<%@ page import="com.liveramp.hank.Hank" %>
<%
Coordinator coord = (Coordinator)getServletContext().getAttribute("coordinator");
%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
  <meta http-equiv="Content-Type" content="text/html; charset=ISO-8859-1">
  <title>Hank: Home</title>

  <jsp:include page='_head.jsp' />
</head>
<body>

  <jsp:include page='_top_nav.jsp' />

  <h1>Hank</h1>

  <h2>System Summary</h2>

  <%= coord.getDomains().size() %> <a href='domains.jsp'>domains</a>,
  <%= coord.getDomainGroups().size() %> <a href='domain_groups.jsp'>domain groups</a>,
  and <%= coord.getRingGroups().size() %> <a href='ring_groups.jsp'>ring groups</a>.

  <h2>Coordinator</h2>
  <div style="font-family: courier new">
    <%= coord %>
  </div>

  <h2>Administration</h2>
  <a href='admin.jsp'><div style='display: inline-block;'>Open administration panel</div></a>

  <h2>Version Information</h2>
  <div>
    Hank version <%= Hank.getVersion() %>, commit <%= Hank.getGitCommit() %>
  </div>
  <div>
    Please report bugs on <a href="https://github.com/LiveRamp/hank/issues">GitHub issues page</a>.
  </div>

<jsp:include page="_footer.jsp"/>

</body>
</html>
