<%@ page language="java" contentType="text/html; charset=ISO-8859-1"
    pageEncoding="ISO-8859-1"%>

<%@page import="com.liveramp.hank.coordinator.*"%>
<%@page import="com.liveramp.hank.ui.*"%>
<%@page import="java.util.*"%>

<%
Coordinator coord = (Coordinator)getServletContext().getAttribute("coordinator");
%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
  <meta http-equiv="Content-Type" content="text/html; charset=ISO-8859-1">
  <title>Hank: Domain Groups</title>

  <script type="text/javascript">
    function validateCreate() {
      var domainGroupName = document.getElementById('name');
      if (domainGroupName.value.match(/^ *$/)) {
        alert("Domain group names must contain some non-space characters. (Leading and trailing spaces are OK.)");
        return false;
      }
      if (domainGroupName.value.match(/^\./)) {
        alert("Domain names may not start with a '.'!");
        return false;
      }
      return true;
    }
  </script>

  <jsp:include page="_head.jsp" />
</head>
<body>

  <jsp:include page="_top_nav.jsp" />

  <h1>Domain Groups</h1>

  <table class='table-blue-compact'>
    <tr>
      <th>Domain Group</th>
    </tr>
    <%
      for (DomainGroup domainGroup : coord.getDomainGroupsSorted()) {
    %>
      <tr>
        <td class='centered'>
        <%= UiUtils.formatDomainGroupInfoTooltip(domainGroup,
        "<a href='/domain_group.jsp?n=" + URLEnc.encode(domainGroup.getName()) +
        "'>" + domainGroup.getName() + "</a>") %>
        </td>
      </tr>
      <%
    }
    %>
  </table>

<jsp:include page="_footer.jsp"/>

</body>
</html>
