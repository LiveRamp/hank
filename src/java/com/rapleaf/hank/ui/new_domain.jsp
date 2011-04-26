<%@ page language="java" contentType="text/html; charset=ISO-8859-1"
    pageEncoding="ISO-8859-1"%>

<%@page import="com.rapleaf.hank.coordinator.*"%>

<%
Coordinator coord = (Coordinator)getServletContext().getAttribute("coordinator");
%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=ISO-8859-1">
<title>New Domain (Hank)</title>
</head>
<body>


<form action="/controller/new_domain" method=post>
<h2>Create New Domain</h2>
<table>
  <tr>
    <td>Domain Name</td>
    <td><input type=text></input></td>
  </tr>
  <tr>
    <td>Partitioner Class Name (fully qualified)</td>
    <td><input type=text></input></td>
  </tr>
  <tr>
    <td>Num Partitions</td>
    <td><input type=text></input></td>
  </tr>
  <tr>
    <td>Storage Engine Factory Class Name (fully qualified)</td>
    <td><input type=text></input></td>
  </tr>
  <tr>
    <td colspan=2>
      Storage Engine Options (<a href="http://www.yaml.org/">YAML</a>)<br/>
      <textarea rows=10 cols=80>---</textarea>
    </td>
  </tr>
</table>
<input type=submit value="Create"/> <a href="domains.jsp">Cancel</a>
</form>

</body>
</html>