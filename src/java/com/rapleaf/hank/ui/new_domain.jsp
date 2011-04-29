<%@ page language="java" contentType="text/html; charset=ISO-8859-1"
    pageEncoding="ISO-8859-1"%>

<%@page import="com.rapleaf.hank.coordinator.*"%>
<%@page import="com.rapleaf.hank.partitioner.Murmur64Partitioner"%>
<%@page import="com.rapleaf.hank.storage.curly.Curly.Factory"%>

<%
Coordinator coord = (Coordinator)getServletContext().getAttribute("coordinator");
%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
  <meta http-equiv="Content-Type" content="text/html; charset=ISO-8859-1">
  <title>New Domain (Hank)</title>

  <jsp:include page="head.jsp" />
</head>
<body>

  <form action="/domain/create" method=post>
  <h2>Create New Domain</h2>
  <table>
    <tr>
      <td>Domain Name</td>
      <td><input type=text name="name" size=50 /></td>
    </tr>
    <tr>
      <td>Partitioner Class Name (fully qualified)</td>
      <td><input type=text name="partitionerName" size=50 value="<%= Murmur64Partitioner.class.getName() %>"/></td>
    </tr>
    <tr>
      <td>Num Partitions</td>
      <td><input type=text name="numParts" size=50 /></td>
    </tr>
    <tr>
      <td>Storage Engine Factory Class Name (fully qualified)</td>
      <td><input type=text name="storageEngineFactoryName" size=50 value="<%= Factory.class.getName() %>" /></td>
    </tr>
    <tr>
      <td colspan=2>
        Storage Engine Options (<a href="http://www.yaml.org/">YAML</a>)<br/>
        <textarea rows=10 cols=80 name="storageEngineOptions">---</textarea>
      </td>
    </tr>
  </table>
  <input type=submit value="Create"/> <a href="domains.jsp">Cancel</a>
  </form>

</body>
</html>