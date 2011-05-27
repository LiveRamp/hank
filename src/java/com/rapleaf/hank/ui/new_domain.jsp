<%@ page language="java" contentType="text/html; charset=ISO-8859-1"
    pageEncoding="ISO-8859-1"%>

<%@page import="java.util.*"%>
<%@page import="com.rapleaf.hank.coordinator.*"%>
<%@page import="com.rapleaf.hank.partitioner.*"%>
<%@page import="com.rapleaf.hank.storage.curly.Curly.Factory"%>

<%
Coordinator coord = (Coordinator)getServletContext().getAttribute("coordinator");

List<StorageEngineFactory> knownStorageEngineFactories = Arrays.asList((StorageEngineFactory)new Cueball.Factory(), new com.rapleaf.hank.storage.curly.Curly.Factory());

%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">

<%@page import="com.rapleaf.hank.storage.StorageEngineFactory"%>
<%@page import="com.rapleaf.hank.storage.cueball.Cueball"%><html>
<head>
  <meta http-equiv="Content-Type" content="text/html; charset=ISO-8859-1">
  <title>New Domain (Hank)</title>

  <jsp:include page="_head.jsp" />

  <script type="text/javascript">
    function getDefaultOptionsText(klass) {
      <% for (StorageEngineFactory factory : knownStorageEngineFactories) {%>
      if (klass == "<%= factory.getClass().getName() %>") {
        return "<%= factory.getDefaultOptions().replace("\n", "\\n\\\n") %>";
      }
      <%}%>
      return "---";
    }

    function resetStorageEngineOptions() {
      var storageEngineOptions = document.getElementById('storageEngineOptions');
      var storageEngineFactoryName = document.getElementById('storageEngineFactorySelect');
      var newOpts = getDefaultOptionsText(storageEngineFactoryName.value);
      storageEngineOptions.value = newOpts;
    }
  </script>
</head>
<body onload="resetStorageEngineOptions();">

  <form action="/domain/create" method=post>
  <h2>Create New Domain</h2>
  <table>
    <tr>
      <td>Domain Name</td>
      <td><input type=text name="name" size=50 /></td>
    </tr>
    <tr>
      <td>Num Partitions</td>
      <td><input type=text name="numParts" size=50 value="1024"/></td>
    </tr>
    <tr>
      <td style="vertical-align: top">Partitioner</td>
      <td>
        <div>
          <select id="partitionerSelect" name="partitionerSelect">
            <% for (Class<? extends Partitioner> klass : Arrays.asList((Class<? extends Partitioner>)Murmur64Partitioner.class)) { %>
            <option value="<%= klass.getName() %>"><%= klass.getSimpleName() %></option>
            <% } %>
            <option value="__other__">Other (specify fully qualified class name below)</option>
          </select>
        </div>
        <div>
          <input type=text name="partitionerOther" size=50/>
        </div>
      </td>
    </tr>
    <tr>
      <td style="vertical-align: top">Storage Engine Factory</td>
      <td>
        <div>
          <select id="storageEngineFactorySelect" name="storageEngineFactorySelect"
             onchange="resetStorageEngineOptions();"
          >
            <% 
            for (StorageEngineFactory factory : knownStorageEngineFactories) {
            %>
            <option value="<%= factory.getClass().getName() %>"><%= factory.getPrettyName() %></option>
            <% } %>
            <option value="__other__">Other (specify fully qualified class name below)</option>
          </select>
        </div>
        <div>
          <input type=text name="storageEngineFactoryName" size=50 />
        </div>
      </td>
    </tr>
    <tr>
      <td colspan=2>
        Storage Engine Options (<a href="http://www.yaml.org/">YAML</a>)<br/>
        <textarea rows=30 cols=80 id="storageEngineOptions" name="storageEngineOptions">---</textarea>
      </td>
    </tr>
  </table>
  <input type=submit value="Create"/> <a href="domains.jsp">Cancel</a>
  </form>

</body>
</html>