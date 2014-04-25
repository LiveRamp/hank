<%@ page language="java" contentType="text/html; charset=ISO-8859-1"
    pageEncoding="ISO-8859-1"%>

<%@page import="java.util.*"%>
<%@page import="com.liveramp.hank.coordinator.*"%>
<%@page import="com.liveramp.hank.partitioner.*"%>
<%@page import="com.liveramp.hank.storage.curly.Curly.Factory"%>

<%
Coordinator coord = (Coordinator)getServletContext().getAttribute("coordinator");

List<StorageEngineFactory> knownStorageEngineFactories = Arrays.asList((StorageEngineFactory)
    new Cueball.Factory(),
    new com.liveramp.hank.storage.curly.Curly.Factory(),
    new com.liveramp.hank.storage.echo.Echo.Factory());

%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">

<%@page import="com.liveramp.hank.storage.StorageEngineFactory"%>
<%@page import="com.liveramp.hank.storage.cueball.Cueball"%><html>
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

    function validateDomainName() {
      var domainName = document.getElementById('domainName');
      if (domainName.value.match(/^ *$/)) {
        alert("Domain names must contain some non-space characters. (Leading and trailing spaces are OK.)");
        return false;
      }
      if (domainName.value.match(/^\./)) {
        alert("Domain names may not start with a '.'!");
        return false;
      }
      return true;
    }
  </script>
</head>
<body onload="resetStorageEngineOptions();">

<jsp:include page="_top_nav.jsp"/>

  <form action="/domain/create" method=post onsubmit="return validateDomainName();">
  <h1>Create New Domain</h1>
  <table>
    <tr>
      <td>Domain Name</td>
      <td><input type=text id="domainName" name="name" size=50 /></td>
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
      <td>Required Host Flags</td>
      <td><input type=text name="requiredHostFlags" size=50 /></td>
    </tr>

    <tr>
      <td style="vertical-align: top">Storage Engine Factory</td>
      <td>
        <div>
          <select id="storageEngineFactorySelect" name="storageEngineFactorySelect"
             onchange="resetStorageEngineOptions();">
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
  <input type=submit value="Create"/>
  </form>

<jsp:include page="_footer.jsp"/>

</body>
</html>
