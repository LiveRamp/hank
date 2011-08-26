package com.rapleaf.hank.ui;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONException;
import org.json.JSONObject;

import com.rapleaf.hank.coordinator.Coordinator;
import com.rapleaf.hank.coordinator.Domain;
import com.rapleaf.hank.coordinator.DomainGroup;
import com.rapleaf.hank.coordinator.DomainVersion;

public class HankApiServlet extends HttpServlet {

  public static class HankApiRequestData {
    private Map<String, Object> params;

    public HankApiRequestData(Map<String, Object> params) {
      this.params = params;
    }

    public Map<String, Object> getParams() {
      return params;
    }
  }

  public static class PARAMS {
    public static final String DOMAIN = "domain";
    public static final String DOMAIN_VERSION = "domain_version";
    public static final String DOMAIN_GROUP = "domain_group";

    public static String[] getParamKeys() {
      return new String[] {DOMAIN, DOMAIN_VERSION, DOMAIN_GROUP};
    }
  }

  static final String JSON_FORMAT = "application/json;charset=utf-8";

  private final Coordinator coordinator;

  public HankApiServlet(Coordinator coordinator) {
    this.coordinator = coordinator;
  }

  @Override
  public void doPost(HttpServletRequest request, HttpServletResponse response) throws IOException {
    doGet(request, response);
  }

  /**
   * Respond to GET requests.
   */
  @Override
  public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
    String responseBody;

    try {
      // Parse request data
      HankApiRequestData requestData = getRequestData(request);
      Map<String, Object> responseData = getResponseData(requestData);
      responseBody = getJsonResponseBody(responseData).toString();
    } catch (Exception e) {
      response.sendError(400, "Error: " + e);
      return;
    }

    response.setContentType(JSON_FORMAT);
    response.setStatus(HttpServletResponse.SC_OK);
    response.getWriter().print(responseBody);
  }

  protected HankApiRequestData getRequestData(HttpServletRequest request) {
    Map<String, Object> requestParams = parseRequestParams(request, PARAMS.getParamKeys());
    return new HankApiRequestData(requestParams);
  }

  protected Map<String, Object> parseRequestParams(HttpServletRequest request, String[] paramKeys) {
    Map<String, Object> params = new HashMap<String, Object>();

    Map requestParamsMap = request.getParameterMap();
    for (String key : paramKeys) {
      if (requestParamsMap.containsKey(key)) {
        params.put(key, request.getParameter(key));
      }
    }

    return params;
  }

  public Map<String, Object> getResponseData(HankApiRequestData requestData) throws IOException {
    Map<String, Object> responseData =  new HashMap<String, Object>();
    Map params = requestData.getParams();
    if (params.containsKey(PARAMS.DOMAIN)) {
      if (params.containsKey(PARAMS.DOMAIN_VERSION)){
        addDomainVersionDataToResponse(requestData, responseData);
      } else {
        addDomainDataToResponse(requestData, responseData);
      }
    }
    if (params.containsKey(PARAMS.DOMAIN_GROUP)){
      addDomainGroupDataToResponse(requestData, responseData);
    }
    return responseData;
  }

  private void addDomainDataToResponse(HankApiRequestData requestData, Map<String, Object> responseData) throws IOException {
    Domain domain = coordinator.getDomain((String) requestData.getParams().get(PARAMS.DOMAIN));
    if (domain != null){
      responseData.put(domain.getName(), getDomainData(domain));
    }
  }

  private void addDomainVersionDataToResponse(HankApiRequestData requestData, Map<String, Object> responseData) throws IOException {
    Domain domain = coordinator.getDomain((String) requestData.getParams().get(PARAMS.DOMAIN));
    try {
      DomainVersion version = domain.getVersionByNumber(Integer.valueOf((String) requestData.getParams().get(PARAMS.DOMAIN_VERSION)));
      responseData.put(String.valueOf(version.getVersionNumber()), getDomainVersionData(version));
    } catch (NumberFormatException e){} // No data added, but no harm done
  }

  private Map<String, Object> getDomainData(Domain domain) throws IOException {
    Map<String, Object> domainData =  new HashMap<String, Object>();
    domainData.put("name", domain.getName());
    domainData.put("num_partitions", domain.getNumParts());

    Map<String, Object> versionsMap =  new HashMap<String, Object>();
    for (DomainVersion v : domain.getVersions()){
      versionsMap.put(String.valueOf(v.getVersionNumber()), getDomainVersionData(v));
    }
    domainData.put("versions", versionsMap);
    return domainData;
  }

  private Map<String, Object> getDomainVersionData(DomainVersion version) throws IOException {
    Map<String, Object> versionData =  new HashMap<String, Object>();
    versionData.put("version_number", version.getVersionNumber());
    versionData.put("total_num_bytes", version.getTotalNumBytes());
    versionData.put("total_num_records", version.getTotalNumRecords());
    versionData.put("is_closed", version.isClosed());
    versionData.put("closed_at", version.getClosedAt());

    return versionData;
  }

  private void addDomainGroupDataToResponse(HankApiRequestData requestData, Map<String, Object> responseData) throws IOException {
    DomainGroup domainGroup = coordinator.getDomainGroup((String) requestData.getParams().get(PARAMS.DOMAIN_GROUP));
    if (domainGroup != null){
      Map<String, Object> domainData =  new HashMap<String, Object>();
      domainData.put("name", domainGroup.getName());

      Map<String, Object> domainsMap =  new HashMap<String, Object>();
      for (Domain d : domainGroup.getDomains()){
        domainsMap.put(String.valueOf(d.getName()), getDomainData(d));
      }
      domainData.put("domains", domainsMap);


      responseData.put(domainGroup.getName(), domainData);
    }
  }

  private JSONObject getJsonResponseBody(Map<String, Object> data) throws JSONException, IOException {
    JSONObject json = new JSONObject();

    for (Map.Entry<String, Object> e : data.entrySet()){
      if (e.getValue() instanceof Map){
        json.put(e.getKey(), getJsonResponseBody((Map<String, Object>) e.getValue()));
      } else {
        json.put(e.getKey(), e.getValue());
      }
    }

    return json;
  }
}
