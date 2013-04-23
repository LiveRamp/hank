package com.liveramp.hank.ui;

import com.google.common.base.Throwables;
import com.liveramp.hank.coordinator.*;
import org.json.JSONException;
import org.json.JSONObject;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class HankApiServlet extends HttpServlet {

  private static class InvalidParamsException extends Exception {
  }

  private static class Params {
    public static final String DOMAIN = "domain";
    public static final String DOMAIN_VERSION = "domain_version";
    public static final String DOMAIN_GROUP = "domain_group";
    public static final String RING_GROUP = "ring_group";
    public static final String ALL_RING_GROUPS = "get_all_ring_groups";
    public static final String DEPLOY_STATUS_FOR_DOMAIN = "deploy_status_for_domain";
    public static final String DEPLOY_STATUS_FOR_DOMAIN_GROUP = "deploy_status_for_domain_group";

    public static String[] getParamKeys() {
      return new String[]{DOMAIN, DOMAIN_VERSION, DOMAIN_GROUP, RING_GROUP, ALL_RING_GROUPS, DEPLOY_STATUS_FOR_DOMAIN, DEPLOY_STATUS_FOR_DOMAIN_GROUP};
    }

    public static boolean paramsAreValid(Collection<String> params) {
      return paramsMatch(params, DOMAIN) ||
          paramsMatch(params, DOMAIN, DOMAIN_VERSION) ||
          paramsMatch(params, DEPLOY_STATUS_FOR_DOMAIN) ||
          paramsMatch(params, DOMAIN_GROUP) ||
          paramsMatch(params, DEPLOY_STATUS_FOR_DOMAIN_GROUP) ||
          paramsMatch(params, RING_GROUP) ||
          paramsMatch(params, ALL_RING_GROUPS);
    }

    private static boolean paramsMatch(Collection<String> params, String... expected) {
      if (params.size() == expected.length &&
          params.containsAll(Arrays.asList(expected))) {
        return true;
      }
      return false;
    }
  }

  public static final String ERROR_INTERNAL_SERVER_ERROR = "Internal Server Error";
  public static final String ERROR_INVALID_PARAMETERS = "The combination of parameters submitted is not valid.";
  static final String JSON_FORMAT = "application/json;charset=utf-8";

  private final Coordinator coordinator;
  private final HankApiHelper apiHelper;

  public HankApiServlet(Coordinator coordinator) {
    this.coordinator = coordinator;
    this.apiHelper = new HankApiHelper(coordinator);
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
      Map<String, Object> requestData = parseRequestParams(request, Params.getParamKeys());
      Map<String, Object> responseData = getResponseData(requestData);
      responseBody = getJsonResponseBody(responseData).toString();
    } catch (InvalidParamsException ipe) {
      sendResponseInvalidParams(response);
      return;
    } catch (Throwable t) {
      sendResponseInternalServerError(response, t);
      return;
    }

    response.setContentType(JSON_FORMAT);
    response.setStatus(HttpServletResponse.SC_OK);
    response.getWriter().print(responseBody);
  }

  private void sendResponseError(int errorCode, String errorMessage, HttpServletResponse response) {
    response.reset();

    response.setContentType("text/plain;charset=utf-8");
    response.setStatus(errorCode);
    try {
      response.getWriter().print(errorMessage);
    } catch (IOException e) {
    }
  }

  protected void sendResponseInternalServerError(HttpServletResponse response, Throwable t) {
    String error = "";
    for (Throwable cause : Throwables.getCausalChain(t)) {
      error += cause.getMessage();
      error += "\n" + Throwables.getStackTraceAsString(cause);
    }
    sendResponseError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
        ERROR_INTERNAL_SERVER_ERROR + ":\n" + error,
        response);
  }

  protected void sendResponseInvalidParams(HttpServletResponse response) {
    sendResponseError(HttpServletResponse.SC_BAD_REQUEST, ERROR_INVALID_PARAMETERS, response);
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

  public Map<String, Object> getResponseData(Map<String, Object> requestData) throws IOException, InvalidParamsException {
    Map<String, Object> responseData = new HashMap<String, Object>();
    if (!Params.paramsAreValid(requestData.keySet())) {
      throw new InvalidParamsException();
    }
    if (requestData.containsKey(Params.DOMAIN)) {
      if (requestData.containsKey(Params.DOMAIN_VERSION)) {
        addDomainVersionDataToResponse(requestData, responseData);
      } else {
        addDomainDataToResponse(requestData, responseData);
      }
    } else if (requestData.containsKey(Params.DEPLOY_STATUS_FOR_DOMAIN)) {
      addDomainDeployStatusToResponse(requestData, responseData);
    } else if (requestData.containsKey(Params.ALL_RING_GROUPS)) {
      addAllRingGroupsDataToResponse(requestData, responseData);
    } else if (requestData.containsKey(Params.RING_GROUP)) {
      addRingGroupDataToResponse(requestData, responseData);
    } else if (requestData.containsKey(Params.DOMAIN_GROUP)) {
      addDomainGroupDataToResponse(requestData, responseData);
    } else if (requestData.containsKey(Params.DEPLOY_STATUS_FOR_DOMAIN_GROUP)) {
      addDomainGroupDeployStatusToResponse(requestData, responseData);
    }

    return responseData;
  }

  private void addAllRingGroupsDataToResponse(Map<String, Object> requestData, Map<String, Object> responseData) throws IOException {
    for (RingGroup ringGroup : coordinator.getRingGroups()) {
      responseData.put(ringGroup.getName(), apiHelper.getRingGroupData(ringGroup).asMap());
    }
  }

  private void addRingGroupDataToResponse(Map<String, Object> requestData, Map<String, Object> responseData) throws IOException {
    RingGroup ringGroup = coordinator.getRingGroup((String) requestData.get(Params.RING_GROUP));
    if (ringGroup != null) {
      responseData.put(ringGroup.getName(), apiHelper.getRingGroupData(ringGroup).asMap());
    }
  }

  private void addDomainDataToResponse(Map<String, Object> requestData, Map<String, Object> responseData) throws IOException {
    HankApiHelper.DomainData data = apiHelper.getDomainData((String) requestData.get(Params.DOMAIN));
    if (data != null) {
      responseData.put(data.name, data.asMap());
    }
  }

  private void addDomainDeployStatusToResponse(Map<String, Object> requestData, Map<String, Object> responseData) throws IOException {
    Domain domain = coordinator.getDomain((String) requestData.get(Params.DEPLOY_STATUS_FOR_DOMAIN));
    if (domain != null) {
      responseData.put(domain.getName(), apiHelper.getDomainDeployStatus(domain).asMap());
    }
  }

  private void addDomainGroupDeployStatusToResponse(Map<String, Object> requestData, Map<String, Object> responseData) throws IOException {
    DomainGroup domainGroup = coordinator.getDomainGroup((String) requestData.get(Params.DEPLOY_STATUS_FOR_DOMAIN_GROUP));
    if (domainGroup != null) {
      responseData.put(domainGroup.getName(), apiHelper.getDomainGroupDeployStatus(domainGroup).asMap());
    }
  }

  private void addDomainVersionDataToResponse(Map<String, Object> requestData, Map<String, Object> responseData) throws IOException {
    Domain domain = coordinator.getDomain((String) requestData.get(Params.DOMAIN));
    try {
      DomainVersion version = domain.getVersion(Integer.valueOf((String) requestData.get(Params.DOMAIN_VERSION)));
      responseData.put(String.valueOf(version.getVersionNumber()), apiHelper.getDomainVersionData(version).asMap());
    } catch (Exception ignored) {
    } // No data added, but no harm done
  }

  private void addDomainGroupDataToResponse(Map<String, Object> requestData, Map<String, Object> responseData) throws IOException {
    DomainGroup domainGroup = coordinator.getDomainGroup((String) requestData.get(Params.DOMAIN_GROUP));
    if (domainGroup != null) {
      HankApiHelper.DomainGroupData data = apiHelper.getDomainGroupData(domainGroup);
      responseData.put(domainGroup.getName(), data.asMap());
    }
  }

  private JSONObject getJsonResponseBody(Map<String, Object> data) throws JSONException, IOException {
    JSONObject json = new JSONObject();

    for (Map.Entry<String, Object> e : data.entrySet()) {
      if (e.getValue() instanceof Map) {
        json.put(e.getKey(), getJsonResponseBody((Map<String, Object>) e.getValue()));
      } else {
        json.put(e.getKey(), e.getValue());
      }
    }

    return json;
  }
}
