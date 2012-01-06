package com.rapleaf.hank.ui;

import com.rapleaf.hank.coordinator.*;
import org.json.JSONException;
import org.json.JSONObject;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.*;

public class HankApiServlet extends HttpServlet {

  private static class InvalidParamsException extends Exception {
  }

  ;

  private static class Params {
    public static final String DOMAIN = "domain";
    public static final String DOMAIN_VERSION = "domain_version";
    public static final String DOMAIN_GROUP = "domain_group";
    public static final String DOMAIN_GROUP_VERSION = "domain_group_version";
    public static final String RING_GROUP = "ring_group";
    public static final String ALL_RING_GROUPS = "get_all_ring_groups";
    public static final String DEPLOY_STATUS_FOR_DOMAIN = "deploy_status_for_domain";
    public static final String DEPLOY_STATUS_FOR_DOMAIN_GROUP = "deploy_status_for_domain_group";

    public static String[] getParamKeys() {
      return new String[]{DOMAIN, DOMAIN_VERSION, DOMAIN_GROUP, DOMAIN_GROUP_VERSION, RING_GROUP, ALL_RING_GROUPS, DEPLOY_STATUS_FOR_DOMAIN, DEPLOY_STATUS_FOR_DOMAIN_GROUP};
    }

    public static boolean paramsAreValid(Collection<String> params) {
      return paramsMatch(params, DOMAIN) ||
          paramsMatch(params, DOMAIN, DOMAIN_VERSION) ||
          paramsMatch(params, DEPLOY_STATUS_FOR_DOMAIN) ||
          paramsMatch(params, DOMAIN_GROUP) ||
          paramsMatch(params, DOMAIN_GROUP, DOMAIN_GROUP_VERSION) ||
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
      Map<String, Object> requestData = parseRequestParams(request, Params.getParamKeys());
      Map<String, Object> responseData = getResponseData(requestData);
      responseBody = getJsonResponseBody(responseData).toString();
    } catch (InvalidParamsException ipe) {
      sendResponseInvalidParams(response);
      return;
    } catch (Throwable e) {
      sendResponseInternalServerError(response);
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

  protected void sendResponseInternalServerError(HttpServletResponse response) {
    sendResponseError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, ERROR_INTERNAL_SERVER_ERROR, response);
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
      if (requestData.containsKey(Params.DOMAIN_GROUP_VERSION)) {
        addDomainGroupVersionDataToResponse(requestData, responseData);
      } else {
        addDomainGroupDataToResponse(requestData, responseData);
      }
    } else if (requestData.containsKey(Params.DEPLOY_STATUS_FOR_DOMAIN_GROUP)) {
      addDomainGroupDeployStatusToResponse(requestData, responseData);
    }

    return responseData;
  }

  private void addAllRingGroupsDataToResponse(Map<String, Object> requestData, Map<String, Object> responseData) throws IOException {
    for (RingGroup ringGroup : coordinator.getRingGroups()) {
      responseData.put(ringGroup.getName(), getRingGroupData(ringGroup));
    }
  }

  private void addRingGroupDataToResponse(Map<String, Object> requestData, Map<String, Object> responseData) throws IOException {
    RingGroup ringGroup = coordinator.getRingGroup((String) requestData.get(Params.RING_GROUP));
    if (ringGroup != null) {
      responseData.put(ringGroup.getName(), getRingGroupData(ringGroup));
    }
  }

  private Map<String, Object> getRingGroupData(RingGroup ringGroup) throws IOException {
    Map<String, Object> ringGroupData = new HashMap<String, Object>();
    ringGroupData.put("name", ringGroup.getName());
    ringGroupData.put("target_version", ringGroup.getTargetVersionNumber());
    ringGroupData.put("is_ring_group_conductor_online", ringGroup.isRingGroupConductorOnline());
    ringGroupData.put("domain_group", ringGroup.getDomainGroup().getName());
    ringGroupData.put("rings", getRingsMap(ringGroup.getRings()));
    return ringGroupData;
  }

  private Map<String, Object> getRingsMap(Collection<Ring> rings) throws IOException {
    Map<String, Object> ringsMap = new HashMap<String, Object>();
    for (Ring ring : rings) {
      ringsMap.put(String.valueOf(ring.getRingNumber()), getRingData(ring));
    }
    return ringsMap;
  }

  private Map<String, Object> getRingData(Ring ring) throws IOException {
    Map<String, Object> ringData = new HashMap<String, Object>();
    ringData.put("ring_number", ring.getRingNumber());
    ringData.put("hosts", getHostsMap(ring.getHosts()));
    return ringData;
  }

  private Map<String, Object> getHostsMap(Collection<Host> hosts) throws IOException {
    Map<String, Object> ringsMap = new HashMap<String, Object>();
    for (Host host : hosts) {
      ringsMap.put(String.valueOf(host.getAddress()), getHostData(host));
    }
    return ringsMap;
  }

  private Map<String, Object> getHostData(Host host) throws IOException {
    Map<String, Object> hostData = new HashMap<String, Object>();
    hostData.put("address", host.getAddress());
    hostData.put("status", host.getState().name());
    hostData.put("is_online", Hosts.isOnline(host));
    return hostData;
  }

  private void addDomainDataToResponse(Map<String, Object> requestData, Map<String, Object> responseData) throws IOException {
    Domain domain = coordinator.getDomain((String) requestData.get(Params.DOMAIN));
    if (domain != null) {
      responseData.put(domain.getName(), getDomainData(domain));
    }
  }

  private void addDomainDeployStatusToResponse(Map<String, Object> requestData, Map<String, Object> responseData) throws IOException {
    Domain domain = coordinator.getDomain((String) requestData.get(Params.DEPLOY_STATUS_FOR_DOMAIN));
    if (domain != null) {
      Set<DomainGroupVersion> domainGroupVersions = coordinator.getDomainGroupVersionsForDomain(domain);
      for (DomainGroupVersion domainGroupVersion : domainGroupVersions) {
        Set<RingGroup> ringGroups = coordinator.getRingGroupsForDomainGroup(domainGroupVersion.getDomainGroup());

        for (RingGroup ringGroup : ringGroups) {
          Map<String, Object> ringGroupMap = new HashMap<String, Object>();
          Integer targetDomainGroupVersion = ringGroup.getTargetVersionNumber();
          addDomainVersionToRingGroupMap("target_version", domain, domainGroupVersion.getDomainGroup(), ringGroupMap, targetDomainGroupVersion);

          responseData.put(ringGroup.getName(), ringGroupMap);
        }
      }
    }
  }

  private void addDomainVersionToRingGroupMap(String key, Domain domain, DomainGroup domainGroup, Map<String, Object> ringGroupMap, Integer domainGroupVersion) throws IOException {
    if (domainGroupVersion != null) {
      int domainVersion = domainGroup.getVersionByNumber(domainGroupVersion).getDomainVersion(domain).getVersion();
      ringGroupMap.put(key, domainVersion);
    }
  }

  private void addDomainGroupDeployStatusToResponse(Map<String, Object> requestData, Map<String, Object> responseData) throws IOException {
    DomainGroup domainGroup = coordinator.getDomainGroup((String) requestData.get(Params.DEPLOY_STATUS_FOR_DOMAIN_GROUP));
    if (domainGroup != null) {
      Set<RingGroup> ringGroups = coordinator.getRingGroupsForDomainGroup(domainGroup);
      for (RingGroup ringGroup : ringGroups) {
        Map<String, Object> ringGroupMap = new HashMap<String, Object>();
        ringGroupMap.put("target_version", ringGroup.getTargetVersionNumber());
        responseData.put(ringGroup.getName(), ringGroupMap);
      }
    }
  }

  private void addDomainVersionDataToResponse(Map<String, Object> requestData, Map<String, Object> responseData) throws IOException {
    Domain domain = coordinator.getDomain((String) requestData.get(Params.DOMAIN));
    try {
      DomainVersion version = domain.getVersionByNumber(Integer.valueOf((String) requestData.get(Params.DOMAIN_VERSION)));
      responseData.put(String.valueOf(version.getVersionNumber()), getDomainVersionData(version));
    } catch (Exception ignored) {
    } // No data added, but no harm done
  }

  private Map<String, Object> getDomainData(Domain domain) throws IOException {
    Map<String, Object> domainData = new HashMap<String, Object>();
    domainData.put("name", domain.getName());
    domainData.put("num_partitions", domain.getNumParts());
    domainData.put("versions", getDomainVersionsMap(domain.getVersions()));
    return domainData;
  }

  private Map<String, Object> getDomainVersionsMap(Collection<DomainVersion> domainVersions) throws IOException {
    Map<String, Object> versionsMap = new HashMap<String, Object>();
    for (DomainVersion v : domainVersions) {
      versionsMap.put(String.valueOf(v.getVersionNumber()), getDomainVersionData(v));
    }
    return versionsMap;
  }

  private Map<String, Object> getDomainVersionData(DomainVersion version) throws IOException {
    Map<String, Object> versionData = new HashMap<String, Object>();
    versionData.put("version_number", version.getVersionNumber());
    versionData.put("total_num_bytes", DomainVersions.getTotalNumBytes(version));
    versionData.put("total_num_records", DomainVersions.getTotalNumRecords(version));
    versionData.put("is_closed", DomainVersions.isClosed(version));
    versionData.put("closed_at", version.getClosedAt());

    return versionData;
  }

  private Map<String, Object> getDomainGroupVersionData(DomainGroupVersion version) throws IOException {
    Map<String, Object> versionData = new HashMap<String, Object>();
    versionData.put("version_number", version.getVersionNumber());
    versionData.put("domain_versions", getDomainGroupVersionDomainVersionsMap(version.getDomainVersions()));

    return versionData;
  }

  private Map<String, Object> getDomainGroupVersionDomainVersionsMap(Collection<DomainGroupVersionDomainVersion> versions) throws IOException {
    Map<String, Object> versionsMap = new HashMap<String, Object>();
    for (DomainGroupVersionDomainVersion v : versions) {
      versionsMap.put(v.getDomain().getName(), v.getVersion());
    }
    return versionsMap;
  }

  private void addDomainGroupDataToResponse(Map<String, Object> requestData, Map<String, Object> responseData) throws IOException {
    DomainGroup domainGroup = coordinator.getDomainGroup((String) requestData.get(Params.DOMAIN_GROUP));
    if (domainGroup != null) {
      Map<String, Object> groupData = new HashMap<String, Object>();
      groupData.put("name", domainGroup.getName());

      Map<String, Object> versionsMap = new HashMap<String, Object>();
      for (DomainGroupVersion v : domainGroup.getVersions()) {
        versionsMap.put(String.valueOf(v.getVersionNumber()), getDomainGroupVersionData(v));
      }
      groupData.put("versions", versionsMap);

      responseData.put(domainGroup.getName(), groupData);
    }
  }

  private void addDomainGroupVersionDataToResponse(Map<String, Object> requestData, Map<String, Object> responseData) throws IOException {
    DomainGroup domainGroup = coordinator.getDomainGroup((String) requestData.get(Params.DOMAIN_GROUP));
    try {
      DomainGroupVersion version = domainGroup.getVersionByNumber(Integer.valueOf((String) requestData.get(Params.DOMAIN_GROUP_VERSION)));
      responseData.put(String.valueOf(version.getVersionNumber()), getDomainGroupVersionData(version));
    } catch (Exception ignored) {
    } // No data added, but no harm done
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
