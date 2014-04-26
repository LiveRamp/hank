package com.liveramp.hank.ui;


import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

import org.junit.Before;
import org.junit.Test;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import com.liveramp.hank.test.ZkMockCoordinatorTestCase;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

public class TestHankApiServlet extends ZkMockCoordinatorTestCase {

  private HankApiServlet apiServlet;

  @Before
  public void setUp() throws Exception {
    apiServlet = new HankApiServlet(getApiMockCoordinator());
  }

  @Test
  public void testParamsValidation() throws Exception {
    // Valid
    assertEquals(HttpServletResponse.SC_OK, getResponseStatus("domain"));
    assertEquals(HttpServletResponse.SC_OK, getResponseStatus("domain", "domain_version"));
    assertEquals(HttpServletResponse.SC_OK, getResponseStatus("domain_group"));
    assertEquals(HttpServletResponse.SC_OK, getResponseStatus("domain_group", "domain_group_version"));
    assertEquals(HttpServletResponse.SC_OK, getResponseStatus("ring_group"));

    // Invalid
    assertEquals(HttpServletResponse.SC_BAD_REQUEST, getResponseStatus());
    assertEquals(HttpServletResponse.SC_BAD_REQUEST, getResponseStatus("qpoiweurpoi"));
    assertEquals(HttpServletResponse.SC_BAD_REQUEST, getResponseStatus("domain", "domain_group"));
    assertEquals(HttpServletResponse.SC_BAD_REQUEST, getResponseStatus("domain", "ring_group"));
  }

  @Test
  public void testGetDomain() throws IOException {
    assertEmptyResponse(new String[]{"domain"}, new String[]{"blah"});
    assertEmptyResponse(new String[]{"domain", "domain_version"}, new String[]{ZkMockCoordinatorTestCase.DOMAIN_0, "blah"});

    assertNotEmptyResponse(new String[]{"domain"}, new String[]{ZkMockCoordinatorTestCase.DOMAIN_0});
    assertNotEmptyResponse(new String[]{"domain", "domain_version"}, new String[]{ZkMockCoordinatorTestCase.DOMAIN_0, "0"});
  }

  @Test
  public void testGetDomainGroup() throws IOException {
    assertEmptyResponse(new String[]{"domain_group"}, new String[]{"blah"});

    assertNotEmptyResponse(new String[]{"domain_group"}, new String[]{ZkMockCoordinatorTestCase.DOMAIN_GROUP_0});
  }

  @Test
  public void testRingGroup() throws IOException {
    assertEmptyResponse(new String[]{"ring_group"}, new String[]{"blah"});

    assertNotEmptyResponse(new String[]{"ring_group"}, new String[]{ZkMockCoordinatorTestCase.RING_GROUP_0});
  }

  @Test
  public void testDeployStatusForDomain() throws IOException {
    assertEmptyResponse(new String[]{"deploy_status_for_domain"}, new String[]{"blah"});
    assertNotEmptyResponse(new String[]{"deploy_status_for_domain"}, new String[]{ZkMockCoordinatorTestCase.DOMAIN_0});
  }

  @Test
  public void testDeployStatusForDomainGroup() throws IOException {
    assertEmptyResponse(new String[]{"deploy_status_for_domain_group"}, new String[]{"blah"});
    assertNotEmptyResponse(new String[]{"deploy_status_for_domain_group"}, new String[]{ZkMockCoordinatorTestCase.DOMAIN_GROUP_0});
  }

  private void assertEmptyResponse(String[] params, String[] values) throws IOException {
    assertEquals("Response should be empty", "{}", getResponse(params, values).getContentAsString());
  }

  private void assertNotEmptyResponse(String[] params, String[] values) throws IOException {
    assertFalse("Response should not be empty", "{}".equals(getResponse(params, values).getContentAsString()));
  }

  private MockHttpServletResponse getResponse(String[] params, String[] values) throws IOException {
    MockHttpServletRequest request = new MockHttpServletRequest();
    MockHttpServletResponse response = new MockHttpServletResponse();
    request.setRequestURI("/api");

    if (params.length != values.length) {
      fail("Invalid call to getResponse!");
    }

    for (int i = 0; i < params.length; i++) {
      request.setParameter(params[i], values[i]);
    }

    apiServlet.doGet(request, response);

    return response;
  }


  private int getResponseStatus(String... params) throws IOException {
    return getResponse(params, new String[params.length]).getStatus();
  }
}
