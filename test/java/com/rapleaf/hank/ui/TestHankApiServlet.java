package com.rapleaf.hank.ui;


import java.io.IOException;
import javax.servlet.http.HttpServletResponse;

import org.junit.Test;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import com.rapleaf.hank.ZkTestCase;

public class TestHankApiServlet extends ZkTestCase {

  private HankApiServlet apiServlet;

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    apiServlet = new  HankApiServlet(getMockCoordinator());
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
    assertEmptyResponse(new String[]{"domain", "domain_version"}, new String[]{"domain0", "blah"});

    assertNotEmptyResponse(new String[]{"domain"}, new String[]{"domain0"});
    assertNotEmptyResponse(new String[]{"domain", "domain_version"}, new String[]{"domain0", "0"});
  }

  @Test
  public void testGetDomainGroup() throws IOException {
    assertEmptyResponse(new String[]{"domain_group"}, new String[]{"blah"});
    assertEmptyResponse(new String[]{"domain_group", "domain_group_version"}, new String[]{"Group_1", "blah"});

    assertNotEmptyResponse(new String[]{"domain_group"}, new String[]{"Group_1"});
    assertNotEmptyResponse(new String[]{"domain_group", "domain_group_version"}, new String[]{"Group_1", "0"});
  }

  @Test
  public void testRingGroup() throws IOException {
    assertEmptyResponse(new String[]{"ring_group"}, new String[]{"blah"});

    assertNotEmptyResponse(new String[]{"ring_group"}, new String[]{"RG_Alpha"});
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
