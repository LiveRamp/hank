package com.rapleaf.hank.ui;

import java.io.IOException;
import java.net.URLDecoder;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.rapleaf.hank.coordinator.Coordinator;
import com.rapleaf.hank.coordinator.HostConfig;
import com.rapleaf.hank.coordinator.HostDomainConfig;
import com.rapleaf.hank.coordinator.HostDomainPartitionConfig;
import com.rapleaf.hank.coordinator.PartDaemonAddress;
import com.rapleaf.hank.coordinator.RingConfig;
import com.rapleaf.hank.coordinator.RingGroupConfig;

public class HostController extends HttpServlet{
  private final Coordinator coordinator;

  public HostController(Coordinator coordinator) {
    this.coordinator = coordinator;
  }

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    doPost(req, resp);
  }

  @Override
  protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    if (req.getRequestURI().contains("add_domain_part")) {
      doAddDomainPart(req, resp);
    }
  }

  private void doAddDomainPart(HttpServletRequest req, HttpServletResponse resp) throws IOException {
    RingGroupConfig rgc = coordinator.getRingGroupConfig(req.getParameter("g"));
    RingConfig rc = rgc.getRingConfig(Integer.parseInt(req.getParameter("n")));
    HostConfig hc = rc.getHostConfigByAddress(PartDaemonAddress.parse(URLDecoder.decode(req.getParameter("h"))));
    int dId = Integer.parseInt(req.getParameter("domainId"));
    HostDomainConfig d = hc.getDomainById(dId);
    if (d == null) {
      d = hc.addDomain(dId);
    }
    HostDomainPartitionConfig p = d.addPartition(Integer.parseInt(req.getParameter("partNum")), Integer.parseInt(req.getParameter("initialVersion")));
    
    resp.sendRedirect(String.format("/host.jsp?g=%s&n=%s&h=%s", "", "", ""));
  }
}
