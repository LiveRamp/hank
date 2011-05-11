package com.rapleaf.hank.ui.controllers;

import java.io.IOException;
import java.net.URLDecoder;
import java.net.URLEncoder;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.rapleaf.hank.coordinator.Coordinator;
import com.rapleaf.hank.coordinator.HostConfig;
import com.rapleaf.hank.coordinator.HostDomainConfig;
import com.rapleaf.hank.coordinator.HostDomainPartitionConfig;
import com.rapleaf.hank.coordinator.PartDaemonAddress;
import com.rapleaf.hank.coordinator.RingConfig;
import com.rapleaf.hank.coordinator.RingGroupConfig;

public class HostController extends Controller {

  private final Coordinator coordinator;

  public HostController(String name, Coordinator coordinator) {
    super(name);
    this.coordinator = coordinator;
    actions.put("add_domain_part", new Action() {
      @Override
      protected void action(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        doAddDomainPart(req, resp);
      }
    });
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

    resp.sendRedirect(String.format("/host.jsp?g=%s&r=%s&h=%s", rgc.getName(), rc.getRingNumber(), URLEncoder.encode(hc.getAddress().toString())));
  }
}
