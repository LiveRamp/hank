package com.rapleaf.hank.ui.controllers;

import java.io.IOException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.rapleaf.hank.coordinator.Coordinator;
import com.rapleaf.hank.coordinator.HostCommand;
import com.rapleaf.hank.coordinator.Host;
import com.rapleaf.hank.coordinator.HostDomain;
import com.rapleaf.hank.coordinator.PartDaemonAddress;
import com.rapleaf.hank.coordinator.RingConfig;
import com.rapleaf.hank.coordinator.RingGroupConfig;
import com.rapleaf.hank.ui.URLEnc;

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
    actions.put("enqueue_command", new Action() {
      @Override
      protected void action(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        doEnqueueCommand(req, resp);
      }
    });
  }

  protected void doEnqueueCommand(HttpServletRequest req, HttpServletResponse resp) throws IOException {
    RingGroupConfig rgc = coordinator.getRingGroupConfig(req.getParameter("g"));
    RingConfig rc = rgc.getRingConfig(Integer.parseInt(req.getParameter("n")));
    Host hc = rc.getHostConfigByAddress(PartDaemonAddress.parse(URLEnc.decode(req.getParameter("h"))));
    hc.enqueueCommand(HostCommand.valueOf(req.getParameter("command")));

    resp.sendRedirect(String.format("/host.jsp?g=%s&r=%s&h=%s", rgc.getName(), rc.getRingNumber(), URLEnc.encode(hc.getAddress().toString())));
  }

  private void doAddDomainPart(HttpServletRequest req, HttpServletResponse resp) throws IOException {
    RingGroupConfig rgc = coordinator.getRingGroupConfig(req.getParameter("g"));
    RingConfig rc = rgc.getRingConfig(Integer.parseInt(req.getParameter("n")));
    Host hc = rc.getHostConfigByAddress(PartDaemonAddress.parse(URLEnc.decode(req.getParameter("h"))));
    int dId = Integer.parseInt(req.getParameter("domainId"));
    HostDomain d = hc.getDomainById(dId);
    if (d == null) {
      d = hc.addDomain(dId);
    }
    d.addPartition(Integer.parseInt(req.getParameter("partNum")), Integer.parseInt(req.getParameter("initialVersion")));

    resp.sendRedirect(String.format("/host.jsp?g=%s&r=%s&h=%s", rgc.getName(), rc.getRingNumber(), URLEnc.encode(hc.getAddress().toString())));
  }
}
