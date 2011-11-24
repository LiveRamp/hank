package com.rapleaf.hank.ui.controllers;

import com.rapleaf.hank.coordinator.*;
import com.rapleaf.hank.ui.URLEnc;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

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
    actions.put("delete_partition", new Action() {
      @Override
      protected void action(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        doDeleteOrUndeletePartition(req, resp, true);
      }
    });
    actions.put("undelete_partition", new Action() {
      @Override
      protected void action(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        doDeleteOrUndeletePartition(req, resp, false);
      }
    });
    actions.put("clear_command_queue", new Action() {
      @Override
      protected void action(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        doClearCommandQueue(req, resp);
      }
    });
    actions.put("discard_current_command", new Action() {
      @Override
      protected void action(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        doDiscardCurrentCommand(req, resp);
      }
    });
  }

  protected void doDiscardCurrentCommand(HttpServletRequest req, HttpServletResponse resp) throws IOException {
    RingGroup rg = coordinator.getRingGroup(req.getParameter("g"));
    Ring r = rg.getRing(Integer.parseInt(req.getParameter("n")));
    Host h = r.getHostByAddress(PartitionServerAddress.parse(URLEnc.decode(req.getParameter("h"))));
    h.nextCommand();
    redirectBack(resp, rg, r, h);
  }

  protected void doClearCommandQueue(HttpServletRequest req, HttpServletResponse resp) throws IOException {
    RingGroup rg = coordinator.getRingGroup(req.getParameter("g"));
    Ring r = rg.getRing(Integer.parseInt(req.getParameter("n")));
    Host h = r.getHostByAddress(PartitionServerAddress.parse(URLEnc.decode(req.getParameter("h"))));
    h.clearCommandQueue();

    redirectBack(resp, rg, r, h);
  }

  private void redirectBack(HttpServletResponse resp, RingGroup rgc, Ring rc, Host hc) throws IOException {
    resp.sendRedirect(String.format("/host.jsp?g=%s&r=%s&h=%s", rgc.getName(), rc.getRingNumber(),
        URLEnc.encode(hc.getAddress().toString())));
  }

  protected void doEnqueueCommand(HttpServletRequest req, HttpServletResponse resp) throws IOException {
    RingGroup rg = coordinator.getRingGroup(req.getParameter("g"));
    Ring r = rg.getRing(Integer.parseInt(req.getParameter("n")));
    Host h = r.getHostByAddress(PartitionServerAddress.parse(URLEnc.decode(req.getParameter("h"))));
    h.enqueueCommand(HostCommand.valueOf(req.getParameter("command")));

    redirectBack(resp, rg, r, h);
  }

  private void doAddDomainPart(HttpServletRequest req, HttpServletResponse resp) throws IOException {
    RingGroup rg = coordinator.getRingGroup(req.getParameter("g"));
    Ring r = rg.getRing(Integer.parseInt(req.getParameter("n")));
    Host h = r.getHostByAddress(PartitionServerAddress.parse(URLEnc.decode(req.getParameter("h"))));
    final Domain domain = coordinator.getDomain(req.getParameter("domainId"));
    HostDomain d = h.getHostDomain(domain);
    if (d == null) {
      d = h.addDomain(domain);
    }
    d.addPartition(Integer.parseInt(req.getParameter("partNum")),
        Integer.parseInt(req.getParameter("initialVersion")));

    redirectBack(resp, rg, r, h);
  }

  private void doDeleteOrUndeletePartition(HttpServletRequest req, HttpServletResponse resp, boolean deletable) throws IOException {
    RingGroup rg = coordinator.getRingGroup(req.getParameter("g"));
    Ring r = rg.getRing(Integer.parseInt(req.getParameter("n")));
    Host h = r.getHostByAddress(PartitionServerAddress.parse(URLEnc.decode(req.getParameter("h"))));
    HostDomain hd = h.getHostDomain(coordinator.getDomain(req.getParameter("d")));
    HostDomainPartition hdp = hd.getPartitionByNumber(Integer.parseInt(req.getParameter("p")));
    hdp.setDeletable(deletable);

    redirectBack(resp, rg, r, h);
  }

  public static String getHostUrl(RingGroup ringGroup, Ring ring, Host host) {
    return "/host.jsp?g=" + URLEnc.encode(ringGroup.getName())
        + "&r=" + ring.getRingNumber()
        + "&h=" + URLEnc.encode(host.getAddress().toString());
  }
}
