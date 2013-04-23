package com.liveramp.hank.ui.controllers;

import com.liveramp.hank.coordinator.*;
import com.liveramp.hank.ui.URLEnc;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

public class RingController extends Controller {

  private final Coordinator coordinator;

  public RingController(String name, Coordinator coordinator) {
    super(name);
    this.coordinator = coordinator;
    actions.put("add_host", new Action() {
      @Override
      protected void action(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        doAddHost(req, resp);
      }
    });

    actions.put("delete_host", new Action() {
      @Override
      protected void action(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        doDeleteHost(req, resp);
      }
    });

    actions.put("command_all", new Action() {
      @Override
      protected void action(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        doCommandAll(req, resp);
      }
    });
  }

  protected void doDeleteHost(HttpServletRequest req, HttpServletResponse resp) throws IOException {
    RingGroup ringGroup = coordinator.getRingGroup(req.getParameter("g"));
    Ring ring = ringGroup.getRing(Integer.parseInt(req.getParameter("n")));
    ring.removeHost(PartitionServerAddress.parse(URLEnc.decode(req.getParameter("h"))));

    resp.sendRedirect(String.format("/ring.jsp?g=%s&n=%d", ringGroup.getName(),
        ring.getRingNumber()));
  }

  private void doAddHost(HttpServletRequest req, HttpServletResponse resp) throws IOException {
    String rgName = req.getParameter("rgName");
    int ringNum = Integer.parseInt(req.getParameter("ringNum"));
    String hostname = req.getParameter("hostname");
    int portNum = Integer.parseInt(req.getParameter("port"));
    String flagsStr = req.getParameter("hostFlags");
    coordinator.getRingGroup(rgName).getRing(ringNum).addHost(
        new PartitionServerAddress(hostname, portNum), Hosts.splitHostFlags(flagsStr));
    resp.sendRedirect("/ring.jsp?g=" + rgName + "&n=" + ringNum);
  }

  private void doCommandAll(HttpServletRequest req, HttpServletResponse resp) throws IOException {
    String rgName = req.getParameter("rgName");
    int ringNum = Integer.parseInt(req.getParameter("ringNum"));
    HostCommand command = HostCommand.valueOf(req.getParameter("command"));
    Rings.commandAll(coordinator.getRingGroup(rgName).getRing(ringNum), command);
    resp.sendRedirect("/ring.jsp?g=" + rgName + "&n=" + ringNum);
  }
}
