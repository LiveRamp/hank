package com.rapleaf.hank.ui;

import java.io.IOException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.rapleaf.hank.coordinator.Coordinator;
import com.rapleaf.hank.coordinator.PartDaemonAddress;
import com.rapleaf.hank.ui.controller.Action;
import com.rapleaf.hank.ui.controller.Controller;

public class RingController extends Controller {

  private final Coordinator coordinator;

  public RingController(String name, Coordinator coordinator) {
    super(name);
    this.coordinator = coordinator;
    actions.put("create", new Action() {
      @Override
      protected void action(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        doAddHost(req, resp);
      }
    });
  }

  private void doAddHost(HttpServletRequest req, HttpServletResponse resp) throws IOException {
    String rgName = req.getParameter("rgName");
    int ringNum = Integer.parseInt(req.getParameter("ringNum"));
    String hostname = req.getParameter("hostname");
    int portNum = Integer.parseInt(req.getParameter("port"));
    coordinator.getRingGroupConfig(rgName).getRingConfig(ringNum).addHost(new PartDaemonAddress(hostname, portNum));
    resp.sendRedirect("/ring.jsp?g=" + rgName + "&n=" + ringNum);
  }
}
