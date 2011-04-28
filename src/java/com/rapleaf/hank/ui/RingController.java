package com.rapleaf.hank.ui;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.rapleaf.hank.coordinator.Coordinator;
import com.rapleaf.hank.coordinator.PartDaemonAddress;
import com.rapleaf.hank.exception.DataNotFoundException;

public class RingController extends HttpServlet {

  private final Coordinator coordinator;

  public RingController(Coordinator coordinator) {
    this.coordinator = coordinator;
  }

  @Override
  protected void doGet(HttpServletRequest arg0, HttpServletResponse arg1) throws ServletException, IOException {
    doPost(arg0, arg1);
  }

  @Override
  protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    if (req.getRequestURI().contains("add_host")) {
      doAddHost(req, resp);
    } else {
      resp.sendError(404);
    }
  }

  private void doAddHost(HttpServletRequest req, HttpServletResponse resp) throws IOException {
    String rgName = req.getParameter("rgName");
    int ringNum = Integer.parseInt(req.getParameter("ringNum"));
    String hostname = req.getParameter("hostname");
    int portNum = Integer.parseInt(req.getParameter("port"));
    try {
      coordinator.getRingGroupConfig(rgName).getRingConfig(ringNum).addHost(new PartDaemonAddress(hostname, portNum));
    } catch (DataNotFoundException e) {
      throw new IOException(e);
    }
    resp.sendRedirect("/ring.jsp?g=" + rgName + "&n=" + ringNum);
  }
  
}
