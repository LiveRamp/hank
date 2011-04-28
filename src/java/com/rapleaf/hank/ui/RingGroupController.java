package com.rapleaf.hank.ui;

import java.io.IOException;
import java.net.URLDecoder;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.rapleaf.hank.coordinator.Coordinator;
import com.rapleaf.hank.coordinator.RingGroupConfig;
import com.rapleaf.hank.exception.DataNotFoundException;

public class RingGroupController extends HttpServlet {

  private final Coordinator coordinator;

  public RingGroupController(Coordinator coordinator) {
    this.coordinator = coordinator;
  }

  @Override
  protected void doGet(HttpServletRequest arg0, HttpServletResponse arg1) throws ServletException, IOException {
    doPost(arg0, arg1);
  }

  @Override
  protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    if (req.getRequestURI().contains("create")) {
      doCreate(req, resp);
    } else if (req.getRequestURI().contains("add_ring")) {
      doAddRing(req, resp);
    } else {
      resp.sendError(404);
    }
  }

  private void doAddRing(HttpServletRequest req, HttpServletResponse resp) throws IOException {
    RingGroupConfig ringGroupConfig;
    String encodedRingGroupName = req.getParameter("g");
    try {
      ringGroupConfig = coordinator.getRingGroupConfig(URLDecoder.decode(encodedRingGroupName));
      if (ringGroupConfig == null) {
        throw new IOException("couldn't find any ring group called " + URLDecoder.decode(encodedRingGroupName));
      }
    } catch (DataNotFoundException e) {
      throw new IOException(e);
    }
    ringGroupConfig.addRing(ringGroupConfig.getRingConfigs().size() + 1);
    resp.sendRedirect("/ring_group.jsp?name=" + encodedRingGroupName);
  }

  private void doCreate(HttpServletRequest req, HttpServletResponse resp) throws IOException {
    coordinator.addRingGroup(req.getParameter("rgName"), req.getParameter("dgName"));
    // could log the rg...
    resp.sendRedirect("/ring_groups.jsp");
  }
}
