package com.rapleaf.hank.ui;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.rapleaf.hank.coordinator.Coordinator;

public class RingGroupController extends HttpServlet {

  private final Coordinator coordinator;

  public RingGroupController(Coordinator coordinator) {
    this.coordinator = coordinator;
  }

  @Override
  protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    if (req.getRequestURI().contains("create")) {
      doCreate(req, resp);
    } else {
      resp.sendError(404);
    }
  }

  private void doCreate(HttpServletRequest req, HttpServletResponse resp) throws IOException {
    coordinator.addRingGroup(req.getParameter("rgName"), req.getParameter("dgName"));
    // could log the rg...
    resp.sendRedirect("/ring_groups.jsp");
  }
}
