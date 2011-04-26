package com.rapleaf.hank.ui;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.rapleaf.hank.coordinator.Coordinator;

public class DomainGroupController extends HttpServlet {
  private final Coordinator coordinator;

  public DomainGroupController(Coordinator coordinator) {
    this.coordinator = coordinator;
  }

  @Override
  protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    if (req.getRequestURI().contains("create")) {
      doCreate(req, resp);
    } else {
      System.out.println("Bad URI!" + req.getRequestURI());
      resp.sendError(404);
    }
  }

  private void doCreate(HttpServletRequest req, HttpServletResponse response) throws IOException {
    coordinator.addDomainGroup(req.getParameter("name"));
    response.sendRedirect("/domain_groups.jsp");
  }
}
