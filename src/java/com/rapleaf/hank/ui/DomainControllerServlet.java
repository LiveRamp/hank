package com.rapleaf.hank.ui;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.rapleaf.hank.coordinator.Coordinator;

public class DomainControllerServlet extends HttpServlet {

  private final Coordinator coordinator;

  public DomainControllerServlet(Coordinator coordinator) {
    this.coordinator = coordinator;
  }

  @Override
  protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    String domainName = req.getParameter("name");
    int numParts = Integer.parseInt(req.getParameter("numParts"));
    String storageEngineFactoryName = req.getParameter("storageEngineFactoryName");
    String storageEngineOptions = req.getParameter("storageEngineOptions");
    String partitionerName = req.getParameter("partitionerName");
    System.out.println(coordinator.addDomain(domainName, numParts, storageEngineFactoryName, storageEngineOptions, partitionerName, 1));

    resp.sendRedirect("/domains.jsp");
    resp.setStatus(HttpServletResponse.SC_OK);
  }
}