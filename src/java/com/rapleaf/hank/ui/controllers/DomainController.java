package com.rapleaf.hank.ui.controllers;

import java.io.IOException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.rapleaf.hank.coordinator.Coordinator;
import com.rapleaf.hank.coordinator.Domain;

public class DomainController extends Controller {

  private final Coordinator coordinator;

  public DomainController(String name, Coordinator coordinator) {
    super(name);
    this.coordinator = coordinator;
    actions.put("create", new Action() {
      @Override
      protected void action(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        String domainName = req.getParameter("name");
        int numParts = Integer.parseInt(req.getParameter("numParts"));
        String storageEngineFactoryName = req.getParameter("storageEngineFactoryName");
        String storageEngineOptions = req.getParameter("storageEngineOptions");
        String partitionerName = req.getParameter("partitionerName");
        System.out.println(DomainController.this.coordinator.addDomain(domainName, numParts, storageEngineFactoryName, storageEngineOptions, partitionerName));
        redirect("/domains.jsp", resp);
      }
    });
    actions.put("delete", new Action() {
      @Override
      protected void action(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        DomainController.this.coordinator.deleteDomainConfig(req.getParameter("name"));
        redirect("/domains.jsp", resp);
      }
    });
    actions.put("new_version", new Action() {
      @Override
      protected void action(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        Domain domainConfig = DomainController.this.coordinator.getDomainConfig(req.getParameter("n"));
        if (domainConfig.openNewVersion() != null) {
          domainConfig.getVersions().last().close();
        }
        redirect("/domain.jsp?n=" + req.getParameter("n"), resp);
      }
    });
  }
}