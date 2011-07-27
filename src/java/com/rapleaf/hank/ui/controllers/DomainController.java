package com.rapleaf.hank.ui.controllers;

import com.rapleaf.hank.coordinator.Coordinator;
import com.rapleaf.hank.coordinator.Domain;
import com.rapleaf.hank.coordinator.DomainVersion;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

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

        String storageEngineFactoryName = req.getParameter("storageEngineFactorySelect");
        if (storageEngineFactoryName.equals("__other__")) {
          storageEngineFactoryName = req.getParameter("storageEngineFactoryName");
        }

        String storageEngineOptions = req.getParameter("storageEngineOptions");

        String partitionerName = req.getParameter("partitionerSelect");
        if (partitionerName.equals("__other__")) {
          partitionerName = req.getParameter("partitionerOther");
        }
        System.out.println(DomainController.this.coordinator.addDomain(domainName, numParts, storageEngineFactoryName, storageEngineOptions, partitionerName));
        redirect("/domains.jsp", resp);
      }
    });
    actions.put("delete", new Action() {
      @Override
      protected void action(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        DomainController.this.coordinator.deleteDomain(req.getParameter("name"));
        redirect("/domains.jsp", resp);
      }
    });
    actions.put("new_version", new Action() {
      @Override
      protected void action(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        Domain domain = DomainController.this.coordinator.getDomain(req.getParameter("n"));
        if (domain.openNewVersion() != null) {
          domain.getOpenedVersion().close();
        }
        redirect("/domain.jsp?n=" + req.getParameter("n"), resp);
      }
    });
    actions.put("defunctify", new Action() {
      @Override
      protected void action(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        Domain domain = DomainController.this.coordinator.getDomain(req.getParameter("n"));
        final DomainVersion domainVersion = domain.getVersionByNumber(Integer.parseInt(req.getParameter("ver")));
        domainVersion.setDefunct(true);
        redirect("/domain.jsp?n=" + req.getParameter("n"), resp);
      }
    });
    actions.put("close", new Action() {
      @Override
      protected void action(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        Domain domain = DomainController.this.coordinator.getDomain(req.getParameter("n"));
        final DomainVersion domainVersion = domain.getVersionByNumber(Integer.parseInt(req.getParameter("ver")));
        domainVersion.close();
        redirect("/domain.jsp?n=" + req.getParameter("n"), resp);
      }
    });
  }
}
