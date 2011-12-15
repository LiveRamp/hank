package com.rapleaf.hank.ui.controllers;

import com.rapleaf.hank.coordinator.*;

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
        DomainController.this.coordinator.addDomain(domainName, numParts, storageEngineFactoryName,
            storageEngineOptions, partitionerName);
        redirect("/domains.jsp", resp);
      }
    });
    actions.put("delete", new Action() {
      @Override
      protected void action(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        doDeleteDomain(req, resp);
      }
    });
    actions.put("new_version", new Action() {
      @Override
      protected void action(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        Domain domain = DomainController.this.coordinator.getDomain(req.getParameter("n"));
        if (domain.openNewVersion() != null) {
          Domains.getOpenedVersion(domain).close();
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
    actions.put("undefunctify", new Action() {
      @Override
      protected void action(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        Domain domain = DomainController.this.coordinator.getDomain(req.getParameter("n"));
        final DomainVersion domainVersion = domain.getVersionByNumber(Integer.parseInt(req.getParameter("ver")));
        domainVersion.setDefunct(false);
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
    actions.put("update", new Action() {
      @Override
      protected void action(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        doUpdateDomain(req, resp);
      }
    });
    actions.put("cleanup", new Action() {
      @Override
      protected void action(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        Domain domain = DomainController.this.coordinator.getDomain(req.getParameter("n"));
        domain.getStorageEngine().getRemoteDomainVersionDeleter().deleteVersion(Integer.parseInt(req.getParameter("ver")));
        final DomainVersion domainVersion = domain.getVersionByNumber(Integer.parseInt(req.getParameter("ver")));
        domainVersion.setDefunct(true);
        redirect("/domain.jsp?n=" + req.getParameter("n"), resp);
      }
    });
    actions.put("clean_domains", new Action() {
      @Override
      protected void action(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        doCleanDomains();
        redirect("/domains.jsp", resp);
      }
    });
  }

  private void doCleanDomains() throws IOException {
    Domains.cleanDomains(coordinator.getDomains());
  }

  private void doDeleteDomain(HttpServletRequest req, HttpServletResponse resp) throws IOException {
    final Domain domain = coordinator.getDomain(req.getParameter("name"));
    // check if this domain is in use anywhere
    for (RingGroup rg : coordinator.getRingGroups()) {
      if (rg.getCurrentVersion() == null) {
        continue;
      }

      DomainGroup dg = rg.getDomainGroup();
      DomainGroupVersion dgv = rg.getCurrentVersion();
      if (DomainGroupVersions.containsDomain(dgv, domain)) {
        resp.sendRedirect("/domain.jsp?n=" + req.getParameter("name") + "&used_in_dg=" + dg.getName());
        return;
      }
    }

    coordinator.deleteDomain(domain.getName());
    resp.sendRedirect("/domains.jsp");
  }

  private void doUpdateDomain(HttpServletRequest req, HttpServletResponse resp) throws IOException {
    final String domainName = req.getParameter("name");
    final String storageEngineOptions = req.getParameter("storageEngineOptions");
    final Domain domain = coordinator.getDomain(domainName);
    if (domain == null) {
      throw new IOException("Could not get Domain '" + domainName + "' from Configurator.");
    } else {
      coordinator.updateDomain(domainName, domain.getNumParts(),
          domain.getStorageEngineFactoryClass().getName(), storageEngineOptions, domain.getPartitioner().getClass().getName());
    }
    resp.sendRedirect("/domains.jsp");
  }

}
