package com.rapleaf.hank.ui.controllers;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang.NotImplementedException;

import com.rapleaf.hank.coordinator.Coordinator;
import com.rapleaf.hank.coordinator.Domain;
import com.rapleaf.hank.coordinator.DomainGroup;
import com.rapleaf.hank.coordinator.VersionOrAction;
import com.rapleaf.hank.ui.URLEnc;

public class DomainGroupController extends Controller {

  private final Coordinator coordinator;

  public DomainGroupController(String name, Coordinator coordinator) {
    super(name);
    this.coordinator = coordinator;

    actions.put("create", new Action() {
      @Override
      protected void action(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        doCreate(req, resp);
      }
    });
    actions.put("add_version", new Action() {
      @Override
      protected void action(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        doAddVersion(req, resp);
      }
    });
    actions.put("unassign", new Action() {
      @Override
      protected void action(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        doUnassign(req, resp);
      }
    });
    actions.put("delete", new Action() {
      @Override
      protected void action(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        doDeleteDomain(req, resp);
      }
    });
  }

  protected void doDeleteDomain(HttpServletRequest req, HttpServletResponse resp) throws IOException {
    String dgName = URLEnc.decode(req.getParameter("name"));

    if (coordinator.deleteDomainGroup(dgName)) {
      resp.sendRedirect("/domain_groups.jsp");
    } else {
      resp.sendRedirect("/domain_group.jsp?n=" + dgName);
    }
  }

  private void doAddVersion(HttpServletRequest req, HttpServletResponse resp) throws IOException {
    String dgName = URLEnc.decode(req.getParameter("n"));

    DomainGroup dg = coordinator.getDomainGroup(dgName);

    Map<Domain, VersionOrAction> domainVersions = new HashMap<Domain, VersionOrAction>();
    for (Domain domain : coordinator.getDomains()) {
      String version = req.getParameter(domain.getName() + "_version");
      if (version == null) {
        continue;
      }
      domainVersions.put(domain, new VersionOrAction(Integer.parseInt(version)));
    }

    dg.createNewVersion(domainVersions);

    resp.sendRedirect("/domain_group.jsp?n=" + req.getParameter("n"));
  }

  private void doCreate(HttpServletRequest req, HttpServletResponse response) throws IOException {
    coordinator.addDomainGroup(req.getParameter("name"));
    response.sendRedirect("/domain_groups.jsp");
  }

  private void doUnassign(HttpServletRequest req, HttpServletResponse resp) {
    throw new NotImplementedException();
  }
}
