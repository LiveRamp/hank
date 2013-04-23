package com.liveramp.hank.ui.controllers;

import com.liveramp.hank.coordinator.Coordinator;
import com.liveramp.hank.coordinator.Domain;
import com.liveramp.hank.coordinator.DomainGroup;
import com.liveramp.hank.ui.URLEnc;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

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
    actions.put("delete", new Action() {
      @Override
      protected void action(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        doDeleteDomain(req, resp);
      }
    });
    actions.put("update_domain_versions", new Action() {
      @Override
      protected void action(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        doUpdateDomainVersions(req, resp);
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

  private void doCreate(HttpServletRequest req, HttpServletResponse response) throws IOException {
    coordinator.addDomainGroup(req.getParameter("name"));
    response.sendRedirect("/domain_groups.jsp");
  }

  protected void doUpdateDomainVersions(HttpServletRequest req, HttpServletResponse resp) throws IOException {
    String dgName = URLEnc.decode(req.getParameter("name"));
    DomainGroup dg = coordinator.getDomainGroup(dgName);
    Map<Domain, Integer> domainVersions = new HashMap<Domain, Integer>();
    for (Domain domain : coordinator.getDomains()) {
      String version = req.getParameter(domain.getName() + "_version");
      if (version == null) {
        continue;
      }
      domainVersions.put(domain, Integer.parseInt(version));
    }
    dg.setDomainVersions(domainVersions);
    resp.sendRedirect("/domain_group.jsp?n=" + dgName);
  }
}
