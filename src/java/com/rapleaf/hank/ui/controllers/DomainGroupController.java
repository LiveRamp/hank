package com.rapleaf.hank.ui.controllers;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.rapleaf.hank.coordinator.Coordinator;
import com.rapleaf.hank.coordinator.DomainConfig;
import com.rapleaf.hank.coordinator.DomainGroupConfig;
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
    actions.put("add_domain", new Action() {
      @Override
      protected void action(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        doAddDomain(req, resp);
      }
    });
    actions.put("add_version", new Action() {
      @Override
      protected void action(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        doAddVersion(req, resp);
      }
    });
  }

  private void doAddVersion(HttpServletRequest req, HttpServletResponse resp) throws IOException {
    String dgName = URLEnc.decode(req.getParameter("n"));

    DomainGroupConfig dg = coordinator.getDomainGroupConfig(dgName);

    Map<String, Integer> domainVersions = new HashMap<String, Integer>();
    for (DomainConfig domainConfig : dg.getDomainConfigs()) {
      int v = Integer.parseInt(req.getParameter(domainConfig.getName() + "_version"));
      domainVersions.put(domainConfig.getName(), v);
    }
    dg.createNewVersion(domainVersions);

    resp.sendRedirect("/domain_group.jsp?n=" + req.getParameter("n"));
  }

  private void doAddDomain(HttpServletRequest req, HttpServletResponse resp) throws IOException {
    String dgName = URLEnc.decode(req.getParameter("n"));
    String dName = URLEnc.decode(req.getParameter("d"));
    DomainGroupConfig dg = coordinator.getDomainGroupConfig(dgName);
    DomainConfig domainConfig = coordinator.getDomainConfig(dName);

    int domainId = -1;
    for (DomainConfig dc : dg.getDomainConfigs()) {
      int thisDomainId = dg.getDomainId(dc.getName());
      if (thisDomainId > domainId) {
        domainId = thisDomainId;
      }
    }
    domainId++;

    dg.addDomain(domainConfig, domainId);
    resp.sendRedirect("/domain_group.jsp?n=" + req.getParameter("n"));
  }

  private void doCreate(HttpServletRequest req, HttpServletResponse response) throws IOException {
    coordinator.addDomainGroup(req.getParameter("name"));
    response.sendRedirect("/domain_groups.jsp");
  }
}
