package com.rapleaf.hank.ui;

import java.io.IOException;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.rapleaf.hank.coordinator.Coordinator;
import com.rapleaf.hank.coordinator.DomainConfig;
import com.rapleaf.hank.coordinator.DomainGroupConfig;
import com.rapleaf.hank.coordinator.DomainGroupConfigVersion;
import com.rapleaf.hank.exception.DataNotFoundException;
import com.rapleaf.hank.ui.controller.Action;
import com.rapleaf.hank.ui.controller.Controller;

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
    String dgName = URLDecoder.decode(req.getParameter("n"));
    try {
      DomainGroupConfig dg = coordinator.getDomainGroupConfig(dgName);

      Map<String, Integer> domainVersions = new HashMap<String, Integer>();
      for (DomainConfig domainConfig : dg.getDomainConfigs()) {
        int v = Integer.parseInt(req.getParameter(domainConfig.getName() + "_version"));
        domainVersions.put(domainConfig.getName(), v);
      }
      DomainGroupConfigVersion newVersion = dg.createNewVersion(domainVersions);

      resp.sendRedirect("/domain_group.jsp?n=" + req.getParameter("n"));
    } catch (DataNotFoundException e) {
      throw new IOException(e);
    }
  }

  private void doAddDomain(HttpServletRequest req, HttpServletResponse resp) throws IOException {
    String dgName = URLDecoder.decode(req.getParameter("n"));
    String dName = URLDecoder.decode(req.getParameter("d"));
    try {
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
    } catch (DataNotFoundException e) {
      throw new IOException(e);
    }
  }

  private void doCreate(HttpServletRequest req, HttpServletResponse response) throws IOException {
    coordinator.addDomainGroup(req.getParameter("name"));
    response.sendRedirect("/domain_groups.jsp");
  }
}
