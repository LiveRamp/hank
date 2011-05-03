package com.rapleaf.hank.ui;

import java.io.IOException;
import java.net.URLDecoder;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.rapleaf.hank.coordinator.Coordinator;
import com.rapleaf.hank.coordinator.DomainConfig;
import com.rapleaf.hank.coordinator.DomainGroupConfig;
import com.rapleaf.hank.exception.DataNotFoundException;

public class DomainGroupController extends HttpServlet {
  private final Coordinator coordinator;

  public DomainGroupController(Coordinator coordinator) {
    this.coordinator = coordinator;
  }

  @Override
  protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
    if (req.getRequestURI().contains("create")) {
      doCreate(req, resp);
    } else if (req.getRequestURI().contains("add_domain")) {
      doAddDomain(req, resp);
    } else {
      System.out.println("Bad URI!" + req.getRequestURI());
      resp.sendError(404);
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
