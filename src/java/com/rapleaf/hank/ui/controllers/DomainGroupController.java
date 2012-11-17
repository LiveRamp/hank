package com.rapleaf.hank.ui.controllers;

import com.rapleaf.hank.coordinator.Coordinator;
import com.rapleaf.hank.ui.URLEnc;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

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
}
