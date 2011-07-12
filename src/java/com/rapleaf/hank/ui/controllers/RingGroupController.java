package com.rapleaf.hank.ui.controllers;

import java.io.IOException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.rapleaf.hank.coordinator.Coordinator;
import com.rapleaf.hank.coordinator.Ring;
import com.rapleaf.hank.coordinator.RingGroup;
import com.rapleaf.hank.ui.URLEnc;

public class RingGroupController extends Controller {

  private final Coordinator coordinator;

  public RingGroupController(String name, Coordinator coordinator) {
    super(name);
    this.coordinator = coordinator;

    actions.put("create", new Action() {
      @Override
      protected void action(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        doCreate(req, resp);
      }
    });
    actions.put("add_ring", new Action() {
      @Override
      protected void action(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        doAddRing(req, resp);
      }
    });
    actions.put("delete_ring", new Action() {
      @Override
      protected void action(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        doDeleteRing(req, resp);
      }
    });
  }

  protected void doDeleteRing(HttpServletRequest req, HttpServletResponse resp) throws IOException {
    RingGroup ringGroupConfig;
    String encodedRingGroupName = req.getParameter("g");

    ringGroupConfig = coordinator.getRingGroupConfig(URLEnc.decode(encodedRingGroupName));
    if (ringGroupConfig == null) {
      throw new IOException("couldn't find any ring group called "
          + URLEnc.decode(encodedRingGroupName));
    }
    final Ring ring = ringGroupConfig.getRing(Integer.parseInt(req.getParameter("n")));
    ring.delete();
    resp.sendRedirect("/ring_group.jsp?name=" + encodedRingGroupName);
  }

  private void doAddRing(HttpServletRequest req, HttpServletResponse resp) throws IOException {
    RingGroup ringGroupConfig;
    String encodedRingGroupName = req.getParameter("g");

    ringGroupConfig = coordinator.getRingGroupConfig(URLEnc.decode(encodedRingGroupName));
    if (ringGroupConfig == null) {
      throw new IOException("couldn't find any ring group called "
          + URLEnc.decode(encodedRingGroupName));
    }
    ringGroupConfig.addRing(ringGroupConfig.getRings().size() + 1);
    resp.sendRedirect("/ring_group.jsp?name=" + encodedRingGroupName);
  }

  private void doCreate(HttpServletRequest req, HttpServletResponse resp) throws IOException {
    coordinator.addRingGroup(req.getParameter("rgName"), req.getParameter("dgName"));
    // could log the rg...
    resp.sendRedirect("/ring_groups.jsp");
  }
}
