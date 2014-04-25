package com.liveramp.hank.ui.controllers;

import com.liveramp.hank.coordinator.Coordinator;
import com.liveramp.hank.coordinator.Ring;
import com.liveramp.hank.coordinator.RingGroup;
import com.liveramp.hank.ring_group_conductor.RingGroupConductorMode;
import com.liveramp.hank.ui.URLEnc;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

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
    actions.put("delete_ring_group", new Action() {
      @Override
      protected void action(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        doDeleteRingGroup(req, resp);
      }
    });
    actions.put("set_ring_group_conductor_mode", new Action() {
      @Override
      protected void action(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        doSetRingGroupConductorMode(req, resp);
      }
    });
  }

  protected void doDeleteRing(HttpServletRequest req, HttpServletResponse resp) throws IOException {
    RingGroup ringGroup;
    String encodedRingGroupName = req.getParameter("g");

    ringGroup = coordinator.getRingGroup(URLEnc.decode(encodedRingGroupName));
    if (ringGroup == null) {
      throw new IOException("couldn't find any ring group called "
          + URLEnc.decode(encodedRingGroupName));
    }
    ringGroup.removeRing(Integer.parseInt(req.getParameter("n")));
    resp.sendRedirect("/ring_group.jsp?name=" + encodedRingGroupName);
  }

  private void doAddRing(HttpServletRequest req, HttpServletResponse resp) throws IOException {
    RingGroup ringGroup;
    String encodedRingGroupName = req.getParameter("g");

    ringGroup = coordinator.getRingGroup(URLEnc.decode(encodedRingGroupName));
    if (ringGroup == null) {
      throw new IOException("couldn't find any ring group called "
          + URLEnc.decode(encodedRingGroupName));
    }
    // Find new ring ID (largest ID + 1)
    int newRingID = 0;
    for (Ring ring : ringGroup.getRings()) {
      if (ring.getRingNumber() >= newRingID) {
        newRingID = ring.getRingNumber() + 1;
      }
    }
    ringGroup.addRing(newRingID);
    resp.sendRedirect("/ring_group.jsp?name=" + encodedRingGroupName);
  }

  private void doCreate(HttpServletRequest req, HttpServletResponse resp) throws IOException {
    coordinator.addRingGroup(req.getParameter("rgName"), req.getParameter("dgName"));
    // could log the rg...
    resp.sendRedirect("/ring_groups.jsp");
  }

  protected void doDeleteRingGroup(HttpServletRequest req, HttpServletResponse resp) throws IOException {
    String encodedRingGroupName = req.getParameter("g");
    coordinator.deleteRingGroup(URLEnc.decode(encodedRingGroupName));
    resp.sendRedirect("/ring_groups.jsp");
  }

  protected void doSetRingGroupConductorMode(HttpServletRequest req, HttpServletResponse resp) throws IOException {
    String encodedRingGroupName = req.getParameter("g");
    RingGroup ringGroup = coordinator.getRingGroup(URLEnc.decode(encodedRingGroupName));
    // Only change the mode if it's not OFFLINE
    if (ringGroup.getRingGroupConductorMode() != null &&
        ringGroup.getRingGroupConductorMode() != RingGroupConductorMode.OFFLINE) {
      ringGroup.setRingGroupConductorMode(RingGroupConductorMode.valueOf(URLEnc.decode(req.getParameter("mode"))));
    }
    resp.sendRedirect("/ring_group.jsp?name=" + encodedRingGroupName);
  }

  public static String getRingGroupUrl(RingGroup ringGroup) {
    return "ring_group.jsp?name=" + URLEnc.encode(ringGroup.getName());
  }
}
