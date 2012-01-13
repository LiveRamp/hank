package com.rapleaf.hank.ui.controllers;

import com.rapleaf.hank.coordinator.*;
import com.rapleaf.hank.partition_assigner.PartitionAssigner;
import com.rapleaf.hank.partition_assigner.UniformPartitionAssigner;
import com.rapleaf.hank.ui.URLEnc;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

public class RingController extends Controller {

  private final Coordinator coordinator;

  public RingController(String name, Coordinator coordinator) {
    super(name);
    this.coordinator = coordinator;
    actions.put("add_host", new Action() {
      @Override
      protected void action(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        doAddHost(req, resp);
      }
    });

    actions.put("delete_host", new Action() {
      @Override
      protected void action(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        doDeleteHost(req, resp);
      }
    });

    actions.put("redistribute_partitions_for_ring", new Action() {
      @Override
      protected void action(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        doRedistributePartitionsForRing(req, resp);
      }
    });

    actions.put("command_all", new Action() {
      @Override
      protected void action(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        doCommandAll(req, resp);
      }
    });
  }

  protected void doRedistributePartitionsForRing(HttpServletRequest req, HttpServletResponse resp) throws IOException {
    RingGroup rg = coordinator.getRingGroup(req.getParameter("g"));
    int ringNum = Integer.parseInt(req.getParameter("n"));
    Ring ring = rg.getRing(ringNum);
    PartitionAssigner partitionAssigner = new UniformPartitionAssigner();
    String versionStr = req.getParameter("v");
    DomainGroupVersion version;
    if (versionStr == null) {
      // Assign current version (rebalance)
      version = rg.getTargetVersion();
    } else {
      // Assign specified version
      int versionNumber = Integer.parseInt(versionStr);
      version = rg.getDomainGroup().getVersionByNumber(versionNumber);
    }
    // Perform assignment
    partitionAssigner.assign(ring, version);

    resp.sendRedirect(String.format("/ring_partitions.jsp?g=%s&n=%d", rg.getName(), ringNum));
  }

  protected void doDeleteHost(HttpServletRequest req, HttpServletResponse resp) throws IOException {
    RingGroup ringGroup = coordinator.getRingGroup(req.getParameter("g"));
    Ring ring = ringGroup.getRing(Integer.parseInt(req.getParameter("n")));
    ring.removeHost(PartitionServerAddress.parse(URLEnc.decode(req.getParameter("h"))));

    resp.sendRedirect(String.format("/ring.jsp?g=%s&n=%d", ringGroup.getName(),
        ring.getRingNumber()));
  }

  private void doAddHost(HttpServletRequest req, HttpServletResponse resp) throws IOException {
    String rgName = req.getParameter("rgName");
    int ringNum = Integer.parseInt(req.getParameter("ringNum"));
    String hostname = req.getParameter("hostname");
    int portNum = Integer.parseInt(req.getParameter("port"));
    coordinator.getRingGroup(rgName).getRing(ringNum).addHost(
        new PartitionServerAddress(hostname, portNum));
    resp.sendRedirect("/ring.jsp?g=" + rgName + "&n=" + ringNum);
  }

  private void doCommandAll(HttpServletRequest req, HttpServletResponse resp) throws IOException {
    String rgName = req.getParameter("rgName");
    int ringNum = Integer.parseInt(req.getParameter("ringNum"));
    HostCommand command = HostCommand.valueOf(req.getParameter("command"));
    Rings.commandAll(coordinator.getRingGroup(rgName).getRing(ringNum), command);
    resp.sendRedirect("/ring.jsp?g=" + rgName + "&n=" + ringNum);
  }
}
