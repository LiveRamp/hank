package com.rapleaf.hank.ui.controllers;

import java.io.IOException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.rapleaf.hank.coordinator.Coordinator;
import com.rapleaf.hank.coordinator.DomainGroupVersionDomainVersion;
import com.rapleaf.hank.coordinator.HostCommand;
import com.rapleaf.hank.coordinator.PartitionServerAddress;
import com.rapleaf.hank.coordinator.Ring;
import com.rapleaf.hank.coordinator.RingGroup;
import com.rapleaf.hank.partition_assigner.EqualSizePartitionAssigner;
import com.rapleaf.hank.partition_assigner.PartitionAssigner;
import com.rapleaf.hank.ui.URLEnc;

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
    RingGroup rgc = coordinator.getRingGroup(req.getParameter("g"));
    int ringNum = Integer.parseInt(req.getParameter("n"));

    PartitionAssigner partitionAssigner = new EqualSizePartitionAssigner();
    for (DomainGroupVersionDomainVersion dc : rgc.getDomainGroup().getLatestVersion().getDomainVersions()) {
      partitionAssigner.assign(rgc, ringNum, dc.getDomain());
    }

    resp.sendRedirect(String.format("/ring_partitions.jsp?g=%s&n=%d", rgc.getName(), ringNum));
  }

  protected void doDeleteHost(HttpServletRequest req, HttpServletResponse resp) throws IOException {
    RingGroup rgc = coordinator.getRingGroup(req.getParameter("g"));
    Ring ringConfig = rgc.getRing(Integer.parseInt(req.getParameter("n")));
    ringConfig.removeHost(PartitionServerAddress.parse(URLEnc.decode(req.getParameter("h"))));

    resp.sendRedirect(String.format("/ring.jsp?g=%s&n=%d", rgc.getName(),
        ringConfig.getRingNumber()));
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
    coordinator.getRingGroup(rgName).getRing(ringNum).commandAll(command);
    resp.sendRedirect("/ring.jsp?g=" + rgName + "&n=" + ringNum);
  }
}
