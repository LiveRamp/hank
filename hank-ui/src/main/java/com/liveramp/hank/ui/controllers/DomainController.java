/**
 *  Copyright 2011 LiveRamp
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.liveramp.hank.ui.controllers;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

import com.liveramp.hank.coordinator.Coordinator;
import com.liveramp.hank.coordinator.Domain;
import com.liveramp.hank.coordinator.DomainGroup;
import com.liveramp.hank.coordinator.DomainVersion;
import com.liveramp.hank.coordinator.Host;
import com.liveramp.hank.coordinator.HostDomain;
import com.liveramp.hank.coordinator.Hosts;
import com.liveramp.hank.coordinator.Ring;
import com.liveramp.hank.coordinator.RingGroup;

public class DomainController extends Controller {

  private final Coordinator coordinator;

  public DomainController(String name, Coordinator coordinator) {
    super(name);
    this.coordinator = coordinator;
    actions.put("create", new Action() {
      @Override
      protected void action(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        String domainName = req.getParameter("name");
        int numParts = Integer.parseInt(req.getParameter("numParts"));

        String storageEngineFactoryName = req.getParameter("storageEngineFactorySelect");
        if (storageEngineFactoryName.equals("__other__")) {
          storageEngineFactoryName = req.getParameter("storageEngineFactoryName");
        }

        String storageEngineOptions = req.getParameter("storageEngineOptions");
        final String requiredHostFlags = req.getParameter("requiredHostFlags");

        String partitionerName = req.getParameter("partitionerSelect");
        if (partitionerName.equals("__other__")) {
          partitionerName = req.getParameter("partitionerOther");
        }
        DomainController.this.coordinator.addDomain(domainName, numParts, storageEngineFactoryName,
            storageEngineOptions, partitionerName, Hosts.splitHostFlags(requiredHostFlags));
        redirect("/domains.jsp", resp);
      }
    });
    actions.put("delete", new Action() {
      @Override
      protected void action(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        doDeleteDomain(req, resp);
      }
    });
    actions.put("defunctify", new Action() {
      @Override
      protected void action(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        Domain domain = DomainController.this.coordinator.getDomain(req.getParameter("n"));
        final DomainVersion domainVersion = domain.getVersion(Integer.parseInt(req.getParameter("ver")));
        domainVersion.setDefunct(true);
        redirect("/domain.jsp?n=" + req.getParameter("n"), resp);
      }
    });
    actions.put("undefunctify", new Action() {
      @Override
      protected void action(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        Domain domain = DomainController.this.coordinator.getDomain(req.getParameter("n"));
        final DomainVersion domainVersion = domain.getVersion(Integer.parseInt(req.getParameter("ver")));
        domainVersion.setDefunct(false);
        redirect("/domain.jsp?n=" + req.getParameter("n"), resp);
      }
    });
    actions.put("close", new Action() {
      @Override
      protected void action(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        Domain domain = DomainController.this.coordinator.getDomain(req.getParameter("n"));
        final DomainVersion domainVersion = domain.getVersion(Integer.parseInt(req.getParameter("ver")));
        domainVersion.close();
        redirect("/domain.jsp?n=" + req.getParameter("n"), resp);
      }
    });
    actions.put("delete_version", new Action() {
      @Override
      protected void action(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        Domain domain = DomainController.this.coordinator.getDomain(req.getParameter("n"));
        final Integer domainVersion = Integer.parseInt(req.getParameter("ver"));
        domain.deleteVersion(domainVersion);
        redirect("/domain.jsp?n=" + req.getParameter("n"), resp);
      }
    });
    actions.put("delete_all_defunct_versions", new Action() {
      @Override
      protected void action(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        Domain domain = DomainController.this.coordinator.getDomain(req.getParameter("n"));
        for (DomainVersion domainVersion : domain.getVersions()) {
          if (domainVersion.isDefunct()) {
            domain.deleteVersion(domainVersion.getVersionNumber());
          }
        }
        redirect("/domain.jsp?n=" + req.getParameter("n"), resp);
      }
    });
    actions.put("update", new Action() {
      @Override
      protected void action(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        doUpdateDomain(req, resp);
      }
    });
  }

  private void doDeleteDomain(HttpServletRequest req, HttpServletResponse resp) throws IOException {
    String domainName = req.getParameter("name");
    final Domain domain = coordinator.getDomain(domainName);

    if (!isInUse(domain)) {
      coordinator.deleteDomain(domain.getName());
    }
    resp.sendRedirect("/domains.jsp");
  }

  private boolean isInUse(Domain domain) throws IOException {
    String domainName = domain.getName();

    for (RingGroup rg : coordinator.getRingGroups()) {

      for (Ring ring : rg.getRings()) {
        for (Host host : ring.getHosts()) {
          for (HostDomain hostDomain : host.getAssignedDomains()) {
            if(hostDomain.getDomain().getName().equals(domainName)){
              return true;
            }
          }
        }
      }

      DomainGroup dg = rg.getDomainGroup();
      for (Domain dgd : dg.getDomains()) {
        if(dgd.getName().equals(domainName)){
          return true;
        }
      }

    }

    return false;
  }

  private void doUpdateDomain(HttpServletRequest req, HttpServletResponse resp) throws IOException {
    final String domainName = req.getParameter("name");
    final String partitionerClassName = req.getParameter("partitionerClassName");
    final String requiredHostFlags = req.getParameter("requiredHostFlags");
    final String storageEngineFactoryClassName = req.getParameter("storageEngineFactoryClassName");
    final String storageEngineOptions = req.getParameter("storageEngineOptions");
    final Domain domain = coordinator.getDomain(domainName);
    if (domain == null) {
      throw new IOException("Could not get Domain '" + domainName + "' from Configurator.");
    } else {
      coordinator.updateDomain(domainName,
          domain.getNumParts(),
          storageEngineFactoryClassName,
          storageEngineOptions,
          partitionerClassName,
          Hosts.splitHostFlags(requiredHostFlags));
    }
    resp.sendRedirect("/domains.jsp");
  }
}
