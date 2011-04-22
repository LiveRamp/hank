package com.rapleaf.hank.ui;

import java.net.URL;

import org.apache.log4j.Logger;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.webapp.WebAppContext;

import com.rapleaf.hank.config.ClientConfigurator;
import com.rapleaf.hank.config.yaml.YamlClientConfigurator;

public class StatusWebDaemon {
  private static final Logger LOG = Logger.getLogger(StatusWebDaemon.class);

  private final ClientConfigurator cc;
  private final int port;

  public StatusWebDaemon(ClientConfigurator cc, int port) {
    this.cc = cc;
    this.port = port;
  }

  void run() throws Exception {
    Server server = new Server(port);
    Package p = StatusWebDaemon.class.getPackage();
    String pName = p.getName();
    String pPath = pName.replaceAll("\\.", "/");
    final URL warUrl = getClass().getClassLoader().getResource(pPath);
    final String warUrlString = warUrl.toExternalForm();

    WebAppContext webAppContext = new WebAppContext(warUrlString, "/");
    webAppContext.setAttribute("coordinator", cc.getCoordinator());
    server.setHandler(webAppContext);
    server.start();

    while(true) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        LOG.debug(e);
        break;
      }
    }
  }

  public static void main(String[] args) throws Exception {
    String clientConfigPath = args[0];
    int port = Integer.parseInt(args[1]);
    ClientConfigurator cc = new YamlClientConfigurator(clientConfigPath);
    new StatusWebDaemon(cc, port).run();
  }
}
