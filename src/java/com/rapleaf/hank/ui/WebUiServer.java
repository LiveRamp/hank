package com.rapleaf.hank.ui;

import java.net.URL;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.webapp.WebAppContext;

import com.rapleaf.hank.config.ClientConfigurator;
import com.rapleaf.hank.config.yaml.YamlClientConfigurator;
import com.rapleaf.hank.coordinator.Coordinator;
import com.rapleaf.hank.ui.controllers.DomainController;
import com.rapleaf.hank.ui.controllers.DomainGroupController;
import com.rapleaf.hank.ui.controllers.HostController;
import com.rapleaf.hank.ui.controllers.RingController;
import com.rapleaf.hank.ui.controllers.RingGroupController;
import com.rapleaf.hank.util.CommandLineChecker;

public class WebUiServer {
  @SuppressWarnings("unused")
  private static final Logger LOG = Logger.getLogger(WebUiServer.class);

  private final ClientConfigurator cc;
  private final int port;

  private final IClientCache clientCache;

  public WebUiServer(ClientConfigurator cc, IClientCache clientCache, int port) {
    this.cc = cc;
    this.clientCache = clientCache;
    this.port = port;
  }

  void run() throws Exception {
    Coordinator coordinator = cc.getCoordinator();

    // get the server
    Server server = new Server(port);

    // configure the web app context (for the jsps)
    Package p = WebUiServer.class.getPackage();
    String pName = p.getName();
    String pPath = pName.replaceAll("\\.", "/");
    final URL warUrl = getClass().getClassLoader().getResource(pPath);
    final String warUrlString = warUrl.toExternalForm();
    WebAppContext webAppContext = new WebAppContext(warUrlString, "/");
    webAppContext.setAttribute("coordinator", coordinator);
    webAppContext.setAttribute("clientCache", clientCache);

    // API context
    HankApiServlet apiServlet = new HankApiServlet(coordinator);
    ServletHolder apiHolder = new ServletHolder(apiServlet);

    // get the controller servlet (for the "controller" methods)
    ServletContextHandler servletHandler = new ServletContextHandler();
    servletHandler.setContextPath("/");

    servletHandler.addServlet(apiHolder, "/api");

    new DomainController("domain", coordinator).addServlet(servletHandler);
    new DomainGroupController("domain_group", coordinator).addServlet(servletHandler);
    new RingGroupController("ring_group", coordinator).addServlet(servletHandler);
    new RingController("ring", coordinator).addServlet(servletHandler);
    new HostController("host", coordinator).addServlet(servletHandler);

    // put them together into a context handler
    ContextHandlerCollection contexts = new ContextHandlerCollection();
    contexts.setHandlers(new Handler[]{servletHandler, webAppContext});
    server.setHandler(contexts);

    server.start();
    server.join();
  }

  public static void main(String[] args) throws Exception {
    CommandLineChecker.check(args, new String[]{"configuration_file_path", "port"}, WebUiServer.class);
    Logger.getLogger("com.rapleaf.hank").setLevel(Level.INFO);
    String clientConfigPath = args[0];
    int port = Integer.parseInt(args[1]);
    ClientConfigurator cc = new YamlClientConfigurator(clientConfigPath);
    new WebUiServer(cc, new ClientCache(cc.getCoordinator()), port).run();
  }
}
