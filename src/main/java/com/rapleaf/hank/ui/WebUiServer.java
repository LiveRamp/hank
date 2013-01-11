package com.rapleaf.hank.ui;

import com.rapleaf.hank.config.ClientConfigurator;
import com.rapleaf.hank.config.yaml.YamlClientConfigurator;
import com.rapleaf.hank.config.yaml.YamlMonitorConfigurator;
import com.rapleaf.hank.coordinator.Coordinator;
import com.rapleaf.hank.monitor.Monitor;
import com.rapleaf.hank.ui.controllers.*;
import com.rapleaf.hank.util.CommandLineChecker;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.webapp.WebAppContext;

import java.net.URL;

public class WebUiServer {
  @SuppressWarnings("unused")
  private static final Logger LOG = Logger.getLogger(WebUiServer.class);

  private final Coordinator coordinator;
  private final int port;

  private final IClientCache clientCache;

  public WebUiServer(Coordinator coordinator, IClientCache clientCache, int port) {
    this.coordinator = coordinator;
    this.clientCache = clientCache;
    this.port = port;
  }

  void run() throws Exception {
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
    CommandLineChecker.check(args, new String[]{"web_ui_configuration_file_path", "monitor_configuration_file_path",
        "port"}, WebUiServer.class);
    Logger.getLogger("com.rapleaf.hank").setLevel(Level.INFO);
    String clientConfigPath = args[0];
    String monitorConfigPath = args[1];
    int port = Integer.parseInt(args[2]);
    ClientConfigurator webUiConfigurator = new YamlClientConfigurator(clientConfigPath);
    Coordinator coordinator = webUiConfigurator.createCoordinator();
    new Monitor(coordinator, new YamlMonitorConfigurator(monitorConfigPath));
    new WebUiServer(coordinator, new ClientCache(coordinator), port).run();
  }
}
