package com.liveramp.hank.ui;

import javax.servlet.DispatcherType;
import java.net.URL;
import java.util.EnumSet;

import org.apache.log4j.Level;
import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger; import org.slf4j.LoggerFactory;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.servlets.GzipFilter;
import org.eclipse.jetty.webapp.WebAppContext;

import com.liveramp.hank.config.CoordinatorConfigurator;
import com.liveramp.hank.config.yaml.YamlCoordinatorConfigurator;
import com.liveramp.hank.config.yaml.YamlMonitorConfigurator;
import com.liveramp.hank.coordinator.Coordinator;
import com.liveramp.hank.monitor.Monitor;
import com.liveramp.hank.ui.controllers.DomainController;
import com.liveramp.hank.ui.controllers.DomainGroupController;
import com.liveramp.hank.ui.controllers.HostController;
import com.liveramp.hank.ui.controllers.RingController;
import com.liveramp.hank.ui.controllers.RingGroupController;
import com.liveramp.hank.util.CommandLineChecker;

public class WebUiServer {
  @SuppressWarnings("unused")
  private static final Logger LOG = LoggerFactory.getLogger(WebUiServer.class);

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

    //  turn on gzip compression
    servletHandler.addFilter(GzipFilter.class, "/*", EnumSet.of(DispatcherType.REQUEST));

    // put them together into a context handler
    ContextHandlerCollection contexts = new ContextHandlerCollection();
    contexts.setHandlers(new Handler[]{servletHandler, webAppContext});
    server.setHandler(contexts);

    server.start();
    server.join();
  }

  public static void main(String[] args) throws Exception {
    CommandLineChecker.check(args, new String[]{"web_ui_configuration_file_path", "monitor_configuration_file_path",
        "port", "log4j_config_file"}, WebUiServer.class);
    org.apache.log4j.Logger.getLogger("com.liveramp.hank").setLevel(Level.INFO);
    String clientConfigPath = args[0];
    String monitorConfigPath = args[1];
    int port = Integer.parseInt(args[2]);
    PropertyConfigurator.configure(args[3]);

    CoordinatorConfigurator webUiConfigurator = new YamlCoordinatorConfigurator(clientConfigPath);
    Coordinator coordinator = webUiConfigurator.createCoordinator();
    new Monitor(coordinator, new YamlMonitorConfigurator(monitorConfigPath));
    new WebUiServer(coordinator, new ClientCache(coordinator), port).run();
  }
}
