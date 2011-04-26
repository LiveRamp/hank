package com.rapleaf.hank.ui;

import java.io.IOException;
import java.net.URL;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

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

public class StatusWebDaemon {
  private static final class DomainControllerServlet extends HttpServlet {

    private final Coordinator coordinator;

    public DomainControllerServlet(Coordinator coordinator) {
      this.coordinator = coordinator;
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
      String domainName = req.getParameter("name");
      int numParts = Integer.parseInt(req.getParameter("numParts"));
      String storageEngineFactoryName = req.getParameter("storageEngineFactoryName");
      String storageEngineOptions = req.getParameter("storageEngineOptions");
      String partitionerName = req.getParameter("partitionerName");
//      int initialVersion = Integer.parseInt(req.getParameter("initialVersion"));
      System.out.println(coordinator.addDomain(domainName, numParts, storageEngineFactoryName, storageEngineOptions, partitionerName, 1));

      resp.sendRedirect("/domains.jsp");
      resp.setStatus(HttpServletResponse.SC_OK);
    }
  }

  @SuppressWarnings("unused")
  private static final Logger LOG = Logger.getLogger(StatusWebDaemon.class);

  private final ClientConfigurator cc;
  private final int port;

  public StatusWebDaemon(ClientConfigurator cc, int port) {
    this.cc = cc;
    this.port = port;
  }

  void run() throws Exception {
    Coordinator coordinator = cc.getCoordinator();

    // get the server
    Server server = new Server(port);

    // configure the web app context (for the jsps)
    Package p = StatusWebDaemon.class.getPackage();
    String pName = p.getName();
    String pPath = pName.replaceAll("\\.", "/");
    final URL warUrl = getClass().getClassLoader().getResource(pPath);
    final String warUrlString = warUrl.toExternalForm();
    WebAppContext webAppContext = new WebAppContext(warUrlString, "/");
    webAppContext.setAttribute("coordinator", coordinator);

    // get the controller servlet (for the "controller" methods)
    ServletContextHandler servletHandler = new ServletContextHandler();
    servletHandler.setContextPath("/");

    DomainControllerServlet domainControllerServlet = new DomainControllerServlet(coordinator);
    servletHandler.addServlet(new ServletHolder(domainControllerServlet), "/domain/*");

    // put them together into a context handler
    ContextHandlerCollection contexts = new ContextHandlerCollection();
    contexts.setHandlers(new Handler[] {servletHandler, webAppContext});
    server.setHandler(contexts);

    server.start();
    server.join();
  }

  public static void main(String[] args) throws Exception {
    String clientConfigPath = args[0];
    int port = Integer.parseInt(args[1]);
    ClientConfigurator cc = new YamlClientConfigurator(clientConfigPath);
    new StatusWebDaemon(cc, port).run();
  }
}
