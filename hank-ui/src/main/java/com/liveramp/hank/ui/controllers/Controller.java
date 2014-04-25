package com.liveramp.hank.ui.controllers;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;


public abstract class Controller extends HttpServlet {

  private final String name;
  protected final Map<String, Action> actions = new HashMap<String, Action>();

  public Controller(String name) {
    this.name = name;
  }

  public void addServlet(ServletContextHandler servletContextHandler) {
    servletContextHandler.addServlet(new ServletHolder(this), "/" + name + "/*");
  }

  @Override
  protected final void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
    doAction(request, response);
  }

  @Override
  protected final void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
    doAction(request, response);
  }

  private final void doAction(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
    String[] uriSplits = request.getRequestURI().split("/");
    if (uriSplits.length < 3) {
      invalidRequestFormat(request, response);
      return;
    }
    String actionStr = uriSplits[2];
    Action action = actions.get(actionStr);
    if (action == null) {
      unknownAction(actionStr, response);
      return;
    } else {
      response.setStatus(HttpServletResponse.SC_OK);
      action.action(request, response);
    }
  }

  private final void invalidRequestFormat(HttpServletRequest request, HttpServletResponse response) throws IOException {
    response.sendError(400, "Incorrectly formatted request: " + request.getRequestURI());
  }

  private final void unknownAction(String action, HttpServletResponse response) throws IOException {
    response.sendError(404, "Unknown action: " + action);
  }
}
