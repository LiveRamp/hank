package com.liveramp.hank.ui.controllers;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public abstract class Action {

  protected abstract void action(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException;

  protected void redirect(String uri, HttpServletResponse resp) throws IOException {
    resp.sendRedirect(uri);
    resp.setStatus(HttpServletResponse.SC_OK);
  }
}
