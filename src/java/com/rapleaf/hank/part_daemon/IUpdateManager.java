package com.rapleaf.hank.part_daemon;

import java.io.IOException;

import com.rapleaf.hank.exception.DataNotFoundException;

public interface IUpdateManager {
  public void update() throws DataNotFoundException, IOException;
}
