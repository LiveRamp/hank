/*
 *  Copyright 2011 Rapleaf
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
package com.rapleaf.hank.client;

import com.rapleaf.hank.generated.HankBulkResponse;
import com.rapleaf.hank.generated.HankException;
import com.rapleaf.hank.generated.HankResponse;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class PartitionServerConnectionSet {
  private static final HankResponse NO_CONNECTION_AVAILABLE = HankResponse.xception(HankException.no_connection_available(true));
  private static final HankBulkResponse NO_CONNECTION_AVAILABLE_BULK = HankBulkResponse.xception(HankException.no_connection_available(true));

  private static final Logger LOG = Logger.getLogger(PartitionServerConnectionSet.class);

  private final List<PartitionServerConnection> connections = new ArrayList<PartitionServerConnection>();
  private final AtomicInteger nextIdx = new AtomicInteger(0);

  public PartitionServerConnectionSet(List<PartitionServerConnection> connections) {
    this.connections.addAll(connections);
  }

  public List<PartitionServerConnection> getConnections() {
    return connections;
  }

  private abstract class ConnectionAction {

    protected void execute() {
      int numAttempts = 0;
      int maxAttempts = connections.size();
      LOG.trace("There are " + connections.size() + " connections");
      while (numAttempts < maxAttempts) {
        int pos = nextIdx.getAndIncrement() % connections.size();
        PartitionServerConnection connection = connections.get(pos);
        if (!connection.isAvailable()) {
          LOG.trace("Connection " + connection + " was not available, so skipped it.");
          numAttempts++;
          continue;
        }
        try {
          executeAction(connection);
          break;
        } catch (IOException e) {
          LOG.trace("Failed to execute with connection " + connection + ", so skipped it.", e);
          numAttempts++;
          continue;
        }
      }
      if (numAttempts >= maxAttempts) {
        noConnectionAvailable();
        LOG.trace("None of the " + connections.size() + " connections were available.");
      }
    }

    protected abstract void executeAction(PartitionServerConnection connection) throws IOException;

    protected abstract void noConnectionAvailable();
  }

  private class GetAction extends ConnectionAction {

    private HankResponse result = null;
    private final int domainId;
    private final ByteBuffer key;

    public GetAction(int domainId, ByteBuffer key) {
      this.domainId = domainId;
      this.key = key;
    }

    public HankResponse run() {
      execute();
      return result;
    }

    @Override
    protected void executeAction(PartitionServerConnection connection) throws IOException {
      result = connection.get(domainId, key);
    }

    @Override
    protected void noConnectionAvailable() {
      result = NO_CONNECTION_AVAILABLE;
    }
  }

  private class GetBulkAction extends ConnectionAction {

    private HankBulkResponse result = null;
    private final int domainId;
    private final List<ByteBuffer> keys;

    public GetBulkAction(int domainId, List<ByteBuffer> keys) {
      this.domainId = domainId;
      this.keys = keys;
    }

    public HankBulkResponse run() {
      execute();
      return result;
    }

    @Override
    protected void executeAction(PartitionServerConnection connection) throws IOException {
      result = connection.getBulk(domainId, keys);
    }

    @Override
    protected void noConnectionAvailable() {
      result = NO_CONNECTION_AVAILABLE_BULK;
    }
  }

  public HankResponse get(int domainId, ByteBuffer key) throws TException {
    return new GetAction(domainId, key).run();
  }

  public HankBulkResponse getBulk(int domainId, List<ByteBuffer> keys) throws TException {
    return new GetBulkAction(domainId, keys).run();
  }
}
