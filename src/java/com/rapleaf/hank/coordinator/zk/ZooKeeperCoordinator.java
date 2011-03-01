/**
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
package com.rapleaf.hank.coordinator.zk;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;

import com.rapleaf.hank.coordinator.Coordinator;
import com.rapleaf.hank.coordinator.CoordinatorFactory;
import com.rapleaf.hank.coordinator.DomainConfig;
import com.rapleaf.hank.coordinator.DomainGroupChangeListener;
import com.rapleaf.hank.coordinator.DomainGroupConfig;
import com.rapleaf.hank.coordinator.RingGroupChangeListener;
import com.rapleaf.hank.coordinator.RingGroupConfig;
import com.rapleaf.hank.exception.DataNotFoundException;
import com.rapleaf.hank.zookeeper.ZooKeeperConnection;

/**
 * An implementation of the Coordinator built on top of the Apache ZooKeeper
 * service. The ZooKeeperCoordinator initially loads all the configuration into
 * local memory for fast reads. It places watches on nodes in the ZooKeeper
 * service so that it is updated when data is changed, so that it can update its
 * local cache and also notify any listeners that are listening on the data.
 * Currently responds to changes in version number for domains and domain
 * groups, as well as the addition or removal of rings. However, the current
 * implementation of ZooKeeperCoordinator will not respond to addition or
 * removal of domains, domain groups, ring groups, or hosts.
 */
public class ZooKeeperCoordinator extends ZooKeeperConnection implements Coordinator, DomainGroupChangeListener, RingGroupChangeListener {
  private static final Logger LOG = Logger.getLogger(ZooKeeperCoordinator.class);

  public static final class Factory implements CoordinatorFactory {
    @Override
    public Coordinator getCoordinator(Map<String, Object> options) {
      try {
        return new ZooKeeperCoordinator((String)options.get("connect_string"),
            (Integer)options.get("session_timeout"),
            (String)options.get("domains_root"),
            (String)options.get("domain_groups_root"),
            (String)options.get("ring_groups_root"));
      } catch (Exception e) {
        throw new RuntimeException("Couldn't make a ZooKeeperCoordinator from options " + options, e);
      }
    }
  }

  /**
   * We save our watchers so that we can reregister them in case of session
   * expiry.
   */
  private Set<ZooKeeperWatcher> myWatchers = new HashSet<ZooKeeperWatcher>();
  private boolean isSessionExpired = false;

  private final Map<String, ZkDomainConfig> domainConfigsByName =
    new HashMap<String, ZkDomainConfig>();;
  private final Map<String, ZkDomainGroupConfig> domainGroupConfigs =
    new HashMap<String, ZkDomainGroupConfig>();
  private final Map<String, ZkRingGroupConfig> ringGroupConfigs =
    new HashMap<String, ZkRingGroupConfig>();

  private final String domainsRoot;
  private final String domainGroupsRoot;
  private final String ringGroupsRoot;

  /**
   * Blocks until the connection to the ZooKeeper service has been established.
   * See {@link ZooKeeperConnection#ZooKeeperConnection(String, int)}
   * 
   * Package-private constructor that is mainly used for testing. The last
   * boolean flag allows you to prevent the ZooKeeperCoordinator from
   * immediately trying to cache all the configuration information from the
   * ZooKeeper service, which is useful if you don't want to have to setup your
   * entire configuration just to run a few simple tests.
   * 
   * @param zkConnectString
   *          comma separated host:port pairs, each corresponding to a ZooKeeper
   *          server. e.g. "127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002"
   * @param sessionTimeoutMs
   *          session timeout in milliseconds
   * @param domainsRoot TODO
   * @param domainGroupsRoot TODO
   * @param ringGroupsRoot TODO
   * @throws InterruptedException
   * @throws KeeperException 
   * @throws IOException 
   */
  ZooKeeperCoordinator(String zkConnectString, int sessionTimeoutMs, String domainsRoot, String domainGroupsRoot, String ringGroupsRoot) throws InterruptedException, KeeperException, IOException {
    super(zkConnectString, sessionTimeoutMs);
    this.domainsRoot = domainsRoot;
    this.domainGroupsRoot = domainGroupsRoot;
    this.ringGroupsRoot = ringGroupsRoot;

    loadAllDomains();
    loadAllDomainGroups();
    loadAllRingGroups();
  }

  //
  // Daemons
  //

  @Override
  protected void onConnect() {
    // if the session expired, then we need to reregister all of our
    // StateChangeListeners
    if (isSessionExpired) {
      for (ZooKeeperWatcher watcher : myWatchers) {
        watcher.register();
      }
      isSessionExpired = false;
    }
  }

  @Override
  protected void onSessionExpire() {
    isSessionExpired = true;
  }

  @Override
  public DomainConfig getDomainConfig(String domainName) throws DataNotFoundException {
    DomainConfig domain;
    if ((domain = domainConfigsByName.get(domainName)) == null) {
      throw new DataNotFoundException("The domain " + domainName + " does not exist");
    }
    return domain;
  }

  @Override
  public DomainGroupConfig getDomainGroupConfig(String domainGroupName) throws DataNotFoundException {
    DomainGroupConfig dg;
    if ((dg = domainGroupConfigs.get(domainGroupName)) == null) {
      throw new DataNotFoundException("The domain group " + domainGroupName
          + " does not exist");
    }
    return dg;
  }

  @Override
  public RingGroupConfig getRingGroupConfig(String ringGroupName) throws DataNotFoundException {
    RingGroupConfig rg;
    if ((rg = ringGroupConfigs.get(ringGroupName)) == null) {
      throw new DataNotFoundException("The ring group " + ringGroupName
          + " does not exist");
    }
    return rg;
  }

  /**
   * Completely reloads the config information stored in ZooKeeper into memory.
   * Discards all existing config information.
   * 
   * @throws InterruptedException
   * @throws KeeperException 
   */
  private void loadAllDomains() throws InterruptedException, KeeperException {
    List<String> domainNames = zk.getChildren(domainsRoot, false);
    for (String domainName : domainNames) {
      try {
        domainConfigsByName.put(domainName, new ZkDomainConfig(zk, domainsRoot + "/" + domainName));
      } catch (DataNotFoundException e) {
        // Perhaps someone deleted the node while we were loading (unlikely)
        LOG.warn("A node disappeared while we were loading domain configs into memory.", e);
      }
    }
  }

  private void loadAllDomainGroups() throws InterruptedException, KeeperException {
    List<String> domainGroupNameList = zk.getChildren(domainGroupsRoot, false);
    for (String domainGroupName : domainGroupNameList) {
      try {
        domainGroupConfigs.put(domainGroupName, new ZkDomainGroupConfig(zk, domainGroupsRoot + "/" + domainGroupName));
      } catch (DataNotFoundException e) {
        // Perhaps someone deleted the node while we were loading (unlikely)
        LOG.warn("A node disappeared while we were loading domain group configs into memory.", e);
      }
    }
  }

  private void loadAllRingGroups() throws InterruptedException, KeeperException {
    List<String> ringGroupNameList = zk.getChildren(ringGroupsRoot, false);
    for (String ringGroupName : ringGroupNameList) {
      String ringGroupPath = ringGroupsRoot + "/" + ringGroupName;
      ZkDomainGroupConfig dgc = domainGroupConfigs.get(new String(zk.getData(ringGroupPath, false, null)));
      ringGroupConfigs.put(ringGroupName, new ZkRingGroupConfig(zk, ringGroupPath, dgc));
    }
  }

  /**
   * Interface that extends {@link Watcher} to add a
   * {@link ZooKeeperWatcher#register()} method that allows the
   * <code>Watcher</code> to be easily re-registered in case of a session
   * expiry. Also provides a uniform interface for all the different types of
   * ZooKeeperWatchers.
   */
  private interface ZooKeeperWatcher extends Watcher {
    public void register();
  }

  @Override
  public Set<DomainConfig> getDomainConfigs() {
    return new HashSet<DomainConfig>(domainConfigsByName.values());
  }

  @Override
  public Set<DomainGroupConfig> getDomainGroupConfigs() {
    return new HashSet<DomainGroupConfig>(domainGroupConfigs.values());
  }

  public Set<RingGroupConfig> getRingGroups() {
    return new HashSet<RingGroupConfig>(ringGroupConfigs.values());
  }

  @Override
  public void onDomainGroupChange(DomainGroupConfig newDomainGroup) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void onRingGroupChange(RingGroupConfig newRingGroup) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void addDomain(String domainName, int numParts,
      String storageEngineFactoryName, String storageEngineOptions,
      String partitionerName, int initialVersion)
  throws IOException {
    try {
      ZkDomainConfig domain = (ZkDomainConfig) ZkDomainConfig.create(zk, domainsRoot, domainName, numParts, storageEngineFactoryName, storageEngineOptions, partitionerName, initialVersion);
      domainConfigsByName.put(domainName, domain);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public void addDomainGroup(String name) throws IOException {
    try {
      ZkDomainGroupConfig dgc = ZkDomainGroupConfig.create(zk, domainGroupsRoot, name);
      domainGroupConfigs.put(name, dgc);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }
}
