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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.data.Stat;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import com.rapleaf.hank.coordinator.Coordinator;
import com.rapleaf.hank.coordinator.DomainChangeListener;
import com.rapleaf.hank.coordinator.DomainConfig;
import com.rapleaf.hank.coordinator.DomainGroupChangeListener;
import com.rapleaf.hank.coordinator.DomainGroupConfig;
import com.rapleaf.hank.coordinator.RingGroupChangeListener;
import com.rapleaf.hank.coordinator.RingGroupConfig;
import com.rapleaf.hank.exception.DataNotFoundException;
import com.rapleaf.hank.util.ZooKeeperUtils;
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
public class ZooKeeperCoordinator extends ZooKeeperConnection implements Coordinator {
  private static final Logger LOG = Logger.getLogger(ZooKeeperCoordinator.class);

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
  private final Map<String, RingGroupConfigImpl> ringGroupConfigs =
    new HashMap<String, RingGroupConfigImpl>();

  private final Map<String, Set<DomainChangeListener>> domainListeners =
    new HashMap<String, Set<DomainChangeListener>>();
  private final Map<String, Set<DomainGroupChangeListener>> domainGroupListeners =
    new HashMap<String, Set<DomainGroupChangeListener>>();
  private final Map<String, Set<RingGroupChangeListener>> ringGroupListeners =
    new HashMap<String, Set<RingGroupChangeListener>>();


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
   */
  ZooKeeperCoordinator(String zkConnectString, int sessionTimeoutMs, String domainsRoot, String domainGroupsRoot, String ringGroupsRoot) throws InterruptedException, KeeperException {
    super(zkConnectString, sessionTimeoutMs);
    this.domainsRoot = domainsRoot;
    this.domainGroupsRoot = domainGroupsRoot;
    this.ringGroupsRoot = ringGroupsRoot;

    loadAllDomains();
    loadAllDomainGroups();
    loadAllRingGroups();

    registerListenersAndWatchers();
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
   */
  private void loadAllDomains() throws InterruptedException {
    ZooKeeperUtils.checkExistsOrDie(zk, domainsRoot);

    List<String> domainNames = ZooKeeperUtils.getChildrenOrDie(zk, domainsRoot);
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
    ZooKeeperUtils.checkExistsOrDie(zk, domainGroupsRoot);

    List<String> domainGroupNameList = ZooKeeperUtils.getChildrenOrDie(zk, domainGroupsRoot);
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
    ZooKeeperUtils.checkExistsOrDie(zk, ringGroupsRoot);

    List<String> ringGroupNameList = ZooKeeperUtils.getChildrenOrDie(zk, ringGroupsRoot);
    for (String ringGroupName : ringGroupNameList) {
      String ringGroupPath = ringGroupsRoot + "/" + ringGroupName;
      ZkDomainGroupConfig dgc = domainGroupConfigs.get(ZooKeeperUtils.getStringOrDie(zk, ringGroupPath));
      ringGroupConfigs.put(ringGroupName, new RingGroupConfigImpl(zk, ringGroupPath, dgc));
    }
  }

  @Override
  public int updateDomain(String domainName) throws DataNotFoundException {
    throw new NotImplementedException();
//    DomainConfig oldDomain;
//    if ((oldDomain = domainConfigsByName.get(domainName)) == null) {
//      throw new DataNotFoundException("Domain " + domainName + " does not exist");
//    }
//    // --Update the domain config
//    // Update our local copy
//    int newDomainVersion = oldDomain.getVersion() + 1;
//    DomainConfigImpl newDomain = new DomainConfigImpl(oldDomain.getName(),
//        oldDomain.getNumParts(),
//        oldDomain.getPartitioner(),
//        oldDomain.getStorageEngine(),
//        newDomainVersion);
//    domainConfigsByName.put(domainName, newDomain);
//    // Update the ZooKeeper Service
//    String path = ZooKeeperUtils.domainPath(null, domainName) + "/version";
//    try {
//      ZooKeeperUtils.setDataOrDie(zk, path, Bytes.intToBytes(newDomainVersion));
//    } catch (InterruptedException e) {
//      // Server is probably going down
//      return -1;
//    }
//
//    pushNewDomain(newDomain);
//
//    // --Update every domain group that has this domain
//    for (DomainGroupConfigImpl dg : domainGroupConfigs.values()) {
//      if (!dg.getDomainConfigMap().values().contains(oldDomain)) {
//        continue;
//      }
//      int domainId = dg.getDomainId(oldDomain.getName());
//      // Update the domain group's set
//      // Clobber the old domain
//      dg.getDomainConfigMap().put(domainId, newDomain);
//
//      // Update the domain group's map
//      throw new NotImplementedException();
////      Map<Integer, Map<Integer, Integer>> versionMap = dg.getVersions();
////      int oldDgVersion = Collections.max(versionMap.keySet());
////      int newDgVersion = oldDgVersion + 1;
////      Map<Integer, Integer> newVersionMap = Collections.synchronizedMap(new HashMap<Integer, Integer>(versionMap.get(oldDgVersion))); //Create a copy of the version map
////      newVersionMap.put(domainId, newDomainVersion); // Update the version map
////      versionMap.put(newDgVersion, newVersionMap); // Add the new version map
////
////      // Update the ZooKeeper Service
////      String versionPath = ZooKeeperUtils.getDomainGroupPath(dg.getName())
////          + "/versions/" + newDgVersion;
////      try {
////        ZooKeeperUtils.createNodeOrFailSilently(zk, versionPath);
////        for (Entry<Integer, Integer> entry : newVersionMap.entrySet()) {
////          ZooKeeperUtils.setDataOrFailSilently(zk, versionPath + '/'
////              + entry.getKey(), Bytes.intToBytes(entry.getValue()));
////        }
////      } catch (InterruptedException e) {
////        // Server is probably going down
////        return -1;
////      }
////
////      pushNewDomainGroup(dg);
//    }

//    return newDomainVersion;
  }

  @Override
  public void addDomainGroupChangeListener(String domainGroupName, DomainGroupChangeListener listener) throws DataNotFoundException {
    if (!domainGroupConfigs.containsKey(domainGroupName)) {
      throw new DataNotFoundException("Domain group " + domainGroupName + " does not exist");
    }
    Set<DomainGroupChangeListener> listeners;
    if ((listeners = domainGroupListeners.get(domainGroupName)) == null) {
      listeners = Collections.synchronizedSet(new HashSet<DomainGroupChangeListener>());
      domainGroupListeners.put(domainGroupName, listeners);
    }
    listeners.add(listener);
  }

  public void addRingGroupChangeListener(String ringGroupName, RingGroupChangeListener listener) throws DataNotFoundException {
    if (!ringGroupConfigs.containsKey(ringGroupName)) {
      throw new DataNotFoundException("Ring group " + ringGroupName + " does not exist");
    }
    Set<RingGroupChangeListener> listeners;
    if ((listeners = ringGroupListeners.get(ringGroupName)) == null) {
      listeners = Collections.synchronizedSet(new HashSet<RingGroupChangeListener>());
      ringGroupListeners.put(ringGroupName, listeners);
    }
    listeners.add(listener);
  }
  
  /**
   * Pushes the domain to all <code>DomainChangeListener</code>s listening on this domain.
   * 
   * @param domain
   */
  private void pushNewDomain(DomainConfig domain) {
    for (DomainChangeListener listener : domainListeners.get(domain.getName())) {
      listener.onDomainChange(domain);
    }
  }
  
  /**
   * Pushes the domain group to all <code>DomainGroupChangeListener</code>s listening on this domain group.
   * 
   * @param dg
   */
  private void pushNewDomainGroup(DomainGroupConfig dg) {
    for (DomainGroupChangeListener listener : domainGroupListeners.get(dg.getName())) {
      listener.onDomainGroupChange(dg);
    }
  }
  
  /**
   * Pushes the ring group to all <code>RingGroupChangeListener</code>s listening on this ring group.
   * 
   * @param rg
   */
  private void pushNewRingGroup(RingGroupConfig rg) {
    //Notify people listening on this ring group
    for (RingGroupChangeListener listener : ringGroupListeners.get(rg.getName())) {
      listener.onRingGroupChange(rg);
    }
  }

//  /**
//   * Checks the state of all the hosts in the specified ring and generates a new
//   * <code>RingState</code> for the ring, and pushes out the new ring group to
//   * all listeners.
//   * 
//   * @param ringGroupName
//   * @param ringNumber
//   * @throws DataNotFoundException
//   *           if the specified ring group or ring could not be found
//   * @throws InterruptedException
//   */
//  // TODO: This is not as efficient as it could be, because it queries the
//  // DaemonState of every single daemon when only one DaemonState has changed.
//  private void refreshRingState(String ringGroupName, int ringNumber) throws DataNotFoundException, InterruptedException {
////    RingGroupConfigImpl rg;
////    if ((rg = ringGroupConfigs.get(ringGroupName)) == null) {
////      throw new DataNotFoundException("Ring group " + ringGroupName + " does not exist");
////    }
////    RingConfigImpl oldRing = (RingConfigImpl) rg.getRingConfig(ringNumber);
////    throw new NotImplementedException();
//////    RingConfigImpl newRing = new RingConfigImpl(ringGroupName, ringNumber, RingConfigImpl.loadRingStateFromZooKeeper(zk, this, ringGroupName, ringNumber), oldRing.getPartsMap());
//////    rg.getRingConfigsMap().put(ringNumber, newRing); // Clobber the old ring
////
//////    pushNewRingGroup(rg);
//  }
  
  /**
   * Goes through the local cached config information and adds the following
   * watchers and listeners: 1. For every host, {@link #ringStateUpdater}
   * listens to the DaemonState of both the part daemon and update daemon. 2.
   * For every domain, a {@link DomainVersionWatcher} listens to changes in the
   * version of the domain 3. For every domain group, a
   * {@link DomainGroupVersionWatcher} listens to changes in the version of the
   * domain group. 4. For every ring group, a {@link RingGroupWatcher} listens
   * to rings being added or removed to the ring group.
   */
  private void registerListenersAndWatchers() {
    // Register all the listeners for the ringStateUpdater
//    for (RingGroupConfig rg : ringGroupConfigs.values()) {
//      for (RingConfig ring : rg.getRingConfigs()) {
//        for (PartDaemonAddress address : ring.getHosts()) {
//          addDaemonStateChangeListener(rg.getName(), ring.getRingNumber(), address, DaemonType.PART_DAEMON, ringStateUpdater);
//          addDaemonStateChangeListener(rg.getName(), ring.getRingNumber(), address, DaemonType.UPDATE_DAEMON, ringStateUpdater);
//        }
//      }
//    }

    // Register DomainVersionWatchers
    for (DomainConfig domain : domainConfigsByName.values()) {
      myWatchers.add(new DomainVersionWatcher(domain.getName()));
    }
    // Register DomainGroupVersionWatchers
    for (DomainGroupConfig dg : domainGroupConfigs.values()) {
      myWatchers.add(new DomainGroupVersionWatcher(dg.getName()));
    }
    // Register all the RingGroupWatchers
    for (RingGroupConfig rg : ringGroupConfigs.values()) {
      myWatchers.add(new RingGroupWatcher(rg.getName()));
    }
  }

//  /**
//   * Used to listen to both the part daemon and the update daemon for every
//   * single host. When a daemon state changes, <code>ringStateUpdater</code>
//   * calls {@link #refreshRingState(String, int)} to update the RingState for
//   * the ring that the daemon is in. The new version of the RingGroupConfig then
//   * gets pushed out to all <code>RingGroupChangeListeners</code>
//   */
//  private DaemonStateChangeListener ringStateUpdater = new DaemonStateChangeListener() {
//    @Override
//    public void onDaemonStateChange(String ringGroupName, int ringNumber,
//        PartDaemonAddress hostAddress, DaemonType type, DaemonState state) {
//      try {
//        refreshRingState(ringGroupName, ringNumber);
//      } catch (DataNotFoundException e) {
//        //This shouldn't happen
//        LOG.warn(e);
//      } catch (InterruptedException e) {
//        // Server is probably going down
//        return;
//      }
//    }
//  };

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

  /**
   * Watches for a ring being added or removed from a ring group. If so, caches
   * the new config information and pushes the new config to all listeners.
   */
  private class RingGroupWatcher implements ZooKeeperWatcher, ChildrenCallback {
    private final String ringGroupName;
    private final String path;

    public RingGroupWatcher(String ringGroupName) {
      this.ringGroupName = ringGroupName;
      this.path = ringGroupsRoot + '/' + ringGroupName;
      register();
    }

    @Override
    public void process(WatchedEvent event) {
      if (event.getType() == EventType.NodeChildrenChanged) {
        processChange();
      }
    }

    private void processChange() {
      try {
        RingGroupConfigImpl rg = new RingGroupConfigImpl(zk, path, getRingGroupConfig(ringGroupName).getDomainGroupConfig());
        ringGroupConfigs.put(ringGroupName, rg);
        pushNewRingGroup(rg);
        register();
      } catch (Exception e) {
        // subtree probably under construction
        LOG.warn(e);
      }
    }

    @Override
    public void register() {
      // Set a watch on the children
      zk.getChildren(path, RingGroupWatcher.this, RingGroupWatcher.this, null); 
    }

    @Override
    public void processResult(int rc, String path, Object ctx, List<String> children) {
      processChange();
    }
  }

  private class DomainVersionWatcher implements ZooKeeperWatcher, DataCallback {
    private String path;
    private final String domainName;

    public DomainVersionWatcher(String domainName) {
      this.domainName = domainName;
      this.path = ZooKeeperUtils.domainPath(domainsRoot, domainName) + "/version";
      register();
    }

    @Override
    public void process(WatchedEvent event) {
      switch (event.getType()) {
        case NodeDataChanged:
          processDomainChange();
          break;

        default:
          LOG.debug("DomainVersionWatcher for " + path + " notified of event " + event + ". Ignoring.");
      }
    }

    private void processDomainChange() {
      try {
        ZkDomainConfig newDomain = new ZkDomainConfig(zk, ZooKeeperUtils.domainPath(domainsRoot, domainName));
        domainConfigsByName.put(path, newDomain);
        pushNewDomain(newDomain);
        register();
      } catch (DataNotFoundException e) {
        // This shouldn't happen
        LOG.error(e);
      }
    }

    @Override
    public void register() {
      zk.getData(path, DomainVersionWatcher.this, DomainVersionWatcher.this, null);
    }

    @Override
    public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
      processDomainChange();
    }
  }

  private class DomainGroupVersionWatcher implements ZooKeeperWatcher, ChildrenCallback {
    private String domainGroupName;
    private String path;

    public DomainGroupVersionWatcher(String domainGroupName) {
      this.domainGroupName = domainGroupName;
      this.path = ZooKeeperUtils.domainGroupPath(domainGroupsRoot, domainGroupName);
      register();
    }

    @Override
    public void process(WatchedEvent event) {
      if (event.getType() == EventType.NodeDataChanged) {
        processChange();
      }
    }

    private void processChange() {
      try {
        ZkDomainGroupConfig dg = new ZkDomainGroupConfig(zk, domainGroupsRoot + "/" + domainGroupName);
        domainGroupConfigs.put(domainGroupName, dg);
        pushNewDomainGroup(dg);
        register();
      } catch (InterruptedException e) {
        // Server probably going down.
        return;
      } catch (DataNotFoundException e) {
        // This shouldn't happen
        LOG.warn(e);
      } catch (KeeperException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }

    @Override
    public void register() {
      zk.getChildren(path, DomainGroupVersionWatcher.this, DomainGroupVersionWatcher.this, null);
    }

    @Override
    public void processResult(int rc, String path, Object ctx, List<String> children) {
      processChange();
    }
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
}
