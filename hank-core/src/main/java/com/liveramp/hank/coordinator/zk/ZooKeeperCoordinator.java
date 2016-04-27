/**
 *  Copyright 2011 LiveRamp
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
package com.liveramp.hank.coordinator.zk;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.slf4j.Logger; import org.slf4j.LoggerFactory;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

import com.liveramp.hank.coordinator.Coordinator;
import com.liveramp.hank.coordinator.CoordinatorFactory;
import com.liveramp.hank.coordinator.Domain;
import com.liveramp.hank.coordinator.DomainGroup;
import com.liveramp.hank.coordinator.RingGroup;
import com.liveramp.hank.zookeeper.WatchedMap;
import com.liveramp.hank.zookeeper.ZkPath;
import com.liveramp.hank.zookeeper.ZooKeeperConnection;
import com.liveramp.hank.zookeeper.ZooKeeperPlus;

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

  private static final String KEY_DOMAIN_ID_COUNTER = ".domain_id_counter";
  private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperCoordinator.class);

  /**
   * Used to instantiate a ZooKeeperCoordinator generically.
   */
  public static final class Factory implements CoordinatorFactory {
    private static final String RING_GROUPS_ROOT_KEY = "ring_groups_root";
    private static final String DOMAIN_GROUPS_ROOT_KEY = "domain_groups_root";
    private static final String DOMAINS_ROOT_KEY = "domains_root";
    private static final String SESSION_TIMEOUT_KEY = "session_timeout";
    private static final String CONNECT_STRING_KEY = "connect_string";
    private static final String MAX_CONNECTION_ATTEMPTS_KEY = "max_connection_attempts";
    private static final List<String> REQUIRED_KEYS = Arrays.asList(RING_GROUPS_ROOT_KEY, DOMAIN_GROUPS_ROOT_KEY, DOMAINS_ROOT_KEY, SESSION_TIMEOUT_KEY, CONNECT_STRING_KEY);

    public static Map<String, Object> requiredOptions(String zkConnectString, int sessionTimeoutMs, String domainsRoot, String domainGroupsRoot, String ringGroupsRoot, Integer maxConnectionAttempts) {
      Map<String, Object> opts = new HashMap<String, Object>();
      opts.put(CONNECT_STRING_KEY, zkConnectString);
      opts.put(SESSION_TIMEOUT_KEY, sessionTimeoutMs);
      opts.put(DOMAINS_ROOT_KEY, domainsRoot);
      opts.put(DOMAIN_GROUPS_ROOT_KEY, domainGroupsRoot);
      opts.put(RING_GROUPS_ROOT_KEY, ringGroupsRoot);
      opts.put(MAX_CONNECTION_ATTEMPTS_KEY, maxConnectionAttempts);
      return opts;
    }

    @Override
    public Coordinator getCoordinator(Map<String, Object> options) {
      LOG.info("Creating Coordinator with options: "+options);

      validateOptions(options);
      try {
        //  TODO temporary until clean upusages
        if(options.containsKey(MAX_CONNECTION_ATTEMPTS_KEY)) {
          return new ZooKeeperCoordinator(
              (String)options.get(CONNECT_STRING_KEY),
              (Integer)options.get(SESSION_TIMEOUT_KEY),
              (String)options.get(DOMAINS_ROOT_KEY),
              (String)options.get(DOMAIN_GROUPS_ROOT_KEY),
              (String)options.get(RING_GROUPS_ROOT_KEY),
              (Integer)options.get(MAX_CONNECTION_ATTEMPTS_KEY));
        }else{
          return new ZooKeeperCoordinator(
              (String)options.get(CONNECT_STRING_KEY),
              (Integer)options.get(SESSION_TIMEOUT_KEY),
              (String)options.get(DOMAINS_ROOT_KEY),
              (String)options.get(DOMAIN_GROUPS_ROOT_KEY),
              (String)options.get(RING_GROUPS_ROOT_KEY));
        }
      } catch (Exception e) {
        throw new RuntimeException("Couldn't make a ZooKeeperCoordinator from options "
            + options, e);
      }
    }

    private void validateOptions(Map<String, Object> options) {
      Set<String> missingKeys = new HashSet<String>();
      for (String requiredKey : REQUIRED_KEYS) {
        if (!options.containsKey(requiredKey)) {
          missingKeys.add(requiredKey);
        }
      }
      if (!missingKeys.isEmpty()) {
        throw new RuntimeException("Options for ZooKeeperCoordinator was missing required keys: "
            + missingKeys);
      }
    }
  }

  /**
   * We save our watchers so that we can reregister them in case of session
   * expiry.
   */
  private boolean isSessionExpired = false;

  private final WatchedMap<ZkDomain> domains;
  private final WatchedMap<ZkDomainGroup> domainGroups;
  private final WatchedMap<ZkRingGroup> ringGroups;

  private final String domainsRoot;
  private final String domainGroupsRoot;
  private final String ringGroupsRoot;

  ZooKeeperCoordinator(String zkConnectString,
                       int sessionTimeoutMs,
                       String domainsRoot,
                       String domainGroupsRoot,
                       String ringGroupsRoot) throws InterruptedException, IOException, KeeperException {
    this(zkConnectString, sessionTimeoutMs, domainsRoot, domainGroupsRoot, ringGroupsRoot, ZooKeeperConnection.DEFAULT_MAX_ATTEMPTS);
  }

  /**
   * Blocks until the connection to the ZooKeeper service has been established.
   * See {@link ZooKeeperConnection#ZooKeeperConnection(String, int)}
   * <p/>
   * Package-private constructor that is mainly used for testing. The last
   * boolean flag allows you to prevent the ZooKeeperCoordinator from
   * immediately trying to cache all the configuration information from the
   * ZooKeeper service, which is useful if you don't want to have to setup your
   * entire configuration just to run a few simple tests.
   *
   * @param zkConnectString  comma separated host:port pairs, each corresponding to a ZooKeeper
   *                         server. e.g. "127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002"
   * @param sessionTimeoutMs session timeout in milliseconds
   * @param domainsRoot
   * @param domainGroupsRoot
   * @param ringGroupsRoot
   * @throws InterruptedException
   * @throws KeeperException
   * @throws IOException
   */
  ZooKeeperCoordinator(String zkConnectString,
                       int sessionTimeoutMs,
                       String domainsRoot,
                       String domainGroupsRoot,
                       String ringGroupsRoot,
                       int maxConnectAttempts)
      throws InterruptedException, KeeperException, IOException {
    super(zkConnectString, sessionTimeoutMs, maxConnectAttempts);
    this.domainsRoot = domainsRoot;
    this.domainGroupsRoot = domainGroupsRoot;
    this.ringGroupsRoot = ringGroupsRoot;

    LOG.info("ZooKeeperCoordinator.domainsRoot: ",domainsRoot);
    LOG.info("ZooKeeperCoordinator.domainGroupsRoot: ",domainGroupsRoot);
    LOG.info("ZooKeeperCoordinator.ringGroupsRoot: ",ringGroupsRoot);

    // Domains
    zk.ensureCreated(domainsRoot, null);
    domains = new WatchedMap<ZkDomain>(zk, domainsRoot, new WatchedMap.ElementLoader<ZkDomain>() {
      @Override
      public ZkDomain load(ZooKeeperPlus zk, String basePath, String relPath) throws KeeperException, InterruptedException {
        if (ZkPath.isHidden(relPath)) {
          return null;
        } else {
          return new ZkDomain(zk, ZkPath.append(basePath, relPath));
        }
      }
    });

    // Domain Groups
    zk.ensureCreated(domainGroupsRoot, null);
    domainGroups = new WatchedMap<ZkDomainGroup>(zk, domainGroupsRoot, new WatchedMap.ElementLoader<ZkDomainGroup>() {
      @Override
      public ZkDomainGroup load(ZooKeeperPlus zk, String basePath, String relPath) throws KeeperException, InterruptedException {
        try {
          return new ZkDomainGroup(zk, ZooKeeperCoordinator.this, ZkPath.append(basePath, relPath));
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    });

    // Ring Groups
    zk.ensureCreated(ringGroupsRoot, null);
    ringGroups = new WatchedMap<ZkRingGroup>(zk, ringGroupsRoot, new WatchedMap.ElementLoader<ZkRingGroup>() {
      @Override
      public ZkRingGroup load(ZooKeeperPlus zk, String basePath, String relPath) throws KeeperException, InterruptedException {
        if (ZkPath.isHidden(relPath)) {
          return null;
        } else {
          String ringGroupPath = ZkPath.append(basePath, relPath);
          return new ZkRingGroup(zk, ringGroupPath,
              domainGroups.get(new String(zk.getData(ringGroupPath, false, null))), ZooKeeperCoordinator.this);
        }
      }
    }, new DotComplete());
  }

  @Override
  protected void onConnect() {
    if (isSessionExpired) {
      isSessionExpired = false;
    }
  }

  @Override
  protected void onSessionExpire() {
    isSessionExpired = true;
  }

  @Override
  public Domain getDomain(String domainName) {
    return domains.get(domainName);
  }

  @Override
  public Domain getDomainShallow(String domainName) {
    if (domains.isLoaded()) {
      return getDomain(domainName);
    } else {
      try {
        return new ZkDomain(zk, ZkPath.append(domainsRoot, domainName));
      } catch (InterruptedException e) {
        return null;
      } catch (KeeperException e) {
        return null;
      }
    }
  }

  @Override
  public Domain getDomainById(int domainId) {
    for (Domain domain : getDomains()) {
      if (domain.getId() == domainId) {
        return domain;
      }
    }
    return null;
  }

  @Override
  public DomainGroup getDomainGroup(String domainGroupName) {
    return domainGroups.get(domainGroupName);
  }

  @Override
  public RingGroup getRingGroup(String ringGroupName) {
    return ringGroups.get(ringGroupName);
  }

  @Override
  public Set<Domain> getDomains() {
    return new HashSet<Domain>(domains.values());
  }

  @Override
  public SortedSet<Domain> getDomainsSorted() {
    return new TreeSet<Domain>(getDomains());
  }

  @Override
  public Set<DomainGroup> getDomainGroups() {
    synchronized (domainGroups) {
      return new HashSet<DomainGroup>(domainGroups.values());
    }
  }

  @Override
  public SortedSet<DomainGroup> getDomainGroupsSorted() {
    return new TreeSet<DomainGroup>(getDomainGroups());
  }

  @Override
  public Set<RingGroup> getRingGroups() {
    return new HashSet<RingGroup>(ringGroups.values());
  }

  @Override
  public SortedSet<RingGroup> getRingGroupsSorted() {
    return new TreeSet<RingGroup>(ringGroups.values());
  }

  @Override
  public Set<RingGroup> getRingGroupsForDomainGroup(DomainGroup domainGroup) {
    String domainGroupName = domainGroup.getName();
    Set<RingGroup> groups = new HashSet<RingGroup>();
    for (RingGroup group : ringGroups.values()) {
      if (group.getDomainGroup().getName().equals(domainGroupName)) {
        groups.add(group);
      }
    }
    return groups;
  }

  @Override
  public Domain addDomain(String domainName,
                          int numParts,
                          String storageEngineFactoryName,
                          String storageEngineOptions,
                          String partitionerName,
                          List<String> requiredHostFlags) throws IOException {
    try {
      ZkDomain domain = ZkDomain.create(zk, domainsRoot, domainName, numParts, storageEngineFactoryName,
          storageEngineOptions, partitionerName, getNextDomainId(), requiredHostFlags);
      domains.put(domainName, domain);
      return domain;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  private int getNextDomainId() throws KeeperException, InterruptedException {
    final String domainIdCounterPath = ZkPath.append(domainsRoot, KEY_DOMAIN_ID_COUNTER);
    if (zk.exists(domainIdCounterPath, false) == null) {
      zk.create(domainIdCounterPath, Integer.toString(1).getBytes());
      return 1;
    }
    while (true) {
      final Stat stat = new Stat();
      final byte[] data = zk.getData(domainIdCounterPath, false, stat);
      int lastVersionNumber = Integer.parseInt(new String(data));
      try {
        lastVersionNumber++;
        zk.setData(domainIdCounterPath, Integer.toString(lastVersionNumber).getBytes(), stat.getVersion());
        return lastVersionNumber;
      } catch (KeeperException.BadVersionException e) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Tried to set the domain id counter to " + lastVersionNumber + " but was preempted by another writer. Retrying.");
        }
      }
    }
  }

  @Override
  public Domain updateDomain(String domainName,
                             int numParts,
                             String storageEngineFactoryClassName,
                             String storageEngineOptions,
                             String partitionerClassName,
                             List<String> requiredHostFlags) throws IOException {
    ZkDomain domain = (ZkDomain)getDomain(domainName);
    if (domain == null) {
      throw new IOException("Could not get Domain '" + domainName + "' from Coordinator.");
    } else {
      try {
        domain.update(domain.getId(), numParts, storageEngineFactoryClassName, storageEngineOptions,
            partitionerClassName, requiredHostFlags);
        return domain;
      } catch (Exception e) {
        throw new IOException(e);
      }
    }
  }

  @Override
  public DomainGroup addDomainGroup(String name) throws IOException {
    try {
      ZkDomainGroup dgc = ZkDomainGroup.create(zk, this, domainGroupsRoot, name);
      synchronized (domainGroups) {
        domainGroups.put(name, dgc);
      }
      return dgc;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public RingGroup addRingGroup(String ringGroupName, String domainGroupName) throws IOException {
    try {
      RingGroup rg = ZkRingGroup.create(zk, ZkPath.append(ringGroupsRoot, ringGroupName),
          (ZkDomainGroup)getDomainGroup(domainGroupName), this);
      ringGroups.put(ringGroupName, (ZkRingGroup)rg);
      return rg;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public void close() throws IOException {
    try {
      LOG.info("Closing ZooKeeperCoordinator.");
      zk.close();
    } catch (InterruptedException e) {
      throw new IOException("Interrupted while trying to close ZooKeeper connection.", e);
    }
  }

  @Override
  public String toString() {
    return "ZooKeeperCoordinator [quorum=" + getConnectString()
        + ", domainsRoot=" + domainsRoot + ", domainGroupsRoot="
        + domainGroupsRoot + ", ringGroupsRoot=" + ringGroupsRoot + "]";
  }

  @Override
  public boolean deleteDomain(String domainName) throws IOException {
    ZkDomain domain = domains.remove(domainName);
    if (domain == null) {
      return false;
    }
    // remove domain from all domain groups
    for (DomainGroup domainGroup : getDomainGroups()) {
      domainGroup.removeDomain(domain);
    }
    return domain.delete();
  }

  @Override
  public boolean deleteDomainVersion(String domainName, int versionNumber) throws IOException {
    Domain domain = getDomain(domainName);
    if (domain == null) {
      return false;
    }
    return domain.deleteVersion(versionNumber);
  }

  @Override
  public boolean deleteDomainGroup(String domainGroupName) throws IOException {
    ZkDomainGroup domainGroup = domainGroups.remove(domainGroupName);
    if (domainGroup == null) {
      return false;
    }
    return domainGroup.delete();
  }

  @Override
  public boolean deleteRingGroup(String ringGroupName) throws IOException {
    ZkRingGroup ringGroup = ringGroups.remove(ringGroupName);
    if (ringGroup == null) {
      return false;
    }
    return ringGroup.delete();
  }
}
