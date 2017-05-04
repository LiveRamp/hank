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

package com.liveramp.hank.coordinator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import org.slf4j.Logger; import org.slf4j.LoggerFactory;

import com.liveramp.hank.storage.RemoteDomainCleaner;
import com.liveramp.hank.storage.RemoteDomainVersionDeleter;
import com.liveramp.hank.storage.StorageEngine;
import com.liveramp.hank.storage.incremental.IncrementalDomainVersionProperties;
import com.liveramp.hank.util.ReverseComparator;

public final class Domains {

  private static final Logger LOG = LoggerFactory.getLogger(Domains.class);

  private Domains() {
  }

  public static long getTotalNumBytes(Domain domain) throws IOException {
    long total = 0;
    for (DomainVersion version : domain.getVersions()) {
      if (version.isDefunct()) {
        continue;
      }
      total += DomainVersions.getTotalNumBytes(version);
    }
    return total;
  }

  public static DomainVersion getLatestDelta(Domain domain) throws IOException {
    SortedSet<DomainVersion> versions = domain.getVersions();
    if (versions == null || versions.size() == 0) {
      return null;
    } else {

      ArrayList<DomainVersion> list = Lists.newArrayList(versions);
      Collections.reverse(list);

      for (DomainVersion version : list) {
        if(!isBase(version)){
          return version;
        }
      }

      return null;
    }
  }

  public static DomainVersion getLatestVersion(Domain domain) throws IOException {
    SortedSet<DomainVersion> versions = domain.getVersions();
    if (versions == null || versions.size() == 0) {
      return null;
    } else {
      return versions.last();
    }
  }

  public static DomainVersion getLatestVersionNotOpenNotDefunct(Domain domain) throws IOException {
    Set<DomainVersion> originalVersions = domain.getVersions();
    if (originalVersions == null || originalVersions.size() == 0) {
      return null;
    }
    SortedSet<DomainVersion> versions = new TreeSet<DomainVersion>(new ReverseComparator<DomainVersion>());
    versions.addAll(originalVersions);
    for (DomainVersion version : versions) {
      if (isCompleteToBase(version, domain)) {
        return version;
      }
    }

    return null;
  }

  public static boolean hasOpenDelta(Domain domain) throws IOException {
    return getLatestVersion(domain) != null && !allDeltasComplete(getLatestDelta(domain), domain);
  }

  private static boolean allDeltasComplete(DomainVersion version, Domain domain) throws IOException {

    if(version == null){
      return true;
    }

    if (isBase(version)) {
      return true;
    }

    if (!DomainVersions.isClosed(version)) {
      return false;
    }

    IncrementalDomainVersionProperties properties = (IncrementalDomainVersionProperties)version.getProperties();

    return allDeltasComplete(
        domain.getVersion(properties.getParentVersionNumber()),
        domain
    );

  }

  public static boolean hasOpenDeltaAfterLastValidBase(Domain domain) throws IOException {
    final DomainVersion latestVersion = getLatestVersion(domain);
    if (isCompleteToBase(latestVersion, domain)) {
      return false;
    } else {
      return hasOpenDelta(domain);
    }
  }

  public static Optional<DomainVersion> getLatestOpenDeltaIfExists(Domain domain) throws IOException {
    final DomainVersion latestDelta = getLatestDelta(domain);
    return hasOpenDelta(domain) ? Optional.fromNullable(latestDelta) : Optional.<DomainVersion>absent();
  }

  private static boolean isCompleteToBase(DomainVersion version, Domain domain) throws IOException {

    if(version == null){
      return true;
    }

    if (!DomainVersions.isClosed(version)) {
      return false;
    }

    if (isBase(version)) {
      return !version.isDefunct();
    }

    //  TODO is there ever going to be a different impl of props?
    IncrementalDomainVersionProperties properties = (IncrementalDomainVersionProperties)version.getProperties();

    return isCompleteToBase(
        domain.getVersion(properties.getParentVersionNumber()),
        domain
    );

  }

  private static boolean isBase(DomainVersion version) throws IOException {
    IncrementalDomainVersionProperties properties = (IncrementalDomainVersionProperties)version.getProperties();
    return properties.isBase();
  }

  public static void cleanDomains(Collection<Domain> domains, StorageEngine.RemoteLocation dataLocation) throws IOException {
    cleanDomains(domains, true, dataLocation);
  }

  public static void cleanDomains(Collection <Domain> domains, boolean deleteMetadata, StorageEngine.RemoteLocation dataLocation) throws IOException {
    for (Domain domain : domains) {
      StorageEngine storageEngine = domain.getStorageEngine();
      RemoteDomainCleaner cleaner = storageEngine.getRemoteDomainCleaner();
      if (cleaner == null) {
        LOG.info("Failed to clean Domain " + domain.getName() + ". No Remote Domain Cleaner is configured.");
        continue;
      }
      RemoteDomainVersionDeleter deleter = storageEngine.getRemoteDomainVersionDeleter(dataLocation);
      if (deleter == null) {
        LOG.info("Failed to clean Domain " + domain.getName() + ". No Remote Domain Version Deleter is configured.");
        continue;
      }
      LOG.info("Cleaning Domain " + domain.getName());
      cleaner.deleteOldVersions(deleter, deleteMetadata);
    }
  }



}
