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

package com.rapleaf.hank.coordinator;

import com.rapleaf.hank.storage.RemoteDomainCleaner;
import com.rapleaf.hank.storage.RemoteDomainVersionDeleter;
import com.rapleaf.hank.storage.StorageEngine;
import com.rapleaf.hank.util.ReverseComparator;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;

public final class Domains {

  private static final Logger LOG = Logger.getLogger(Domains.class);

  private Domains() {
  }

  /**
   * Get the sum of the num bytes used for all versions.
   *
   * @return
   * @throws IOException
   */
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
      if (DomainVersions.isClosed(version) && !version.isDefunct()) {
        return version;
      }
    }
    return null;
  }

  public static void cleanDomains(Collection<Domain> domains) throws IOException {
    LOG.info("Cleaning all Domains");
    for (Domain domain : domains) {
      StorageEngine storageEngine = domain.getStorageEngine();
      RemoteDomainCleaner cleaner = storageEngine.getRemoteDomainCleaner();
      if (cleaner == null) {
        LOG.info("Failed to clean Domain " + domain.getName() + ". No Remote Domain Cleaner is configured.");
        continue;
      }
      RemoteDomainVersionDeleter deleter = storageEngine.getRemoteDomainVersionDeleter();
      if (deleter == null) {
        LOG.info("Failed to clean Domain " + domain.getName() + ". No Remote Domain Version Deleter is configured.");
        continue;
      }
      LOG.info("Cleaning Domain " + domain.getName());
      cleaner.deleteOldVersions(deleter);
    }
    LOG.info("Done cleaning all Domains");
  }

  public static String joinPartitionServerFlags(List<String> flags) {
    List<String> results = new ArrayList<String>();
    for (String flag : flags) {
      results.add(StringUtils.trim(flag));
    }
    return StringUtils.join(results.toArray(), ",");
  }

  public static List<String> splitPartitionServerFlags(String flags) {
    String[] flagArray = StringUtils.split(flags, ",");
    List<String> results = new ArrayList<String>(flagArray.length);
    for (String flag : flagArray) {
      results.add(StringUtils.trim(flag));
    }
    return results;
  }
}
