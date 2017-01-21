/**
 * Copyright 2011 LiveRamp
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.liveramp.hank.storage.cueball;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;

import com.liveramp.hank.compression.cueball.CueballCompressionCodec;
import com.liveramp.hank.compression.cueball.NoCueballCompressionCodec;
import com.liveramp.hank.config.DataDirectoriesConfigurator;
import com.liveramp.hank.config.ReaderConfigurator;
import com.liveramp.hank.coordinator.Domain;
import com.liveramp.hank.coordinator.DomainVersion;
import com.liveramp.hank.hasher.Hasher;
import com.liveramp.hank.hasher.IdentityHasher;
import com.liveramp.hank.partition_server.DiskPartitionAssignment;
import com.liveramp.hank.storage.Compactor;
import com.liveramp.hank.storage.Deleter;
import com.liveramp.hank.storage.FileOpsUtil;
import com.liveramp.hank.storage.PartitionRemoteFileOps;
import com.liveramp.hank.storage.PartitionRemoteFileOpsFactory;
import com.liveramp.hank.storage.PartitionUpdater;
import com.liveramp.hank.storage.Reader;
import com.liveramp.hank.storage.RemoteDomainCleaner;
import com.liveramp.hank.storage.RemoteDomainVersionDeleter;
import com.liveramp.hank.storage.StorageEngine;
import com.liveramp.hank.storage.StorageEngineFactory;
import com.liveramp.hank.storage.Writer;
import com.liveramp.hank.storage.incremental.IncrementalDomainVersionProperties;
import com.liveramp.hank.storage.incremental.IncrementalStorageEngine;
import com.liveramp.hank.storage.incremental.IncrementalUpdatePlanner;
import com.liveramp.hank.util.FsUtils;

/**
 * Cueball is a storage engine optimized for small, fixed-size values.
 */
public class Cueball extends IncrementalStorageEngine implements StorageEngine {

  private static final Pattern BASE_OR_DELTA_PATTERN = Pattern.compile(".*(\\d{5})\\.((base)|(delta))\\.cueball");
  static final String BASE_REGEX = ".*\\d{5}\\.base\\.cueball";
  static final String DELTA_REGEX = ".*\\d{5}\\.delta\\.cueball";

  public static class Factory implements StorageEngineFactory {

    public static final String REMOTE_DOMAIN_ROOT_KEY = "remote_domain_root";
    public static final String HASH_INDEX_BITS_KEY = "hash_index_bits";
    public static final String VALUE_SIZE_KEY = "value_size";
    public static final String KEY_HASH_SIZE_KEY = "key_hash_size";
    public static final String FILE_OPS_FACTORY_KEY = "file_ops_factory";
    public static final String HASHER_KEY = "hasher";
    public static final String COMPRESSION_CODEC = "compression_codec";
    public static final String NUM_REMOTE_LEAF_VERSIONS_TO_KEEP = "num_remote_leaf_versions_to_keep";

    private static final Set<String> REQUIRED_KEYS =
        new HashSet<String>(Arrays.asList(
            HASH_INDEX_BITS_KEY,
            HASHER_KEY,
            VALUE_SIZE_KEY,
            KEY_HASH_SIZE_KEY,
            FILE_OPS_FACTORY_KEY,
            NUM_REMOTE_LEAF_VERSIONS_TO_KEEP));

    @Override
    public StorageEngine getStorageEngine(Map<String, Object> options, Domain domain) throws IOException {
      for (String requiredKey : REQUIRED_KEYS) {
        if (options == null || options.get(requiredKey) == null) {
          throw new IOException("Required key '" + requiredKey
              + "' was not found!");
        }
      }

      // Hasher
      Hasher hasher;
      PartitionRemoteFileOpsFactory fileOpsFactory;
      try {
        hasher = (Hasher)Class.forName((String)options.get(HASHER_KEY)).newInstance();
        fileOpsFactory = (PartitionRemoteFileOpsFactory)Class.forName((String)options.get(FILE_OPS_FACTORY_KEY)).newInstance();
      } catch (Exception e) {
        throw new IOException(e);
      }

      // Compression codec
      String compressionCodec = (String)options.get(COMPRESSION_CODEC);
      Class<? extends CueballCompressionCodec> compressionCodecClass = NoCueballCompressionCodec.class;
      if (compressionCodec != null) {
        try {
          compressionCodecClass = (Class<? extends CueballCompressionCodec>)Class.forName(compressionCodec);
        } catch (ClassNotFoundException e) {
          throw new IOException("Failed to get CompressionCodec class '"
              + compressionCodec + "'!", e);
        }
      }

      // Num remote bases to keep
      Integer numRemoteLeafVersionsToKeep = (Integer)options.get(NUM_REMOTE_LEAF_VERSIONS_TO_KEEP);

      return new Cueball((Integer)options.get(KEY_HASH_SIZE_KEY),
          hasher,
          (Integer)options.get(VALUE_SIZE_KEY),
          (Integer)options.get(HASH_INDEX_BITS_KEY),
          FileOpsUtil.getDomainBuilderRoot(options),
          FileOpsUtil.getPartitionServerRoot(options),
          fileOpsFactory,
          compressionCodecClass,
          domain,
          numRemoteLeafVersionsToKeep);
    }

    @Override
    public String getPrettyName() {
      return "Cueball";
    }

    @Override
    public String getDefaultOptions() {
      return "";
    }
  }

  private final Domain domain;

  private final int keyHashSize;
  private final Hasher hasher;
  private final int valueSize;
  private final int hashIndexBits;
  private final String domainBuilderRemoteDomainRoot;
  private final String partitionServerRemoteDomainRoot;
  private final PartitionRemoteFileOpsFactory partitionRemoteFileOpsFactory;
  private final ByteBuffer keyHashBuffer;
  private final int numRemoteLeafVersionsToKeep;

  private final Class<? extends CueballCompressionCodec> compressionCodecClass;

  public Cueball(int keyHashSize,
                 Hasher hasher,
                 int valueSize,
                 int hashIndexBits,
                 String domainBuilderRemoteDomainRoot,
                 String partitionServerRemoteDomainRoot,
                 PartitionRemoteFileOpsFactory partitionRemoteFileOpsFactory,
                 Class<? extends CueballCompressionCodec> compressionCodecClass,
                 Domain domain,
                 int numRemoteLeafVersionsToKeep) {
    this.keyHashSize = keyHashSize;
    this.hasher = hasher;
    this.valueSize = valueSize;
    this.hashIndexBits = hashIndexBits;
    this.domainBuilderRemoteDomainRoot = domainBuilderRemoteDomainRoot;
    this.partitionServerRemoteDomainRoot = partitionServerRemoteDomainRoot;
    this.partitionRemoteFileOpsFactory = partitionRemoteFileOpsFactory;
    this.keyHashBuffer = ByteBuffer.allocate(keyHashSize);
    this.compressionCodecClass = compressionCodecClass;
    this.domain = domain;
    this.numRemoteLeafVersionsToKeep = numRemoteLeafVersionsToKeep;
    // Sanity check
    if (hashIndexBits > 32) {
      throw new RuntimeException("hashIndexBits is much too large (" + hashIndexBits + ")");
    }
  }

  @Override
  public Reader getReader(ReaderConfigurator configurator, int partitionNumber, DiskPartitionAssignment assignment) throws IOException {
    return new CueballReader(
        getTargetDirectory(assignment, partitionNumber),
        keyHashSize,
        hasher,
        valueSize,
        hashIndexBits,
        getCompressionCodec(),
        configurator.getCacheNumBytesCapacity(),
        (int)configurator.getCacheNumItemsCapacity());
  }

  private CueballCompressionCodec getCompressionCodec() throws IOException {
    try {
      return compressionCodecClass.newInstance();
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public Writer getWriter(DomainVersion domainVersion,
                          PartitionRemoteFileOps partitionRemoteFileOps,
                          int partitionNumber) throws IOException {
    IncrementalDomainVersionProperties domainVersionProperties = getDomainVersionProperties(domainVersion);
    return new CueballWriter(partitionRemoteFileOps.getOutputStream(getName(domainVersion.getVersionNumber(),
        domainVersionProperties.isBase())),
        keyHashSize, hasher, valueSize, getCompressionCodec(), hashIndexBits
    );
  }

  private IncrementalDomainVersionProperties getDomainVersionProperties(DomainVersion domainVersion) throws IOException {
    IncrementalDomainVersionProperties result;
    try {
      result = (IncrementalDomainVersionProperties)domainVersion.getProperties();
    } catch (ClassCastException e) {
      throw new IOException("Failed to load properties of version " + domainVersion);
    }
    if (result == null) {
      throw new IOException("Null properties for version " + domainVersion);
    }
    return result;
  }

  @Override
  public IncrementalUpdatePlanner getUpdatePlanner(Domain domain) {
    return new CueballUpdatePlanner(domain);
  }

  @Override
  public PartitionUpdater getUpdater(DiskPartitionAssignment assignment, int partitionNumber) throws IOException {
    String localDir = getTargetDirectory(assignment, partitionNumber);
    return new CueballPartitionUpdater(domain,
        getPartitionRemoteFileOps(RemoteLocation.PARTITION_SERVER, partitionNumber),
        new CueballMerger(),
        keyHashSize,
        valueSize,
        hashIndexBits,
        getCompressionCodec(),
        localDir);
  }

  @Override
  public Compactor getCompactor(DiskPartitionAssignment configurator,
                                int partitionNumber) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Writer getCompactorWriter(DomainVersion domainVersion,
                                   PartitionRemoteFileOps partitionRemoteFileOps,
                                   int partitionNumber) throws IOException {
    IncrementalDomainVersionProperties domainVersionProperties = getDomainVersionProperties(domainVersion);
    // Note: We use the identity hasher since keys coming in are already hashed keys
    return new CueballWriter(partitionRemoteFileOps.getOutputStream(getName(domainVersion.getVersionNumber(),
        domainVersionProperties.isBase())),
        keyHashSize,
        new IdentityHasher(),
        valueSize,
        getCompressionCodec(),
        hashIndexBits
    );
  }

  @Override
  public ByteBuffer getComparableKey(ByteBuffer key) {
    hasher.hash(key, keyHashSize, keyHashBuffer.array());
    return keyHashBuffer;
  }

  @Override
  public PartitionRemoteFileOpsFactory getPartitionRemoteFileOpsFactory(RemoteLocation location) {
    return partitionRemoteFileOpsFactory;
  }

  @Override
  public PartitionRemoteFileOps getPartitionRemoteFileOps(RemoteLocation location, int partitionNumber) throws IOException {
    return partitionRemoteFileOpsFactory.getPartitionRemoteFileOps(getRoot(location), partitionNumber);
  }

  @Override
  public Deleter getDeleter(DiskPartitionAssignment assignment, int partitionNumber) throws IOException {
    String localDir = getTargetDirectory(assignment, partitionNumber);
    return new CueballDeleter(localDir);
  }

  public static String padVersionNumber(int versionNumber) {
    return String.format("%05d", versionNumber);
  }

  public static SortedSet<CueballFilePath> getBases(String... dirs) throws IOException {
    SortedSet<CueballFilePath> result = new TreeSet<CueballFilePath>();
    Set<String> paths = FsUtils.getMatchingPaths(BASE_REGEX, dirs);
    for (String path : paths) {
      result.add(new CueballFilePath(path));
    }
    return result;
  }

  public static SortedSet<CueballFilePath> getDeltas(String... dirs) throws IOException {
    SortedSet<CueballFilePath> result = new TreeSet<CueballFilePath>();
    Set<String> paths = FsUtils.getMatchingPaths(DELTA_REGEX, dirs);
    for (String path : paths) {
      result.add(new CueballFilePath(path));
    }
    return result;
  }

  public static int parseVersionNumber(String name) {
    Matcher matcher = BASE_OR_DELTA_PATTERN.matcher(name);
    if (!matcher.matches()) {
      throw new IllegalArgumentException("string " + name
          + " isn't a path that parseVersionNumber can parse!");
    }

    return Integer.parseInt(matcher.group(1));
  }

  public static String getName(int versionNumber, boolean base) {
    String s = padVersionNumber(versionNumber) + ".";
    if (base) {
      s += "base";
    } else {
      s += "delta";
    }
    return s + ".cueball";
  }

  public static String getName(DomainVersion domainVersion) throws IOException {
    return getName(domainVersion.getVersionNumber(), IncrementalDomainVersionProperties.isBase(domainVersion));
  }

  @Override
  public RemoteDomainVersionDeleter getRemoteDomainVersionDeleter(RemoteLocation location) throws IOException {
    return new CueballRemoteDomainVersionDeleter(domain, getRoot(location), partitionRemoteFileOpsFactory);
  }

  @Override
  public RemoteDomainCleaner getRemoteDomainCleaner() throws IOException {
    return new CueballRemoteDomainCleaner(domain, numRemoteLeafVersionsToKeep);
  }

  @Override
  public DiskPartitionAssignment getDataDirectoryPerPartition(DataDirectoriesConfigurator configurator, Collection<Integer> partitionNumbers) {
    return getDataDirectoryAssignments(configurator, partitionNumbers);
  }

  public static DiskPartitionAssignment getDataDirectoryAssignments(DataDirectoriesConfigurator configurator, Collection<Integer> partitionNumbers) {

    ArrayList<String> sortedDataDirectories = new ArrayList<String>(configurator.getDataDirectories());
    Collections.sort(sortedDataDirectories);

    LinkedList<Integer> sortedPartitions = new LinkedList<>(partitionNumbers);
    Collections.sort(sortedPartitions);

    //  TODO we can make this dynamic based on disk size, but not urgent
    double numPartitionsPerDisk = (double)partitionNumbers.size() / sortedDataDirectories.size();

    Multimap<String, Integer> partitionsPerDisk = HashMultimap.create();
    for (String dataDirectory : sortedDataDirectories) {

      int numToAssign = (int)Math.ceil(numPartitionsPerDisk * (partitionsPerDisk.keySet().size() + 1))
          - partitionsPerDisk.values().size();

      for (int i = 0; i < numToAssign && !sortedPartitions.isEmpty(); i++) {
        partitionsPerDisk.put(dataDirectory, sortedPartitions.pop());
      }

    }

    Map<Integer, String> inverse = Maps.newHashMap();
    for (Map.Entry<String, Integer> entry : partitionsPerDisk.entries()) {
      inverse.put(entry.getValue(), entry.getKey());
    }

    return new DiskPartitionAssignment(inverse);
  }

  private String getTargetDirectory(DiskPartitionAssignment assignment,
                                    int partitionNumber) {
    return assignment.getDisk(partitionNumber) + "/" + domain.getName() + "/" + partitionNumber;
  }

  @Override
  public Set<String> getFiles(DiskPartitionAssignment assignment,
                              int domainVersionNumber,
                              int partitionNumber) throws IOException {
    Set<String> result = new HashSet<String>();
    result.add(getTargetDirectory(assignment, partitionNumber) + "/" + getName(domainVersionNumber, true));
    return result;
  }

  private final String getRoot(RemoteLocation location) {
    if (location == RemoteLocation.DOMAIN_BUILDER) {
      return domainBuilderRemoteDomainRoot;
    } else if (location == RemoteLocation.PARTITION_SERVER) {
      return partitionServerRemoteDomainRoot;
    } else {
      throw new RuntimeException();
    }
  }

  @Override
  public String toString() {
    return "Cueball{" +
        ", keyHashSize=" + keyHashSize +
        ", hasher=" + hasher +
        ", valueSize=" + valueSize +
        ", hashIndexBits=" + hashIndexBits +
        ", domainBuilderRemoteDomainRoot='" + domainBuilderRemoteDomainRoot + '\'' +
        ", partitionServerRemoteDomainRoot='" + partitionServerRemoteDomainRoot + '\'' +
        ", partitionRemoteFileOpsFactory=" + partitionRemoteFileOpsFactory +
        ", keyHashBuffer=" + keyHashBuffer +
        ", numRemoteLeafVersionsToKeep=" + numRemoteLeafVersionsToKeep +
        ", compressionCodecClass=" + compressionCodecClass +
        '}';
  }
}
