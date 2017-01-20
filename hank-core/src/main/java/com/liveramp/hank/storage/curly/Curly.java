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
package com.liveramp.hank.storage.curly;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.liveramp.hank.compression.CompressionCodec;
import com.liveramp.hank.compression.cueball.CueballCompressionCodec;
import com.liveramp.hank.compression.cueball.NoCueballCompressionCodec;
import com.liveramp.hank.config.BaseReaderConfigurator;
import com.liveramp.hank.config.DataDirectoriesConfigurator;
import com.liveramp.hank.config.ReaderConfigurator;
import com.liveramp.hank.coordinator.Domain;
import com.liveramp.hank.coordinator.DomainVersion;
import com.liveramp.hank.hasher.Hasher;
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
import com.liveramp.hank.storage.cueball.Cueball;
import com.liveramp.hank.storage.cueball.CueballMerger;
import com.liveramp.hank.storage.cueball.CueballStreamBufferMergeSort;
import com.liveramp.hank.storage.incremental.IncrementalDomainVersionProperties;
import com.liveramp.hank.storage.incremental.IncrementalStorageEngine;
import com.liveramp.hank.storage.incremental.IncrementalUpdatePlanner;
import com.liveramp.hank.util.FsUtils;

/**
 * Curly is a storage engine designed for larger, variable-sized values. It uses
 * Cueball under the hood.
 */
public class Curly extends IncrementalStorageEngine implements StorageEngine {

  private static final Pattern BASE_OR_REGEX_PATTERN = Pattern.compile(".*(\\d{5})\\.((base)|(delta))\\.curly");
  static final String BASE_REGEX = ".*\\d{5}\\.base\\.curly";
  static final String DELTA_REGEX = ".*\\d{5}\\.delta\\.curly";

  public static class Factory implements StorageEngineFactory {

    public static final String REMOTE_DOMAIN_ROOT_KEY = "remote_domain_root";
    public static final String RECORD_FILE_READ_BUFFER_BYTES_KEY = "record_file_read_buffer_bytes";
    public static final String HASH_INDEX_BITS_KEY = "hash_index_bits";
    public static final String MAX_ALLOWED_PART_SIZE_KEY = "max_allowed_part_size";
    public static final String KEY_HASH_SIZE_KEY = "key_hash_size";
    public static final String FILE_OPS_FACTORY_KEY = "file_ops_factory";
    public static final String HASHER_KEY = "hasher";
    private static final String COMPRESSION_CODEC = "compression_codec";
    public static final String NUM_REMOTE_LEAF_VERSIONS_TO_KEEP = "num_remote_leaf_versions_to_keep";
    public static final String VALUE_FOLDING_CACHE_CAPACITY = "value_folding_cache_capacity";
    private static final String BLOCK_COMPRESSION_CODEC = "block_compression_codec";
    private static final String COMPRESSED_BLOCK_SIZE_THRESHOLD = "compressed_block_size_threshold";
    private static final String OFFSET_IN_BLOCK_NUM_BYTES = "offset_in_block_num_bytes";

    private static final Set<String> REQUIRED_KEYS = new HashSet<String>(Arrays.asList(
        RECORD_FILE_READ_BUFFER_BYTES_KEY, HASH_INDEX_BITS_KEY, MAX_ALLOWED_PART_SIZE_KEY, KEY_HASH_SIZE_KEY,
        FILE_OPS_FACTORY_KEY, HASHER_KEY, NUM_REMOTE_LEAF_VERSIONS_TO_KEEP));

    @Override
    public StorageEngine getStorageEngine(Map<String, Object> options, Domain domain) throws IOException {
      for (String requiredKey : REQUIRED_KEYS) {
        if (options == null || options.get(requiredKey) == null) {
          throw new IOException("Required key '" + requiredKey
              + "' was not found!");
        }
      }

      Hasher hasher;
      PartitionRemoteFileOpsFactory fileOpsFactory;
      Class<? extends CueballCompressionCodec> compressionCodecClass;
      try {
        hasher = (Hasher)Class.forName((String)options.get(HASHER_KEY)).newInstance();
        fileOpsFactory = (PartitionRemoteFileOpsFactory)Class.forName((String)options.get(FILE_OPS_FACTORY_KEY)).newInstance();

        String compressionCodecClassName = (String)options.get(COMPRESSION_CODEC);
        if (compressionCodecClassName == null) {
          compressionCodecClass = NoCueballCompressionCodec.class;
        } else {
          compressionCodecClass = (Class<? extends CueballCompressionCodec>)Class.forName(compressionCodecClassName);
        }
      } catch (Exception e) {
        throw new IOException(e);
      }
      final long maxAllowedPartSize = options.get(MAX_ALLOWED_PART_SIZE_KEY) instanceof Long ? (Long)options.get(MAX_ALLOWED_PART_SIZE_KEY)
          : ((Integer)options.get(MAX_ALLOWED_PART_SIZE_KEY)).longValue();

      // num remote bases to keep
      Integer numRemoteLeafVersionsToKeep = (Integer)options.get(NUM_REMOTE_LEAF_VERSIONS_TO_KEEP);

      // Value folding cache size
      Integer valueFoldingCacheCapacity = (Integer)options.get(VALUE_FOLDING_CACHE_CAPACITY);
      if (valueFoldingCacheCapacity == null) {
        valueFoldingCacheCapacity = -1;
      }

      // Block compression
      CompressionCodec blockCompressionCodec = null;
      String blockCompressionCodecStr = (String)options.get(BLOCK_COMPRESSION_CODEC);
      if (blockCompressionCodecStr != null) {
        blockCompressionCodec = CompressionCodec.valueOf(blockCompressionCodecStr.toUpperCase());
      }
      Integer compressedBlockSizeThreshold = (Integer)options.get(COMPRESSED_BLOCK_SIZE_THRESHOLD);
      if (compressedBlockSizeThreshold == null) {
        compressedBlockSizeThreshold = -1;
      }
      Integer offsetInBlockNumBytes = (Integer)options.get(OFFSET_IN_BLOCK_NUM_BYTES);
      if (offsetInBlockNumBytes == null) {
        offsetInBlockNumBytes = -1;
      }

      return new Curly((Integer)options.get(KEY_HASH_SIZE_KEY),
          hasher,
          maxAllowedPartSize,
          (Integer)options.get(HASH_INDEX_BITS_KEY),
          (Integer)options.get(RECORD_FILE_READ_BUFFER_BYTES_KEY),
          FileOpsUtil.getDomainBuilderRoot(options),
          FileOpsUtil.getPartitionServerRoot(options),
          fileOpsFactory,
          compressionCodecClass,
          domain,
          numRemoteLeafVersionsToKeep,
          valueFoldingCacheCapacity,
          blockCompressionCodec,
          compressedBlockSizeThreshold,
          offsetInBlockNumBytes);
    }

    @Override
    public String getPrettyName() {
      return "Curly";
    }

    @Override
    public String getDefaultOptions() {
      return "";
    }
  }

  private final Domain domain;

  private final int offsetNumBytes;
  private final int recordFileReadBufferBytes;

  private final Cueball cueballStorageEngine;
  private final String domainBuilderRemoteDomainRoot;
  private final String partitionServerRemoteDomainRoot;

  private final int keyHashSize;
  private final PartitionRemoteFileOpsFactory partitionRemoteFileOpsFactory;
  private final int hashIndexBits;
  private final Class<? extends CueballCompressionCodec> keyFileCompressionCodecClass;
  private final int numRemoteLeafVersionsToKeep;
  private final int valueFoldingCacheCapacity;
  private final CompressionCodec blockCompressionCodec;
  private final int compressedBlockSizeThreshold;
  private final int offsetInBlockNumBytes;
  private final int cueballValueNumBytes;

  public Curly(int keyHashSize,
               Hasher hasher,
               long maxAllowedPartSize,
               int hashIndexBits,
               int recordFileReadBufferBytes,
               String domainBuilderRemoteDomainRoot,
               String partitionServerRemoteDomainRoot,
               PartitionRemoteFileOpsFactory partitionRemoteFileOpsFactory,
               Class<? extends CueballCompressionCodec> keyFileCompressionCodecClass,
               Domain domain,
               int numRemoteLeafVersionsToKeep,
               int valueFoldingCacheCapacity,
               CompressionCodec blockCompressionCodec,
               int compressedBlockSizeThreshold,
               int offsetInBlockNumBytes) {
    this.keyHashSize = keyHashSize;
    this.hashIndexBits = hashIndexBits;
    this.recordFileReadBufferBytes = recordFileReadBufferBytes;
    this.domainBuilderRemoteDomainRoot = domainBuilderRemoteDomainRoot;
    this.partitionServerRemoteDomainRoot = partitionServerRemoteDomainRoot;
    this.partitionRemoteFileOpsFactory = partitionRemoteFileOpsFactory;
    this.keyFileCompressionCodecClass = keyFileCompressionCodecClass;
    this.domain = domain;
    this.numRemoteLeafVersionsToKeep = numRemoteLeafVersionsToKeep;
    this.valueFoldingCacheCapacity = valueFoldingCacheCapacity;
    this.blockCompressionCodec = blockCompressionCodec;
    this.compressedBlockSizeThreshold = compressedBlockSizeThreshold;
    this.offsetInBlockNumBytes = offsetInBlockNumBytes;

    this.offsetNumBytes = (int)(Math.ceil(Math.ceil(Math.log(maxAllowedPartSize) / Math.log(2)) / 8.0));

    // Determine size of values in Cueball. If we are using block compression in Curly,
    // the offsets stored in Cueball are appended with the offset in the block.
    if (blockCompressionCodec == null) {
      this.cueballValueNumBytes = offsetNumBytes;
    } else {
      this.cueballValueNumBytes = offsetNumBytes + offsetInBlockNumBytes;
    }

    this.cueballStorageEngine = new Cueball(keyHashSize,
        hasher,
        cueballValueNumBytes,
        hashIndexBits,
        domainBuilderRemoteDomainRoot,
        partitionServerRemoteDomainRoot,
        partitionRemoteFileOpsFactory,
        keyFileCompressionCodecClass,
        domain,
        numRemoteLeafVersionsToKeep);
  }

  @Override
  public Reader getReader(ReaderConfigurator configurator, int partitionNumber, DiskPartitionAssignment assignment) throws IOException {

    // This configurator is used because this reader is composed of 2 underlying readers
    ReaderConfigurator subConfigurator = new BaseReaderConfigurator(
        configurator,
        configurator.getCacheNumBytesCapacity(),
        configurator.getCacheNumItemsCapacity(),
        configurator.getBufferReuseMaxSize(),
        2);

    return new CurlyReader(CurlyReader.getLatestBase(getTargetDirectory(assignment, partitionNumber)),
        recordFileReadBufferBytes,
        cueballStorageEngine.getReader(subConfigurator, partitionNumber, assignment),
        subConfigurator.getCacheNumBytesCapacity(),
        (int)subConfigurator.getCacheNumItemsCapacity(),
        blockCompressionCodec,
        offsetNumBytes,
        offsetInBlockNumBytes,
        false,
        subConfigurator.getBufferReuseMaxSize());
  }

  @Override
  public Writer getWriter(DomainVersion domainVersion,
                          PartitionRemoteFileOps partitionRemoteFileOps,
                          int partitionNumber) throws IOException {
    Writer cueballWriter = cueballStorageEngine.getWriter(domainVersion, partitionRemoteFileOps, partitionNumber);
    return getWriter(domainVersion, partitionRemoteFileOps, partitionNumber, cueballWriter);
  }

  // Helper
  private Writer getWriter(DomainVersion domainVersion,
                           PartitionRemoteFileOps partitionRemoteFileOps,
                           int partitionNumber,
                           Writer keyFileWriter) throws IOException {
    IncrementalDomainVersionProperties domainVersionProperties = getDomainVersionProperties(domainVersion);
    OutputStream outputStream = partitionRemoteFileOps.getOutputStream(getName(domainVersion.getVersionNumber(),
        domainVersionProperties.isBase()));
    return new CurlyWriter(outputStream, keyFileWriter, offsetNumBytes, valueFoldingCacheCapacity,
        blockCompressionCodec, compressedBlockSizeThreshold, offsetInBlockNumBytes);
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

  public static String padVersionNumber(int versionNumber) {
    return String.format("%05d", versionNumber);
  }

  @Override
  public IncrementalUpdatePlanner getUpdatePlanner(Domain domain) {
    return new CurlyUpdatePlanner(domain);
  }

  @Override
  public PartitionUpdater getUpdater(DiskPartitionAssignment assignment, int partitionNumber) throws IOException {
    File localDir = new File(getTargetDirectory(assignment, partitionNumber));
    if (!localDir.exists() && !localDir.mkdirs()) {
      throw new RuntimeException("Failed to create directory " + localDir.getAbsolutePath());
    }
    return getFastPartitionUpdater(localDir.getAbsolutePath(), partitionNumber);
  }

  @Override
  public Compactor getCompactor(DiskPartitionAssignment assignment,
                                int partitionNumber) throws IOException {
    if (assignment != null) {
      File localDir = new File(getTargetDirectory(assignment, partitionNumber));
      if (!localDir.exists() && !localDir.mkdirs()) {
        throw new RuntimeException("Failed to create directory " + localDir.getAbsolutePath());
      }
      return getCompactor(localDir.getAbsolutePath(), partitionNumber);
    } else {
      return getCompactor((String)null, partitionNumber);
    }
  }

  @Override
  public Writer getCompactorWriter(DomainVersion domainVersion,
                                   PartitionRemoteFileOps fileOps,
                                   int partitionNumber) throws IOException {
    Writer cueballWriter = cueballStorageEngine.getCompactorWriter(domainVersion, fileOps, partitionNumber);
    return getWriter(domainVersion, fileOps, partitionNumber, cueballWriter);
  }

  private Compactor getCompactor(String localDir,
                                 int partitionNumber) throws IOException {
    return new CurlyCompactor(domain,
        getPartitionRemoteFileOps(RemoteLocation.DOMAIN_BUILDER, partitionNumber),
        localDir,
        new CurlyCompactingMerger(recordFileReadBufferBytes),
        new CueballStreamBufferMergeSort.Factory(keyHashSize, cueballValueNumBytes, hashIndexBits, getCompressionCodec(), null),
        new ICurlyReaderFactory() {
          @Override
          public ICurlyReader getInstance(CurlyFilePath curlyFilePath) throws IOException {
            // Note: key file reader is null as it will *not* be used
            return new CurlyReader(curlyFilePath, recordFileReadBufferBytes,
                null, 10L << 20, 1 << 10, blockCompressionCodec, offsetNumBytes, offsetInBlockNumBytes, true, 10 << 10);
          }
        }
    );
  }

  private CurlyFastPartitionUpdater getFastPartitionUpdater(String localDir, int partNum) throws IOException {
    return new CurlyFastPartitionUpdater(domain,
        getPartitionRemoteFileOps(RemoteLocation.PARTITION_SERVER, partNum),
        new CurlyMerger(),
        new CueballMerger(),
        keyHashSize,
        offsetNumBytes,
        offsetInBlockNumBytes,
        hashIndexBits,
        getCompressionCodec(),
        localDir);
  }

  private CueballCompressionCodec getCompressionCodec() throws IOException {
    try {
      return keyFileCompressionCodecClass.newInstance();
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public Deleter getDeleter(DiskPartitionAssignment assignment, int partitionNumber)
      throws IOException {
    String localDir = getTargetDirectory(assignment, partitionNumber);
    return new CurlyDeleter(localDir);
  }

  @Override
  public ByteBuffer getComparableKey(ByteBuffer key) {
    return cueballStorageEngine.getComparableKey(key);
  }

  @Override
  public PartitionRemoteFileOpsFactory getPartitionRemoteFileOpsFactory(RemoteLocation location) {
    return partitionRemoteFileOpsFactory;
  }

  @Override
  public PartitionRemoteFileOps getPartitionRemoteFileOps(RemoteLocation location, int partitionNumber) throws IOException {
    return partitionRemoteFileOpsFactory.getPartitionRemoteFileOps(getRoot(location), partitionNumber);
  }

  private final String getRoot(RemoteLocation location){
    if(location == RemoteLocation.DOMAIN_BUILDER){
      return domainBuilderRemoteDomainRoot;
    }else if(location == RemoteLocation.PARTITION_SERVER) {
      return partitionServerRemoteDomainRoot;
    }else{
      throw new RuntimeException();
    }
  }

  public static int parseVersionNumber(String name) {
    Matcher matcher = BASE_OR_REGEX_PATTERN.matcher(name);
    if (!matcher.matches()) {
      throw new IllegalArgumentException("string " + name
          + " isn't a path that parseVersionNumber can parse!");
    }

    return Integer.parseInt(matcher.group(1));
  }

  public static SortedSet<CurlyFilePath> getBases(String... dirs) throws IOException {
    SortedSet<CurlyFilePath> result = new TreeSet<CurlyFilePath>();
    Set<String> paths = FsUtils.getMatchingPaths(BASE_REGEX, dirs);
    for (String path : paths) {
      result.add(new CurlyFilePath(path));
    }
    return result;
  }

  public static SortedSet<CurlyFilePath> getDeltas(String... dirs) throws IOException {
    SortedSet<CurlyFilePath> result = new TreeSet<CurlyFilePath>();
    Set<String> paths = FsUtils.getMatchingPaths(DELTA_REGEX, dirs);
    for (String path : paths) {
      result.add(new CurlyFilePath(path));
    }
    return result;
  }

  public static String getName(int versionNumber, boolean base) {
    String s = padVersionNumber(versionNumber) + ".";
    if (base) {
      s += "base";
    } else {
      s += "delta";
    }
    return s + ".curly";
  }

  public static String getName(DomainVersion domainVersion) throws IOException {
    return getName(domainVersion.getVersionNumber(), IncrementalDomainVersionProperties.isBase(domainVersion));
  }

  @Override
  public RemoteDomainVersionDeleter getRemoteDomainVersionDeleter(RemoteLocation location) throws IOException {
    return new CurlyRemoteDomainVersionDeleter(domain, getRoot(location), partitionRemoteFileOpsFactory);
  }

  @Override
  public RemoteDomainCleaner getRemoteDomainCleaner() throws IOException {
    return new CurlyRemoteDomainCleaner(domain, numRemoteLeafVersionsToKeep);
  }

  @Override
  public DiskPartitionAssignment getDataDirectoryPerPartition(DataDirectoriesConfigurator configurator, Collection<Integer> partitionNumbers) {
    return Cueball.getDataDirectoryAssignments(configurator, partitionNumbers);
  }

  private String getTargetDirectory(DiskPartitionAssignment assignment, int partitionNumber) {
    return assignment.getDisk(partitionNumber) + "/" + domain.getName() + "/" + partitionNumber;
  }

  @Override
  public Set<String> getFiles(DiskPartitionAssignment assignment, int domainVersionNumber, int partitionNumber) throws IOException {
    Set<String> result = new HashSet<String>();
    result.addAll(cueballStorageEngine.getFiles(assignment, domainVersionNumber, partitionNumber));
    result.add(getTargetDirectory(assignment, partitionNumber) + "/" + getName(domainVersionNumber, true));
    return result;
  }

  @Override
  public String toString() {
    return "Curly{" +
        ", offsetNumBytes=" + offsetNumBytes +
        ", recordFileReadBufferBytes=" + recordFileReadBufferBytes +
        ", cueballStorageEngine=" + cueballStorageEngine +
        ", domainBuilderRemoteDomainRoot='" + domainBuilderRemoteDomainRoot + '\'' +
        ", partitionServerRemoteDomainRoot='" + partitionServerRemoteDomainRoot + '\'' +
        ", keyHashSize=" + keyHashSize +
        ", partitionRemoteFileOpsFactory=" + partitionRemoteFileOpsFactory +
        ", hashIndexBits=" + hashIndexBits +
        ", keyFileCompressionCodecClass=" + keyFileCompressionCodecClass +
        ", numRemoteLeafVersionsToKeep=" + numRemoteLeafVersionsToKeep +
        ", valueFoldingCacheCapacity=" + valueFoldingCacheCapacity +
        ", blockCompressionCodec=" + blockCompressionCodec +
        ", compressedBlockSizeThreshold=" + compressedBlockSizeThreshold +
        ", offsetInBlockNumBytes=" + offsetInBlockNumBytes +
        ", cueballValueNumBytes=" + cueballValueNumBytes +
        '}';
  }
}
