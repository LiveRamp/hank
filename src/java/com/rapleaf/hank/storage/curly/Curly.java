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
package com.rapleaf.hank.storage.curly;

import com.rapleaf.hank.compress.CompressionCodec;
import com.rapleaf.hank.compress.NoCompressionCodec;
import com.rapleaf.hank.config.DataDirectoriesConfigurator;
import com.rapleaf.hank.coordinator.Domain;
import com.rapleaf.hank.coordinator.DomainVersion;
import com.rapleaf.hank.coordinator.DomainVersionPropertiesSerialization;
import com.rapleaf.hank.hasher.Hasher;
import com.rapleaf.hank.hasher.Murmur64Hasher;
import com.rapleaf.hank.storage.*;
import com.rapleaf.hank.storage.Reader;
import com.rapleaf.hank.storage.Writer;
import com.rapleaf.hank.storage.cueball.Cueball;
import com.rapleaf.hank.storage.cueball.CueballMerger;
import com.rapleaf.hank.storage.cueball.CueballStreamBufferMergeSort;
import com.rapleaf.hank.storage.incremental.IncrementalDomainVersionProperties;
import com.rapleaf.hank.util.FsUtils;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Curly is a storage engine designed for larger, variable-sized values. It uses
 * Cueball under the hood.
 */
public class Curly implements StorageEngine {

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
    public static final String KEY_FILE_PARTITION_CACHE_CAPACITY = "key_file_partition_cache_capacity";
    public static final String RECORD_FILE_PARTITION_CACHE_CAPACITY = "record_file_partition_cache_capacity";
    private static final String BLOCK_COMPRESSION_CODEC = "block_compression_codec";
    private static final String COMPRESSED_BLOCK_SIZE_THRESHOLD = "compressed_block_size_threshold";
    private static final String OFFSET_IN_BLOCK_NUM_BYTES = "offset_in_block_num_bytes";

    private static final Set<String> REQUIRED_KEYS = new HashSet<String>(Arrays.asList(REMOTE_DOMAIN_ROOT_KEY,
        RECORD_FILE_READ_BUFFER_BYTES_KEY, HASH_INDEX_BITS_KEY, MAX_ALLOWED_PART_SIZE_KEY, KEY_HASH_SIZE_KEY,
        FILE_OPS_FACTORY_KEY, HASHER_KEY, NUM_REMOTE_LEAF_VERSIONS_TO_KEEP));

    @Override
    public StorageEngine getStorageEngine(Map<String, Object> options, Domain domain) throws IOException {
      for (String requiredKey : REQUIRED_KEYS) {
        if (options == null || options.get(requiredKey) == null) {
          throw new RuntimeException("Required key '" + requiredKey
              + "' was not found!");
        }
      }

      Hasher hasher;
      PartitionRemoteFileOpsFactory fileOpsFactory;
      Class<? extends CompressionCodec> compressionCodecClass;
      try {
        hasher = (Hasher) Class.forName((String) options.get(HASHER_KEY)).newInstance();
        fileOpsFactory = (PartitionRemoteFileOpsFactory) Class.forName((String) options.get(FILE_OPS_FACTORY_KEY)).newInstance();

        String compressionCodecClassName = (String) options.get(COMPRESSION_CODEC);
        if (compressionCodecClassName == null) {
          compressionCodecClass = NoCompressionCodec.class;
        } else {
          compressionCodecClass = (Class<? extends CompressionCodec>) Class.forName(compressionCodecClassName);
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      final long maxAllowedPartSize = options.get(MAX_ALLOWED_PART_SIZE_KEY) instanceof Long ? (Long) options.get(MAX_ALLOWED_PART_SIZE_KEY)
          : ((Integer) options.get(MAX_ALLOWED_PART_SIZE_KEY)).longValue();

      // num remote bases to keep
      Integer numRemoteLeafVersionsToKeep = (Integer) options.get(NUM_REMOTE_LEAF_VERSIONS_TO_KEEP);

      // Value folding cache size
      Integer valueFoldingCacheCapacity = (Integer) options.get(VALUE_FOLDING_CACHE_CAPACITY);
      if (valueFoldingCacheCapacity == null) {
        valueFoldingCacheCapacity = -1;
      }

      // Cache capacity
      Integer keyFilePartitionCacheCapacity = (Integer) options.get(KEY_FILE_PARTITION_CACHE_CAPACITY);
      if (keyFilePartitionCacheCapacity == null) {
        keyFilePartitionCacheCapacity = -1;
      }
      Integer recordFilePartitionCacheCapacity = (Integer) options.get(RECORD_FILE_PARTITION_CACHE_CAPACITY);
      if (recordFilePartitionCacheCapacity == null) {
        recordFilePartitionCacheCapacity = -1;
      }

      // Block compression
      BlockCompressionCodec blockCompressionCodec = null;
      String blockCompressionCodecStr = (String) options.get(BLOCK_COMPRESSION_CODEC);
      if (blockCompressionCodecStr != null) {
        blockCompressionCodec = BlockCompressionCodec.valueOf(blockCompressionCodecStr.toUpperCase());
      }
      Integer compressedBlockSizeThreshold = (Integer) options.get(COMPRESSED_BLOCK_SIZE_THRESHOLD);
      if (compressedBlockSizeThreshold == null) {
        compressedBlockSizeThreshold = -1;
      }
      Integer offsetInBlockNumBytes = (Integer) options.get(OFFSET_IN_BLOCK_NUM_BYTES);
      if (offsetInBlockNumBytes == null) {
        offsetInBlockNumBytes = -1;
      }

      return new Curly((Integer) options.get(KEY_HASH_SIZE_KEY),
          hasher,
          maxAllowedPartSize,
          (Integer) options.get(HASH_INDEX_BITS_KEY),
          (Integer) options.get(RECORD_FILE_READ_BUFFER_BYTES_KEY),
          (String) options.get(REMOTE_DOMAIN_ROOT_KEY),
          fileOpsFactory,
          compressionCodecClass,
          domain,
          numRemoteLeafVersionsToKeep,
          valueFoldingCacheCapacity,
          keyFilePartitionCacheCapacity,
          recordFilePartitionCacheCapacity,
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
      StringWriter sw = new StringWriter();
      PrintWriter pw = new PrintWriter(sw);

      pw.println("---");

      pw.println("# The path where your remote domain files reside.");
      pw.println(REMOTE_DOMAIN_ROOT_KEY + ": # FILL THIS IN!!!");
      pw.println();
      pw.println("# The File Ops Factory lets Curly know where to retrieve remote data from.");
      pw.println("# If you're running over NFS or something like that, leave this alone.");
      pw.println("# If you are using Hadoop, switch to the HdfsFileOpsFactory.");
      pw.println(FILE_OPS_FACTORY_KEY + ": "
          + LocalPartitionRemoteFileOps.Factory.class.getName());
      pw.println("#" + FILE_OPS_FACTORY_KEY + ": "
          + HdfsPartitionRemoteFileOps.Factory.class.getName());
      pw.println();
      pw.println("# This allows partitions up to 1TB in size. If you don't need");
      pw.println("# them that big, try reducing this value to 4294967296 (4GB)");
      pw.println("# to reduce the offset size to 4 bytes.");
      pw.println(MAX_ALLOWED_PART_SIZE_KEY + ": " + (1L << 40));
      pw.println();
      pw.println("# The class name of the hasher you want to use. You should only");
      pw.println("# need to adjust this if you have very particular needs.");
      pw.println(HASHER_KEY + ": " + Murmur64Hasher.class.getName());
      pw.println();
      pw.println("# 12-byte hashes are more than sufficient for most people,");
      pw.println("# unless you have just tons of keys.");
      pw.println(KEY_HASH_SIZE_KEY + ": 12");
      pw.println();
      pw.println("# This setting adjusts the number of bits used for the hash index prefixes.");
      pw.println("# Reducing this number can reduce memory usage, but increase the size of blocks.");
      pw.println("# Adjust with caution.");
      pw.println(HASH_INDEX_BITS_KEY + ": 15");
      pw.println();
      pw.println("# This value should be set to a number around the size of your");
      pw.println("# average value, but not much smaller than 32KB. You can safely");
      pw.println("# ignore it in most cases.");
      pw.println(RECORD_FILE_READ_BUFFER_BYTES_KEY + ": " + (32 * 1024));
      pw.println();
      pw.println("# Set the number of newest full versions (bases + deltas) to keep on remote storage. 0 means keep all versions.");
      pw.println(NUM_REMOTE_LEAF_VERSIONS_TO_KEEP + ": " + 0);
      pw.println();
      pw.println("# Optional: compression codec. If no codec is specified,");
      pw.println("# no compression will be used. Be sure to verify that compression");
      pw.println("# actually helps you! If you are just using arbitrary hashed keys,");
      pw.println("# it will probably make things worse!");
      pw.println("#" + COMPRESSION_CODEC + ": " + NoCompressionCodec.class.getName());

      return sw.toString();
    }
  }

  private final Domain domain;
  private final int recordFilePartitionCacheCapacity;

  private final int offsetNumBytes;
  private final int recordFileReadBufferBytes;

  private final Cueball cueballStorageEngine;
  private final String remoteDomainRoot;
  private final int keyHashSize;
  private final PartitionRemoteFileOpsFactory partitionRemoteFileOpsFactory;
  private final int hashIndexBits;
  private final Class<? extends CompressionCodec> keyFileCompressionCodecClass;
  private final int numRemoteLeafVersionsToKeep;
  private final int valueFoldingCacheCapacity;
  private final BlockCompressionCodec blockCompressionCodec;
  private final int compressedBlockSizeThreshold;
  private final int offsetInBlockNumBytes;

  public Curly(int keyHashSize,
               Hasher hasher,
               long maxAllowedPartSize,
               int hashIndexBits,
               int recordFileReadBufferBytes,
               String remoteDomainRoot,
               PartitionRemoteFileOpsFactory partitionRemoteFileOpsFactory,
               Class<? extends CompressionCodec> keyFileCompressionCodecClass,
               Domain domain,
               int numRemoteLeafVersionsToKeep,
               int valueFoldingCacheCapacity,
               int keyFilePartitionCacheCapacity,
               int recordFilePartitionCacheCapacity,
               BlockCompressionCodec blockCompressionCodec,
               int compressedBlockSizeThreshold,
               int offsetInBlockNumBytes) {
    this.keyHashSize = keyHashSize;
    this.hashIndexBits = hashIndexBits;
    this.recordFileReadBufferBytes = recordFileReadBufferBytes;
    this.remoteDomainRoot = remoteDomainRoot;
    this.partitionRemoteFileOpsFactory = partitionRemoteFileOpsFactory;
    this.keyFileCompressionCodecClass = keyFileCompressionCodecClass;
    this.domain = domain;
    this.numRemoteLeafVersionsToKeep = numRemoteLeafVersionsToKeep;
    this.valueFoldingCacheCapacity = valueFoldingCacheCapacity;
    this.recordFilePartitionCacheCapacity = recordFilePartitionCacheCapacity;
    this.blockCompressionCodec = blockCompressionCodec;
    this.compressedBlockSizeThreshold = compressedBlockSizeThreshold;
    this.offsetInBlockNumBytes = offsetInBlockNumBytes;

    this.offsetNumBytes = (int) (Math.ceil(Math.ceil(Math.log(maxAllowedPartSize) / Math.log(2)) / 8.0));

    // Determine size of values in Cueball. If we are using block compression in Curly,
    // the offsets stored in Cueball are appended with the offset in the block.
    int cueballValueNumBytes = offsetNumBytes;
    if (blockCompressionCodec != null) {
      cueballValueNumBytes += offsetInBlockNumBytes;
    }

    this.cueballStorageEngine = new Cueball(keyHashSize,
        hasher,
        cueballValueNumBytes,
        hashIndexBits,
        remoteDomainRoot,
        partitionRemoteFileOpsFactory,
        keyFileCompressionCodecClass,
        domain,
        numRemoteLeafVersionsToKeep,
        keyFilePartitionCacheCapacity);
  }

  @Override
  public Reader getReader(DataDirectoriesConfigurator configurator, int partitionNumber) throws IOException {
    return new CurlyReader(CurlyReader.getLatestBase(getLocalDir(configurator, partitionNumber)),
        recordFileReadBufferBytes,
        cueballStorageEngine.getReader(configurator, partitionNumber),
        recordFilePartitionCacheCapacity,
        blockCompressionCodec,
        offsetNumBytes,
        offsetInBlockNumBytes);
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
      result = (IncrementalDomainVersionProperties) domainVersion.getProperties();
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
  public PartitionUpdater getUpdater(DataDirectoriesConfigurator configurator, int partitionNumber) throws IOException {
    File localDir = new File(getLocalDir(configurator, partitionNumber));
    if (!localDir.exists() && !localDir.mkdirs()) {
      throw new RuntimeException("Failed to create directory " + localDir.getAbsolutePath());
    }
    return getFastPartitionUpdater(localDir.getAbsolutePath(), partitionNumber);
  }

  @Override
  public Compactor getCompactor(DataDirectoriesConfigurator configurator,
                                int partitionNumber) throws IOException {
    File localDir = new File(getLocalDir(configurator, partitionNumber));
    if (!localDir.exists() && !localDir.mkdirs()) {
      throw new RuntimeException("Failed to create directory " + localDir.getAbsolutePath());
    }
    return getCompactor(localDir.getAbsolutePath(), partitionNumber);
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
        partitionRemoteFileOpsFactory.getPartitionRemoteFileOps(remoteDomainRoot, partitionNumber),
        localDir,
        new CurlyCompactingMerger(recordFileReadBufferBytes),
        new CueballStreamBufferMergeSort.Factory(keyHashSize, offsetNumBytes, hashIndexBits, getCompressionCodec(), null),
        new ICurlyReaderFactory() {
          @Override
          public ICurlyReader getInstance(CurlyFilePath curlyFilePath) throws IOException {
            // Note: key file reader is null as it will *not* be used
            return new CurlyReader(curlyFilePath, recordFileReadBufferBytes,
                null, -1, blockCompressionCodec, offsetNumBytes, offsetInBlockNumBytes);
          }
        }
    );
  }

  private CurlyFastPartitionUpdater getFastPartitionUpdater(String localDir, int partNum) throws IOException {
    return new CurlyFastPartitionUpdater(domain,
        partitionRemoteFileOpsFactory.getPartitionRemoteFileOps(remoteDomainRoot, partNum),
        new CurlyMerger(),
        new CueballMerger(),
        keyHashSize,
        offsetNumBytes,
        offsetInBlockNumBytes,
        hashIndexBits,
        getCompressionCodec(),
        localDir);
  }

  private CompressionCodec getCompressionCodec() throws IOException {
    try {
      return keyFileCompressionCodecClass.newInstance();
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public Deleter getDeleter(DataDirectoriesConfigurator configurator, int partitionNumber)
      throws IOException {
    String localDir = getLocalDir(configurator, partitionNumber);
    return new CurlyDeleter(localDir);
  }

  @Override
  public ByteBuffer getComparableKey(ByteBuffer key) {
    return cueballStorageEngine.getComparableKey(key);
  }

  @Override
  public PartitionRemoteFileOpsFactory getPartitionRemoteFileOpsFactory() {
    return partitionRemoteFileOpsFactory;
  }

  private String getLocalDir(DataDirectoriesConfigurator configurator, int partNum) {
    ArrayList<String> l = new ArrayList<String>(configurator.getDataDirectories());
    Collections.sort(l);
    return l.get(partNum % l.size()) + "/" + domain.getName() + "/" + partNum;
  }

  public static int parseVersionNumber(String name) {
    Matcher matcher = BASE_OR_REGEX_PATTERN.matcher(name);
    if (!matcher.matches()) {
      throw new IllegalArgumentException("string " + name
          + " isn't a path that parseVersionNumber can parse!");
    }

    return Integer.parseInt(matcher.group(1));
  }

  public static SortedSet<CurlyFilePath> getBases(String... dirs) {
    SortedSet<CurlyFilePath> result = new TreeSet<CurlyFilePath>();
    Set<String> paths = FsUtils.getMatchingPaths(BASE_REGEX, dirs);
    for (String path : paths) {
      result.add(new CurlyFilePath(path));
    }
    return result;
  }

  public static SortedSet<CurlyFilePath> getDeltas(String... dirs) {
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
  public String toString() {
    return "Curly [compressionCodecClass=" + keyFileCompressionCodecClass
        + ", cueballStorageEngine=" + cueballStorageEngine
        + ", domainName=" + domain.getName()
        + ", fileOpsFactory=" + partitionRemoteFileOpsFactory
        + ", hashIndexBits=" + hashIndexBits
        + ", keyHashSize=" + keyHashSize
        + ", offsetNumBytes=" + offsetNumBytes
        + ", recordFileReadBufferBytes=" + recordFileReadBufferBytes
        + ", remoteDomainRoot=" + remoteDomainRoot
        + ", numRemoteLeafVersionsToKeep=" + numRemoteLeafVersionsToKeep
        + ", valueFoldingCacheSize=" + valueFoldingCacheCapacity
        + ", blockCompressionCodec=" + blockCompressionCodec
        + ", compressedBlockSizeThreshold=" + compressedBlockSizeThreshold
        + ", offsetInBlockNumBytes=" + offsetInBlockNumBytes
        + "]";
  }

  @Override
  public RemoteDomainVersionDeleter getRemoteDomainVersionDeleter() throws IOException {
    return new CurlyRemoteDomainVersionDeleter(domain, remoteDomainRoot, partitionRemoteFileOpsFactory);
  }

  @Override
  public RemoteDomainCleaner getRemoteDomainCleaner() throws IOException {
    return new CurlyRemoteDomainCleaner(domain, numRemoteLeafVersionsToKeep);
  }

  @Override
  public DomainVersionPropertiesSerialization getDomainVersionPropertiesSerialization() {
    return new IncrementalDomainVersionProperties.Serialization();
  }
}
