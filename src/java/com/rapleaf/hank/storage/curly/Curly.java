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

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.rapleaf.hank.compress.CompressionCodec;
import com.rapleaf.hank.compress.NoCompressionCodec;
import com.rapleaf.hank.config.PartservConfigurator;
import com.rapleaf.hank.hasher.Hasher;
import com.rapleaf.hank.hasher.Murmur64Hasher;
import com.rapleaf.hank.storage.OutputStreamFactory;
import com.rapleaf.hank.storage.Reader;
import com.rapleaf.hank.storage.StorageEngine;
import com.rapleaf.hank.storage.StorageEngineFactory;
import com.rapleaf.hank.storage.Updater;
import com.rapleaf.hank.storage.Writer;
import com.rapleaf.hank.storage.cueball.Cueball;
import com.rapleaf.hank.storage.cueball.HdfsFileOps;
import com.rapleaf.hank.storage.cueball.IFileOpsFactory;
import com.rapleaf.hank.storage.cueball.LocalFileOps;
import com.rapleaf.hank.util.FsUtils;

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

    private static final Set<String> REQUIRED_KEYS = new HashSet<String>(Arrays.asList(REMOTE_DOMAIN_ROOT_KEY, RECORD_FILE_READ_BUFFER_BYTES_KEY, HASH_INDEX_BITS_KEY, MAX_ALLOWED_PART_SIZE_KEY, KEY_HASH_SIZE_KEY, FILE_OPS_FACTORY_KEY, HASHER_KEY));

    @Override
    public StorageEngine getStorageEngine(Map<String, Object> options, String domainName) throws IOException {
      for (String requiredKey : REQUIRED_KEYS) {
        if (options == null || options.get(requiredKey) == null) {
          throw new RuntimeException("Required key '" + requiredKey
              + "' was not found!");
        }
      }

      Hasher hasher;
      IFileOpsFactory fileOpsFactory;
      Class<? extends CompressionCodec> compressionCodecClass;
      try {
        hasher = (Hasher) Class.forName((String) options.get(HASHER_KEY)).newInstance();
        fileOpsFactory = (IFileOpsFactory) Class.forName((String) options.get(FILE_OPS_FACTORY_KEY)).newInstance();

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
      return new Curly((Integer) options.get(KEY_HASH_SIZE_KEY), hasher, maxAllowedPartSize, (Integer) options.get(HASH_INDEX_BITS_KEY), (Integer) options.get(RECORD_FILE_READ_BUFFER_BYTES_KEY), (String) options.get(REMOTE_DOMAIN_ROOT_KEY), fileOpsFactory, compressionCodecClass, domainName);
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
          + LocalFileOps.Factory.class.getName());
      pw.println("#" + FILE_OPS_FACTORY_KEY + ": "
          + HdfsFileOps.Factory.class.getName());
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
      pw.println("# Optional: compression codec. If no codec is specified,");
      pw.println("# no compression will be used. Be sure to verify that compression");
      pw.println("# actually helps you! If you are just using arbitrary hashed keys,");
      pw.println("# it will probably make things worse!");
      pw.println("#" + COMPRESSION_CODEC + ": " + NoCompressionCodec.class.getName());

      return sw.toString();
    }
  }

  private final String domainName;

  private final int offsetSize;
  private final int recordFileReadBufferBytes;

  private final Cueball cueballStorageEngine;
  private final String remoteDomainRoot;
  private final int keyHashSize;
  private final IFileOpsFactory fileOpsFactory;
  private final int hashIndexBits;
  private final Class<? extends CompressionCodec> compressionCodecClass;

  public Curly(int keyHashSize,
      Hasher hasher,
      long maxAllowedPartSize,
      int hashIndexBits,
      int recordFileReadBufferBytes,
      String remoteDomainRoot,
      IFileOpsFactory fileOpsFactory,
      Class<? extends CompressionCodec> compressionCodecClass,
      String domainName) {
    this.keyHashSize = keyHashSize;
    this.hashIndexBits = hashIndexBits;
    this.recordFileReadBufferBytes = recordFileReadBufferBytes;
    this.remoteDomainRoot = remoteDomainRoot;
    this.fileOpsFactory = fileOpsFactory;
    this.compressionCodecClass = compressionCodecClass;
    this.domainName = domainName;
    this.offsetSize = (int) (Math.ceil(Math.ceil(Math.log(maxAllowedPartSize)
        / Math.log(2)) / 8.0));
    this.cueballStorageEngine = new Cueball(keyHashSize, hasher, offsetSize, hashIndexBits, remoteDomainRoot, fileOpsFactory, compressionCodecClass, domainName);
  }

  @Override
  public Reader getReader(PartservConfigurator configurator, int partNum) throws IOException {
    return new CurlyReader(getLocalDir(configurator, partNum), recordFileReadBufferBytes, cueballStorageEngine.getReader(configurator, partNum));
  }

  @Override
  public Writer getWriter(OutputStreamFactory streamFactory, int partNum, int versionNumber, boolean base) throws IOException {
    OutputStream outputStream = streamFactory.getOutputStream(partNum, getName(versionNumber, base));
    Writer cueballWriter = cueballStorageEngine.getWriter(streamFactory, partNum, versionNumber, base);
    return new CurlyWriter(outputStream, cueballWriter, offsetSize);
  }

  private String padVersion(int versionNumber) {
    return String.format("%05d", versionNumber);
  }

  @Override
  public Updater getUpdater(PartservConfigurator configurator, int partNum) throws IOException {
    String localDir = getLocalDir(configurator, partNum);
    new File(localDir).mkdirs();
    String remotePartRoot = remoteDomainRoot + "/" + partNum;
    return new CurlyUpdater(localDir, remotePartRoot, keyHashSize, offsetSize, fileOpsFactory.getFileOps(localDir, remotePartRoot), getCompressionCodec(), hashIndexBits);
  }

  private CompressionCodec getCompressionCodec() throws IOException {
    try {
      return compressionCodecClass.newInstance();
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public ByteBuffer getComparableKey(ByteBuffer key) {
    return cueballStorageEngine.getComparableKey(key);
  }

  private String getLocalDir(PartservConfigurator configurator, int partNum) {
    ArrayList<String> l = new ArrayList<String>(configurator.getLocalDataDirectories());
    Collections.sort(l);
    return l.get(partNum % l.size()) + "/" + domainName + "/" + partNum;
  }

  public static int parseVersionNumber(String name) {
    Matcher matcher = BASE_OR_REGEX_PATTERN.matcher(name);
    if (!matcher.matches()) {
      throw new IllegalArgumentException("string " + name
          + " isn't a path that parseVersionNumber can parse!");
    }

    return Integer.parseInt(matcher.group(1));
  }

  public static SortedSet<String> getBases(String localPartitionRoot) {
    return FsUtils.getMatchingPaths(localPartitionRoot, BASE_REGEX);
  }

  public static SortedSet<String> getDeltas(String localPartitionRoot) {
    return FsUtils.getMatchingPaths(localPartitionRoot, DELTA_REGEX);
  }

  private String getName(int versionNumber, boolean base) {
    String s = padVersion(versionNumber) + ".";
    if (base) {
      s += "base";
    } else {
      s += "delta";
    }
    return s + ".curly";
  }

  @Override
  public String toString() {
    return "Curly [compressionCodecClass=" + compressionCodecClass
        + ", cueballStorageEngine=" + cueballStorageEngine + ", domainName="
        + domainName + ", fileOpsFactory=" + fileOpsFactory
        + ", hashIndexBits=" + hashIndexBits + ", keyHashSize=" + keyHashSize
        + ", offsetSize=" + offsetSize + ", recordFileReadBufferBytes="
        + recordFileReadBufferBytes + ", remoteDomainRoot=" + remoteDomainRoot
        + "]";
  }
}
