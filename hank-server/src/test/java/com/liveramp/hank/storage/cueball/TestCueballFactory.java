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
package com.liveramp.hank.storage.cueball;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Sets;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.liveramp.hank.config.SimpleDataDirectoriesConfigurator;
import com.liveramp.hank.hasher.Murmur64Hasher;
import com.liveramp.hank.partition_server.DiskPartitionAssignment;
import com.liveramp.hank.storage.LocalPartitionRemoteFileOps;
import com.liveramp.hank.test.BaseTestCase;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TestCueballFactory extends BaseTestCase {
  private static final Logger LOG = LoggerFactory.getLogger(TestCueballFactory.class);

  @Test
  public void testBadConfig() throws Exception {
    Cueball.Factory factory = new Cueball.Factory();

    Map<String, Object> badOptions = new HashMap<String, Object>();
    try {
      factory.getStorageEngine(badOptions, null);
      fail();
    } catch (Exception e) {
      LOG.error("", e);
    }
  }

  @Test
  public void testGoodConfig() throws Exception {
    Cueball.Factory factory = new Cueball.Factory();
    Map<String, Object> options = new HashMap<String, Object>();
    options.put(Cueball.Factory.HASHER_KEY, Murmur64Hasher.class.getName());
    options.put(Cueball.Factory.FILE_OPS_FACTORY_KEY, LocalPartitionRemoteFileOps.Factory.class.getName());
    options.put(Cueball.Factory.HASH_INDEX_BITS_KEY, 5);
    options.put(Cueball.Factory.KEY_HASH_SIZE_KEY, 7);
    options.put(Cueball.Factory.REMOTE_DOMAIN_ROOT_KEY, "/path/to/what/i/say");
    options.put(Cueball.Factory.VALUE_SIZE_KEY, 15);
    options.put(Cueball.Factory.NUM_REMOTE_LEAF_VERSIONS_TO_KEEP, 0);
    factory.getStorageEngine(options, null);
  }

  @Test
  public void testGetDataDirectory() {

    SimpleDataDirectoriesConfigurator configurator = new SimpleDataDirectoriesConfigurator(Sets.newHashSet("A", "B", "C"));

    Set<Integer> partitions = Sets.newHashSet(
      1, 4, 8, 12, 33, 89, 3
    );

    DiskPartitionAssignment assignments = Cueball.getDataDirectoryAssignments(configurator, partitions);

    assertEquals("A", assignments.getDisk(1));
    assertEquals("A", assignments.getDisk(3));
    assertEquals("A", assignments.getDisk(4));

    assertEquals("B", assignments.getDisk(8));
    assertEquals("B", assignments.getDisk(12));

    assertEquals("C", assignments.getDisk(33));
    assertEquals("C", assignments.getDisk(89));

    partitions = Sets.newHashSet(
        1, 33
    );

    assignments = Cueball.getDataDirectoryAssignments(configurator, partitions);

    assertEquals("A", assignments.getDisk(1));
    assertEquals("B", assignments.getDisk(33));

  }
}
