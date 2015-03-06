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

import com.liveramp.hank.test.BaseTestCase;
import com.liveramp.hank.hasher.Murmur64Hasher;
import com.liveramp.hank.storage.LocalPartitionRemoteFileOps;
import com.liveramp.hank.storage.cueball.TestCueballFactory;
import org.slf4j.Logger; import org.slf4j.LoggerFactory;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.fail;

public class TestCurlyFactory extends BaseTestCase {
  private static final Logger LOG = LoggerFactory.getLogger(TestCueballFactory.class);

  @Test
  public void testBadConfig() throws Exception {
    Curly.Factory factory = new Curly.Factory();

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
    Curly.Factory factory = new Curly.Factory();
    Map<String, Object> options = new HashMap<String, Object>();
    options.put(Curly.Factory.HASHER_KEY, Murmur64Hasher.class.getName());
    options.put(Curly.Factory.FILE_OPS_FACTORY_KEY, LocalPartitionRemoteFileOps.Factory.class.getName());
    options.put(Curly.Factory.HASH_INDEX_BITS_KEY, 5);
    options.put(Curly.Factory.KEY_HASH_SIZE_KEY, 7);
    options.put(Curly.Factory.RECORD_FILE_READ_BUFFER_BYTES_KEY, 32000);
    options.put(Curly.Factory.REMOTE_DOMAIN_ROOT_KEY, "/path/to/what/i/say");
    options.put(Curly.Factory.MAX_ALLOWED_PART_SIZE_KEY, 15000);
    options.put(Curly.Factory.NUM_REMOTE_LEAF_VERSIONS_TO_KEEP, 0);
    factory.getStorageEngine(options, null);
  }
}
