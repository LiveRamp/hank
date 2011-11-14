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

import com.rapleaf.hank.BaseTestCase;
import com.rapleaf.hank.hasher.Murmur64Hasher;
import com.rapleaf.hank.storage.LocalPartitionRemoteFileOps;
import com.rapleaf.hank.storage.cueball.TestCueballFactory;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

public class TestCurlyFactory extends BaseTestCase {
  private static final Logger LOG = Logger.getLogger(TestCueballFactory.class);

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
    factory.getStorageEngine(options, null);
  }
}
