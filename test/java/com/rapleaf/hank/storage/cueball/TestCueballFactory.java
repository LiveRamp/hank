package com.rapleaf.hank.storage.cueball;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.rapleaf.hank.BaseTestCase;
import com.rapleaf.hank.hasher.Murmur64Hasher;
import com.rapleaf.hank.storage.StorageEngine;

public class TestCueballFactory extends BaseTestCase {
  private static final Logger LOG = Logger.getLogger(TestCueballFactory.class);

  public void testBadConfig() throws Exception {
    Cueball.Factory factory = new Cueball.Factory();

    Map<String, Object> badOptions = new HashMap<String, Object>();
    try {
      factory.getStorageEngine(badOptions, "domain0");
      fail();
    } catch (Exception e) {
      LOG.error("", e);
    }
  }

  public void testGoodConfig() throws Exception {
    Cueball.Factory factory = new Cueball.Factory();
    Map<String,Object> options = new HashMap<String, Object>();
    options.put(Cueball.Factory.HASHER_KEY, Murmur64Hasher.class.getName());
    options.put(Cueball.Factory.FILE_OPS_FACTORY_KEY, LocalFileOps.Factory.class.getName());
    options.put(Cueball.Factory.HASH_INDEX_BITS_KEY, 5);
    options.put(Cueball.Factory.KEY_HASH_SIZE_KEY, 7);
    options.put(Cueball.Factory.READ_BUFFER_BYTES_KEY, 32000);
    options.put(Cueball.Factory.REMOTE_DOMAIN_ROOT_KEY, "/path/to/what/i/say");
    options.put(Cueball.Factory.VALUE_SIZE_KEY, 15);
    StorageEngine storageEngine = factory.getStorageEngine(options, "domain0");
  }
}
