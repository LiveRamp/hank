package com.liveramp.hank.storage;

import java.util.Map;

import static com.liveramp.hank.storage.curly.Curly.Factory.REMOTE_DOMAIN_ROOT_KEY;

public class FileOpsUtil {

  public static final String PARTITION_SERVER_DOMAIN_ROOT_KEY = "partition_server_remote_domain_root";
  public static final String DOMAIN_BUILDER_DOMAIN_ROOT_KEY = "domain_builder_remote_domain_root";


  public static String getPartitionServerRoot(Map<String, Object> options){

    if(options.containsKey(REMOTE_DOMAIN_ROOT_KEY)){
      return (String)options.get(REMOTE_DOMAIN_ROOT_KEY);
    }

    return (String) options.get(PARTITION_SERVER_DOMAIN_ROOT_KEY);

  }

  public static String getDomainBuilderRoot(Map<String, Object> options){

    if(options.containsKey(REMOTE_DOMAIN_ROOT_KEY)){
      return (String)options.get(REMOTE_DOMAIN_ROOT_KEY);
    }

    return (String) options.get(DOMAIN_BUILDER_DOMAIN_ROOT_KEY);
  }

}