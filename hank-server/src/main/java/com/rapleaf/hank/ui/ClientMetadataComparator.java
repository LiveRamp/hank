package com.rapleaf.hank.ui;

import com.rapleaf.hank.generated.ClientMetadata;

import java.util.Comparator;

public class ClientMetadataComparator implements Comparator<ClientMetadata> {
  @Override
  public int compare(ClientMetadata a, ClientMetadata b) {
    return a.get_host().compareTo(b.get_host());
  }
}
