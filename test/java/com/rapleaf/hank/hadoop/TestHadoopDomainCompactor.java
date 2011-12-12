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

package com.rapleaf.hank.hadoop;

import com.rapleaf.hank.config.CoordinatorConfigurator;
import com.rapleaf.hank.config.DataDirectoriesConfigurator;
import com.rapleaf.hank.coordinator.Coordinator;
import com.rapleaf.hank.coordinator.Domain;
import com.rapleaf.hank.coordinator.DomainVersion;
import com.rapleaf.hank.coordinator.mock.MockCoordinator;
import com.rapleaf.hank.coordinator.mock.MockDomain;
import com.rapleaf.hank.coordinator.mock.MockDomainVersion;
import com.rapleaf.hank.storage.Compactor;
import com.rapleaf.hank.storage.Writer;
import com.rapleaf.hank.storage.mock.MockStorageEngine;

import java.io.IOException;
import java.util.Collections;

public class TestHadoopDomainCompactor extends HadoopTestCase {

  private final String DOMAIN_A_NAME = "a";
  private final String OUTPUT_PATH_A = OUTPUT_DIR + "/" + DOMAIN_A_NAME;

  public TestHadoopDomainCompactor() throws IOException {
    super(TestHadoopDomainCompactor.class);
  }

  public void setUp() throws Exception {
    super.setUp();
    LocalMockCoordinatorConfigurator.compactor = new LocalMockCompactor();
    LocalMockCoordinatorConfigurator.versionToCompact = new MockDomainVersion(0, (long) 0);
  }

  private static class LocalMockCompactor implements Compactor {

    public DomainVersion versionToCompact;
    public int numCalls = 0;

    @Override
    public void compact(DomainVersion versionToCompact, Writer writer) throws IOException {
      this.versionToCompact = versionToCompact;
      ++numCalls;
    }
  }

  private static class LocalMockCoordinatorConfigurator implements CoordinatorConfigurator {

    private static LocalMockCompactor compactor;
    private static DomainVersion versionToCompact;

    @Override
    public Coordinator createCoordinator() {
      return new MockCoordinator() {
        @Override
        public Domain getDomain(String domainName) {
          return new MockDomain(domainName, 0, 2, null,
              new MockStorageEngine() {
                @Override
                public Compactor getCompactor(DataDirectoriesConfigurator configurator,
                                              int partitionNumber) throws IOException {
                  return compactor;
                }
              }, Collections.<String, Object>emptyMap(), versionToCompact);
        }
      };
    }
  }

  public void testMain() throws IOException {
    CoordinatorConfigurator configurator = new LocalMockCoordinatorConfigurator();
    DomainBuilderProperties properties =
        new DomainCompactorProperties(DOMAIN_A_NAME, 0, configurator, OUTPUT_PATH_A);
    new HadoopDomainCompactor().buildHankDomain(properties);

    // Check that compactor was called with correct version twice
    assertEquals(2, LocalMockCoordinatorConfigurator.compactor.numCalls);
    assertEquals(LocalMockCoordinatorConfigurator.versionToCompact.getVersionNumber(),
        LocalMockCoordinatorConfigurator.compactor.versionToCompact.getVersionNumber());
  }
}
