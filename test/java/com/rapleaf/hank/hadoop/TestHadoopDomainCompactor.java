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
import com.rapleaf.hank.storage.VersionType;

import java.io.IOException;

public class TestHadoopDomainCompactor extends HadoopTestCase {

  private final String DOMAIN_A_NAME = "a";
  private final String OUTPUT_PATH_A = OUTPUT_DIR + "/" + DOMAIN_A_NAME;

  public TestHadoopDomainCompactor() throws IOException {
    super(TestHadoopDomainCompactor.class);
  }

  public void testMain() throws IOException {
    CoordinatorConfigurator configurator = IntStringKeyStorageEngineCoordinator.getConfigurator(2);
    DomainBuilderProperties properties =
        new DomainBuilderProperties(DOMAIN_A_NAME, VersionType.BASE, configurator, OUTPUT_PATH_A);
    new HadoopDomainCompactor().buildHankDomain(properties);
    assertTrue(false);
  }
}
