/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.tools.rumen;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.apache.hadoop.tools.rumen.RandomSeedGenerator.getSeed;

public class TestRandomSeedGenerator {
  @Test
  public void testSeedGeneration() {
    long masterSeed1 = 42;
    long masterSeed2 = 43;
    
    assertTrue(getSeed("stream1", masterSeed1) == getSeed("stream1", masterSeed1),
        "Deterministic seeding");
    assertTrue(getSeed("stream2", masterSeed2) == getSeed("stream2", masterSeed2),
        "Deterministic seeding");
    assertTrue(getSeed("stream1", masterSeed1) != getSeed("stream2", masterSeed1),
        "Different streams");
    assertTrue(getSeed("stream1", masterSeed1) != getSeed("stream1", masterSeed2),
        "Different master seeds");
  }
}
