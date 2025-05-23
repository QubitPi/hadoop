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
package org.apache.hadoop.yarn.sls;

import org.apache.commons.math3.random.JDKRandomGenerator;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.sls.synthetic.SynthJob;
import org.apache.hadoop.yarn.sls.synthetic.SynthTraceJobProducer;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonFactoryBuilder;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;

import static com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Simple test class driving the {@code SynthTraceJobProducer}, and validating
 * jobs produce are within expected range.
 */
public class TestSynthJobGeneration {

  public final static Logger LOG =
      LoggerFactory.getLogger(TestSynthJobGeneration.class);

  @Test
  public void testWorkloadGenerateTime()
      throws IllegalArgumentException, IOException {

    String workloadJson = "{\"job_classes\": [], \"time_distribution\":["
        + "{\"time\": 0, \"weight\": 1}, " + "{\"time\": 30, \"weight\": 0},"
        + "{\"time\": 60, \"weight\": 2}," + "{\"time\": 90, \"weight\": 1}"
        + "]}";

    JsonFactoryBuilder jsonFactoryBuilder = new JsonFactoryBuilder();
    jsonFactoryBuilder.configure(JsonFactory.Feature.INTERN_FIELD_NAMES, true);
    ObjectMapper mapper = new ObjectMapper(jsonFactoryBuilder.build());
    mapper.configure(FAIL_ON_UNKNOWN_PROPERTIES, false);
    SynthTraceJobProducer.Workload wl =
        mapper.readValue(workloadJson, SynthTraceJobProducer.Workload.class);

    JDKRandomGenerator rand = new JDKRandomGenerator();
    rand.setSeed(0);

    wl.init(rand);

    int bucket0 = 0;
    int bucket1 = 0;
    int bucket2 = 0;
    int bucket3 = 0;
    for (int i = 0; i < 1000; ++i) {
      long time = wl.generateSubmissionTime();
      LOG.info("Generated time " + time);
      if (time < 30) {
        bucket0++;
      } else if (time < 60) {
        bucket1++;
      } else if (time < 90) {
        bucket2++;
      } else {
        bucket3++;
      }
    }

    Assertions.assertTrue(bucket0 > 0);
    assertEquals(0, bucket1);
    Assertions.assertTrue(bucket2 > 0);
    Assertions.assertTrue(bucket3 > 0);
    Assertions.assertTrue(bucket2 > bucket0);
    Assertions.assertTrue(bucket2 > bucket3);

    LOG.info("bucket0 {}, bucket1 {}, bucket2 {}, bucket3 {}", bucket0, bucket1,
        bucket2, bucket3);

  }

  @Test
  public void testMapReduce() throws IllegalArgumentException, IOException {

    Configuration conf = new Configuration();

    conf.set(SynthTraceJobProducer.SLS_SYNTHETIC_TRACE_FILE,
        "src/test/resources/syn.json");

    SynthTraceJobProducer stjp = new SynthTraceJobProducer(conf);

    LOG.info(stjp.toString());

    SynthJob js = (SynthJob) stjp.getNextJob();

    int jobCount = 0;

    while (js != null) {
      LOG.info(js.toString());
      validateJob(js);
      js = (SynthJob) stjp.getNextJob();
      jobCount++;
    }

    Assertions.assertEquals(stjp.getNumJobs(), jobCount);
  }

  @Test
  public void testGeneric() throws IllegalArgumentException, IOException {
    Configuration conf = new Configuration();

    conf.set(SynthTraceJobProducer.SLS_SYNTHETIC_TRACE_FILE,
        "src/test/resources/syn_generic.json");

    SynthTraceJobProducer stjp = new SynthTraceJobProducer(conf);

    LOG.info(stjp.toString());

    SynthJob js = (SynthJob) stjp.getNextJob();

    int jobCount = 0;

    while (js != null) {
      LOG.info(js.toString());
      validateJob(js);
      js = (SynthJob) stjp.getNextJob();
      jobCount++;
    }

    Assertions.assertEquals(stjp.getNumJobs(), jobCount);
  }

  @Test
  public void testStream() throws IllegalArgumentException, IOException {
    Configuration conf = new Configuration();

    conf.set(SynthTraceJobProducer.SLS_SYNTHETIC_TRACE_FILE,
        "src/test/resources/syn_stream.json");

    SynthTraceJobProducer stjp = new SynthTraceJobProducer(conf);

    LOG.info(stjp.toString());

    SynthJob js = (SynthJob) stjp.getNextJob();

    int jobCount = 0;

    while (js != null) {
      LOG.info(js.toString());
      validateJob(js);
      js = (SynthJob) stjp.getNextJob();
      jobCount++;
    }

    Assertions.assertEquals(stjp.getNumJobs(), jobCount);
  }

  @Test
  public void testSample() throws IOException {

    JsonFactoryBuilder jsonFactoryBuilder = new JsonFactoryBuilder();
    jsonFactoryBuilder.configure(JsonFactory.Feature.INTERN_FIELD_NAMES, true);
    ObjectMapper mapper = new ObjectMapper(jsonFactoryBuilder.build());
    mapper.configure(FAIL_ON_UNKNOWN_PROPERTIES, false);

    JDKRandomGenerator rand = new JDKRandomGenerator();
    rand.setSeed(0);

    String valJson = "{\"val\" : 5 }";
    SynthTraceJobProducer.Sample valSample =
        mapper.readValue(valJson, SynthTraceJobProducer.Sample.class);
    valSample.init(rand);
    int val = valSample.getInt();
    Assertions.assertEquals(5, val);

    String distJson = "{\"val\" : 5, \"std\" : 1 }";
    SynthTraceJobProducer.Sample distSample =
        mapper.readValue(distJson, SynthTraceJobProducer.Sample.class);
    distSample.init(rand);
    double dist = distSample.getDouble();
    Assertions.assertTrue(dist > 2 && dist < 8);

    String normdistJson = "{\"val\" : 5, \"std\" : 1, \"dist\": \"NORM\" }";
    SynthTraceJobProducer.Sample normdistSample =
        mapper.readValue(normdistJson, SynthTraceJobProducer.Sample.class);
    normdistSample.init(rand);
    double normdist = normdistSample.getDouble();
    Assertions.assertTrue(normdist > 2 && normdist < 8);

    String discreteJson = "{\"discrete\" : [2, 4, 6, 8]}";
    SynthTraceJobProducer.Sample discreteSample =
        mapper.readValue(discreteJson, SynthTraceJobProducer.Sample.class);
    discreteSample.init(rand);
    int discrete = discreteSample.getInt();
    Assertions.assertTrue(
        Arrays.asList(new Integer[] {2, 4, 6, 8}).contains(discrete));

    String discreteWeightsJson =
        "{\"discrete\" : [2, 4, 6, 8], " + "\"weights\": [0, 0, 0, 1]}";
    SynthTraceJobProducer.Sample discreteWeightsSample = mapper
        .readValue(discreteWeightsJson, SynthTraceJobProducer.Sample.class);
    discreteWeightsSample.init(rand);
    int discreteWeights = discreteWeightsSample.getInt();
    Assertions.assertEquals(8, discreteWeights);

    String invalidJson = "{\"val\" : 5, \"discrete\" : [2, 4, 6, 8], "
        + "\"weights\": [0, 0, 0, 1]}";
    try {
      mapper.readValue(invalidJson, SynthTraceJobProducer.Sample.class);
      Assertions.fail();
    } catch (JsonMappingException e) {
      Assertions.assertTrue(e.getMessage().startsWith("Instantiation of"));
    }

    String invalidDistJson =
        "{\"val\" : 5, \"std\" : 1, " + "\"dist\": \"INVALID\" }";
    try {
      mapper.readValue(invalidDistJson, SynthTraceJobProducer.Sample.class);
      Assertions.fail();
    } catch (JsonMappingException e) {
      Assertions.assertTrue(e.getMessage().startsWith("Cannot construct instance of"));
    }
  }

  private void validateJob(SynthJob js) {

    assertTrue(js.getSubmissionTime() > 0);
    assertTrue(js.getDuration() > 0);
    assertTrue(js.getTotalSlotTime() >= 0);

    if (js.hasDeadline()) {
      assertTrue(js.getDeadline() > js.getSubmissionTime() + js.getDuration());
    }

    assertTrue(js.getTasks().size() > 0);

    for (SynthJob.SynthTask t : js.getTasks()) {
      assertNotNull(t.getType());
      assertTrue(t.getTime() > 0);
      assertTrue(t.getMemory() > 0);
      assertTrue(t.getVcores() > 0);
      assertEquals(ExecutionType.GUARANTEED, t.getExecutionType());
    }
  }
}
