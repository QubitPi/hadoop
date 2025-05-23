/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.converter;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.converter.FSConfigConverterTestCommons.CONVERSION_RULES_FILE;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.converter.FSConfigConverterTestCommons.FS_ALLOC_FILE;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.converter.FSConfigConverterTestCommons.OUTPUT_DIR;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.converter.FSConfigConverterTestCommons.YARN_SITE_XML;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.converter.FSConfigConverterTestCommons.setupFSConfigConversionFiles;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.function.Consumer;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;


/**
 * Unit tests for TestFSConfigToCSConfigConverterMain.
 *
 */
public class TestFSConfigToCSConfigConverterMain {
  private FSConfigConverterTestCommons converterTestCommons;
  private ExitFunc exitFunc;

  @BeforeEach
  public void setUp() throws Exception {
    exitFunc = new ExitFunc();
    converterTestCommons = new FSConfigConverterTestCommons();
    converterTestCommons.setUp();
    FSConfigToCSConfigConverterMain.setExit(exitFunc);
  }

  @AfterEach
  public void tearDown() throws Exception {
    QueueMetrics.clearQueueMetrics();
    FSConfigToCSConfigConverterMain.setExit(System::exit);
    converterTestCommons.tearDown();
  }

  /*
   * Example command:
   *   /opt/hadoop/bin/yarn fs2cs
   *   -o /tmp/output
   *   -y /opt/hadoop/etc/hadoop/yarn-site.xml
   *   -f /opt/hadoop/etc/hadoop/fair-scheduler.xml
   *   -r /home/systest/sample-rules-config.properties
   */
  @Test
  public void testConvertFSConfigurationDefaultsWeightMode()
      throws Exception {
    testConvertFSConfigurationDefaults(false);
  }

  /*
   * Example command:
   *   /opt/hadoop/bin/yarn fs2cs
   *   -pc
   *   -o /tmp/output
   *   -y /opt/hadoop/etc/hadoop/yarn-site.xml
   *   -f /opt/hadoop/etc/hadoop/fair-scheduler.xml
   *   -r /home/systest/sample-rules-config.properties
   */
  @Test
  public void testConvertFSConfigurationDefaultsPercentageMode()
      throws IOException {
    testConvertFSConfigurationDefaults(true);
  }

  private void testConvertFSConfigurationDefaults(boolean percentage)
      throws IOException {
    setupFSConfigConversionFiles();

    String[] args = new String[] {
        "-o", OUTPUT_DIR,
        "-y", YARN_SITE_XML,
        "-f", FS_ALLOC_FILE,
        "-r", CONVERSION_RULES_FILE};
    if (percentage) {
      args = Arrays.copyOf(args, args.length + 1);
      args[args.length - 1] = "-pc";
    }

    FSConfigToCSConfigConverterMain.main(args);

    boolean csConfigExists =
        new File(OUTPUT_DIR, "capacity-scheduler.xml").exists();
    boolean yarnSiteConfigExists =
        new File(OUTPUT_DIR, "yarn-site.xml").exists();

    assertTrue(csConfigExists, "capacity-scheduler.xml was not generated");
    assertTrue(yarnSiteConfigExists, "yarn-site.xml was not generated");
    assertEquals(0, exitFunc.exitCode, "Exit code");
  }

  @Test
  public void testConvertFSConfigurationWithConsoleParam()
      throws Exception {
    setupFSConfigConversionFiles();

    FSConfigToCSConfigConverterMain.main(new String[] {
        "-p",
        "-e",
        "-y", YARN_SITE_XML,
        "-f", FS_ALLOC_FILE,
        "-r", CONVERSION_RULES_FILE});

    String stdout = converterTestCommons.getStdOutContent().toString();
    assertTrue(stdout.contains("======= yarn-site.xml ======="),
        "Stdout doesn't contain yarn-site.xml");
    assertTrue(stdout.contains("======= capacity-scheduler.xml ======="),
        "Stdout doesn't contain capacity-scheduler.xml");
    assertTrue(stdout.contains("======= mapping-rules.json ======="),
        "Stdout doesn't contain mapping-rules.json");
    assertEquals(0, exitFunc.exitCode, "Exit code");
  }

  @Test
  public void testShortHelpSwitch() {
    FSConfigToCSConfigConverterMain.main(new String[] {"-h"});

    verifyHelpText();
    assertEquals(0, exitFunc.exitCode, "Exit code");
  }

  @Test
  public void testLongHelpSwitch() {
    FSConfigToCSConfigConverterMain.main(new String[] {"--help"});

    verifyHelpText();
    assertEquals(0, exitFunc.exitCode, "Exit code");
  }

  @Test
  public void testHelpDisplayedWithoutArgs() {
    FSConfigToCSConfigConverterMain.main(new String[] {});

    verifyHelpText();
    assertEquals(0, exitFunc.exitCode, "Exit code");
  }

  @Test
  public void testConvertFSConfigurationWithLongSwitches()
      throws IOException {
    setupFSConfigConversionFiles();

    FSConfigToCSConfigConverterMain.main(new String[] {
        "--print",
        "--rules-to-file",
        "--percentage",
        "--yarnsiteconfig", YARN_SITE_XML,
        "--fsconfig", FS_ALLOC_FILE,
        "--rulesconfig", CONVERSION_RULES_FILE});

    String stdout = converterTestCommons.getStdOutContent().toString();
    assertTrue(stdout.contains("======= yarn-site.xml ======="),
        "Stdout doesn't contain yarn-site.xml");
    assertTrue(stdout.contains("======= capacity-scheduler.xml ======="),
        "Stdout doesn't contain capacity-scheduler.xml");
    assertTrue(stdout.contains("======= mapping-rules.json ======="),
        "Stdout doesn't contain mapping-rules.json");
    assertEquals(0, exitFunc.exitCode, "Exit code");
  }

  @Test
  public void testNegativeReturnValueOnError() {
    FSConfigToCSConfigConverterMain.main(new String[] {
        "--print",
        "--yarnsiteconfig"});

    assertEquals(-1, exitFunc.exitCode, "Exit code");
  }

  private void verifyHelpText() {
    String stdout = converterTestCommons.getStdOutContent().toString();
    assertTrue(stdout.contains("General options are:"),
        "Help was not displayed");
  }

  @SuppressWarnings("checkstyle:visibilitymodifier")
  class ExitFunc implements Consumer<Integer> {
    int exitCode;

    @Override
    public void accept(Integer t) {
      this.exitCode = t.intValue();
    }
  }
}
