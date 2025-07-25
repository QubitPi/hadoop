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

package org.apache.hadoop.fs.azurebfs.services;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.azurebfs.utils.TextFileBasedIdentityHandler;
import org.junit.jupiter.api.io.TempDir;

import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestTextFileBasedIdentityHandler {

  @TempDir
  public static Path tempDir;

  private static File userMappingFile = null;
  private static File groupMappingFile = null;
  private static final String NEW_LINE = "\n";
  private static String testUserDataLine1 =
      "a2b27aec-77bd-46dd-8c8c-39611a333331:user1:11000:21000:spi-user1:abcf86e9-5a5b-49e2-a253-f5c9e2afd4ec"
          + NEW_LINE;
  private static String testUserDataLine2 =
      "#i2j27aec-77bd-46dd-8c8c-39611a333331:user2:41000:21000:spi-user2:mnof86e9-5a5b-49e2-a253-f5c9e2afd4ec"
          + NEW_LINE;
  private static String testUserDataLine3 =
      "c2d27aec-77bd-46dd-8c8c-39611a333331:user2:21000:21000:spi-user2:deff86e9-5a5b-49e2-a253-f5c9e2afd4ec"
          + NEW_LINE;
  private static String testUserDataLine4 = "e2f27aec-77bd-46dd-8c8c-39611a333331c" + NEW_LINE;
  private static String testUserDataLine5 =
      "g2h27aec-77bd-46dd-8c8c-39611a333331:user4:41000:21000:spi-user4:jklf86e9-5a5b-49e2-a253-f5c9e2afd4ec"
          + NEW_LINE;
  private static String testUserDataLine6 = "          " + NEW_LINE;
  private static String testUserDataLine7 =
      "i2j27aec-77bd-46dd-8c8c-39611a333331:user5:41000:21000:spi-user5:mknf86e9-5a5b-49e2-a253-f5c9e2afd4ec"
          + NEW_LINE;

  private static String testGroupDataLine1 = "1d23024d-957c-4456-aac1-a57f9e2de914:group1:21000:sgp-group1" + NEW_LINE;
  private static String testGroupDataLine2 = "3d43024d-957c-4456-aac1-a57f9e2de914:group2:21000:sgp-group2" + NEW_LINE;
  private static String testGroupDataLine3 = "5d63024d-957c-4456-aac1-a57f9e2de914" + NEW_LINE;
  private static String testGroupDataLine4 = "          " + NEW_LINE;
  private static String testGroupDataLine5 = "7d83024d-957c-4456-aac1-a57f9e2de914:group4:21000:sgp-group4" + NEW_LINE;

  @BeforeAll
  public static void init() throws IOException {
    userMappingFile = tempDir.resolve("user-mapping.conf").toFile();
    groupMappingFile = tempDir.resolve("group-mapping.conf").toFile();

    //Stage data for user mapping
    FileUtils.writeStringToFile(userMappingFile, testUserDataLine1, StandardCharsets.UTF_8, true);
    FileUtils.writeStringToFile(userMappingFile, testUserDataLine2, StandardCharsets.UTF_8, true);
    FileUtils.writeStringToFile(userMappingFile, testUserDataLine3, StandardCharsets.UTF_8, true);
    FileUtils.writeStringToFile(userMappingFile, testUserDataLine4, StandardCharsets.UTF_8, true);
    FileUtils.writeStringToFile(userMappingFile, testUserDataLine5, StandardCharsets.UTF_8, true);
    FileUtils.writeStringToFile(userMappingFile, testUserDataLine6, StandardCharsets.UTF_8, true);
    FileUtils.writeStringToFile(userMappingFile, testUserDataLine7, StandardCharsets.UTF_8, true);
    FileUtils.writeStringToFile(userMappingFile, NEW_LINE, StandardCharsets.UTF_8, true);

    //Stage data for group mapping
    FileUtils.writeStringToFile(groupMappingFile, testGroupDataLine1, StandardCharsets.UTF_8, true);
    FileUtils.writeStringToFile(groupMappingFile, testGroupDataLine2, StandardCharsets.UTF_8, true);
    FileUtils.writeStringToFile(groupMappingFile, testGroupDataLine3, StandardCharsets.UTF_8, true);
    FileUtils.writeStringToFile(groupMappingFile, testGroupDataLine4, StandardCharsets.UTF_8, true);
    FileUtils.writeStringToFile(groupMappingFile, testGroupDataLine5, StandardCharsets.UTF_8, true);
    FileUtils.writeStringToFile(groupMappingFile, NEW_LINE, StandardCharsets.UTF_8, true);
  }

  private void assertUserLookup(TextFileBasedIdentityHandler handler, String userInTest, String expectedUser)
      throws IOException {
    String actualUser = handler.lookupForLocalUserIdentity(userInTest);
    assertEquals(expectedUser, actualUser, "Wrong user identity for ");
  }

  @Test
  public void testLookupForUser() throws IOException {
    TextFileBasedIdentityHandler handler =
        new TextFileBasedIdentityHandler(userMappingFile.getPath(), groupMappingFile.getPath());

    //Success scenario =>  user in test -> user2.
    assertUserLookup(handler, testUserDataLine3.split(":")[0], testUserDataLine3.split(":")[1]);

    //No username found in the mapping file.
    assertUserLookup(handler, "bogusIdentity", "");

    //Edge case when username is empty string.
    assertUserLookup(handler, "", "");
  }

  @Test
  public void testLookupForUserFileNotFound() throws Exception {
    TextFileBasedIdentityHandler handler =
        new TextFileBasedIdentityHandler(userMappingFile.getPath() + ".test", groupMappingFile.getPath());
    intercept(NoSuchFileException.class, "NoSuchFileException",
        () -> handler.lookupForLocalUserIdentity(testUserDataLine3.split(":")[0]));
  }

  private void assertGroupLookup(TextFileBasedIdentityHandler handler, String groupInTest, String expectedGroup)
      throws IOException {
    String actualGroup = handler.lookupForLocalGroupIdentity(groupInTest);
    assertEquals(expectedGroup, actualGroup, "Wrong group identity for ");
  }

  @Test
  public void testLookupForGroup() throws IOException {
    TextFileBasedIdentityHandler handler =
        new TextFileBasedIdentityHandler(userMappingFile.getPath(), groupMappingFile.getPath());

    //Success scenario.
    assertGroupLookup(handler, testGroupDataLine2.split(":")[0], testGroupDataLine2.split(":")[1]);

    //No group name found in the mapping file.
    assertGroupLookup(handler, "bogusIdentity", "");

    //Edge case when group name is empty string.
    assertGroupLookup(handler, "", "");
  }

  @Test
  public void testLookupForGroupFileNotFound() throws Exception {
    TextFileBasedIdentityHandler handler =
        new TextFileBasedIdentityHandler(userMappingFile.getPath(), groupMappingFile.getPath() + ".test");
    intercept(NoSuchFileException.class, "NoSuchFileException",
        () -> handler.lookupForLocalGroupIdentity(testGroupDataLine2.split(":")[0]));
  }
}
