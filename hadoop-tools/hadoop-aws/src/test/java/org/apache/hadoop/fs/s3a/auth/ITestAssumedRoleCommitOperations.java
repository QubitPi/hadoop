/*
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

package org.apache.hadoop.fs.s3a.auth;

import java.io.IOException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.commit.ITestCommitOperations;

import static org.apache.hadoop.fs.s3a.Constants.ASSUMED_ROLE_ARN;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.assume;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.disableCreateSession;
import static org.apache.hadoop.fs.s3a.auth.RoleModel.*;
import static org.apache.hadoop.fs.s3a.auth.RolePolicies.*;
import static org.apache.hadoop.fs.s3a.auth.RoleTestUtils.*;
import static org.apache.hadoop.io.IOUtils.cleanupWithLogger;

/**
 * Verify that the commit operations work with a restricted set of operations.
 */
public class ITestAssumedRoleCommitOperations extends ITestCommitOperations {

  private static final Logger LOG =
      LoggerFactory.getLogger(ITestAssumedRoleCommitOperations.class);

  /**
   * The restricted directory.
   */
  private Path restrictedDir;

  /**
   * A role FS; if non-null it is closed in teardown.
   */
  private S3AFileSystem roleFS;

  @Override
  protected Configuration createConfiguration() {
    return disableCreateSession(super.createConfiguration());
  }

  @BeforeEach
  @Override
  public void setup() throws Exception {
    super.setup();
    assumeRoleTests();

    restrictedDir = super.path("restricted");
    Configuration conf = newAssumedRoleConfig(getConfiguration(),
        getAssumedRoleARN());
    bindRolePolicyStatements(conf, STATEMENT_ALLOW_KMS_RW,
        statement(true, S3_ALL_BUCKETS, S3_BUCKET_READ_OPERATIONS),
        new RoleModel.Statement(RoleModel.Effects.Allow)
            .addActions(S3_PATH_RW_OPERATIONS)
            .addResources(directory(restrictedDir))
    );
    roleFS = (S3AFileSystem) restrictedDir.getFileSystem(conf);
  }

  @AfterEach
  @Override
  public void teardown() throws Exception {
    cleanupWithLogger(LOG, roleFS);
    // switches getFileSystem() back to the full FS.
    roleFS = null;
    super.teardown();
  }

  private void assumeRoleTests() {
    assume("No ARN for role tests", !getAssumedRoleARN().isEmpty());
  }

  /**
   * The overridden operation returns the roleFS, so that test cases
   * in the superclass run under restricted rights.
   * There's special handling in startup to avoid NPEs
   * @return {@link #roleFS}
   */
  @Override
  public S3AFileSystem getFileSystem() {
    return roleFS != null ? roleFS : getFullFileSystem();
  }

  /**
   * Get the FS with full access rights.
   * @return the FS created by the superclass.
   */
  public S3AFileSystem getFullFileSystem() {
    return super.getFileSystem();
  }

  /**
   * switch to an restricted path.
   * {@inheritDoc}
   */
  @Override
  protected Path path(String filepath) throws IOException {
    return new Path(restrictedDir, filepath);
  }

  private String getAssumedRoleARN() {
    return getContract().getConf().getTrimmed(ASSUMED_ROLE_ARN, "");
  }

}
