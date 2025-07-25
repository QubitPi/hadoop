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

package org.apache.hadoop.fs.azurebfs;

import java.io.FileNotFoundException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.junit.Test;
import org.mockito.Mockito;

import org.apache.hadoop.fs.azurebfs.constants.AbfsServiceType;
import org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AbfsRestOperationException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.AzureBlobFileSystemException;
import org.apache.hadoop.fs.azurebfs.contracts.exceptions.InvalidConfigurationValueException;
import org.apache.hadoop.fs.azurebfs.enums.Trilean;
import org.apache.hadoop.fs.azurebfs.services.AbfsClient;
import org.apache.hadoop.fs.azurebfs.services.AbfsRestOperation;
import org.apache.hadoop.fs.azurebfs.utils.TracingContext;

import static java.net.HttpURLConnection.HTTP_UNAVAILABLE;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ACCOUNT_IS_HNS_ENABLED;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_ACCOUNT_KEY_PROPERTY_NAME;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.FS_AZURE_INGRESS_SERVICE_TYPE;
import static org.apache.hadoop.fs.azurebfs.constants.ConfigurationKeys.accountProperty;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemUriSchemes.ABFS_BLOB_DOMAIN_NAME;
import static org.apache.hadoop.fs.azurebfs.constants.FileSystemUriSchemes.ABFS_DFS_DOMAIN_NAME;
import static org.apache.hadoop.fs.azurebfs.services.AbfsErrors.INCORRECT_INGRESS_TYPE;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.mockito.ArgumentMatchers.any;

/**
 * Test filesystem initialization and creation.
 */
public class ITestAzureBlobFileSystemInitAndCreate extends
    AbstractAbfsIntegrationTest {

  public ITestAzureBlobFileSystemInitAndCreate() throws Exception {
    this.getConfiguration().unset(ConfigurationKeys.AZURE_CREATE_REMOTE_FILESYSTEM_DURING_INITIALIZATION);
  }

  @Override
  public void setup() throws Exception {
    super.setup();
  }

  @Override
  public void teardown() {
  }

  @Test (expected = FileNotFoundException.class)
  public void ensureFilesystemWillNotBeCreatedIfCreationConfigIsNotSet() throws Exception {
    final AzureBlobFileSystem fs = this.createFileSystem();
    FileStatus[] fileStatuses = fs.listStatus(new Path("/"));
  }

  @Test
  public void testGetAclCallOnHnsConfigAbsence() throws Exception {
    AzureBlobFileSystem fs = ((AzureBlobFileSystem) FileSystem.newInstance(
        getRawConfiguration()));
    AzureBlobFileSystemStore store = Mockito.spy(fs.getAbfsStore());
    AbfsClient client = Mockito.spy(fs.getAbfsStore().getClient(AbfsServiceType.DFS));
    Mockito.doReturn(client).when(store).getClient(AbfsServiceType.DFS);
    store.getAbfsConfiguration().setIsNamespaceEnabledAccountForTesting(Trilean.UNKNOWN);

    TracingContext tracingContext = getSampleTracingContext(fs, true);
    Mockito.doReturn(Mockito.mock(AbfsRestOperation.class))
        .when(client)
        .getAclStatus(Mockito.anyString(), any(TracingContext.class));
    store.getIsNamespaceEnabled(tracingContext);

    Mockito.verify(client, Mockito.times(1))
        .getAclStatus(Mockito.anyString(), any(TracingContext.class));
  }

  @Test
  public void testNoGetAclCallOnHnsConfigPresence() throws Exception {
    AzureBlobFileSystem fs = ((AzureBlobFileSystem) FileSystem.newInstance(
        getRawConfiguration()));
    AzureBlobFileSystemStore store = Mockito.spy(fs.getAbfsStore());
    AbfsClient client = Mockito.spy(fs.getAbfsClient());
    Mockito.doReturn(client).when(store).getClient();

    Mockito.doReturn(true)
        .when(store)
        .isNamespaceEnabled();

    TracingContext tracingContext = getSampleTracingContext(fs, true);
    store.getIsNamespaceEnabled(tracingContext);

    Mockito.verify(client, Mockito.times(0))
        .getAclStatus(Mockito.anyString(), any(TracingContext.class));
  }

  /**
   * Test to verify that the initialization of the AzureBlobFileSystem fails
   * when an invalid ingress service type is configured.
   *
   * This test sets up a configuration with an invalid ingress service type
   * (DFS) for a Blob endpoint and expects an InvalidConfigurationValueException
   * to be thrown during the initialization of the filesystem.
   *
   * @throws Exception if an error occurs during the test execution
   */
  @Test
  public void testFileSystemInitializationFailsForInvalidIngress() throws Exception {
    assumeHnsDisabled();
    Configuration configuration = new Configuration(getRawConfiguration());
    String defaultUri = configuration.get(FS_DEFAULT_NAME_KEY);
    String accountKey = configuration.get(
        accountProperty(FS_AZURE_ACCOUNT_KEY_PROPERTY_NAME, getAccountName()),
        configuration.get(FS_AZURE_ACCOUNT_KEY_PROPERTY_NAME));
    configuration.set(FS_AZURE_ACCOUNT_KEY_PROPERTY_NAME,
        accountKey.replace(ABFS_DFS_DOMAIN_NAME, ABFS_BLOB_DOMAIN_NAME));
    configuration.set(FS_AZURE_INGRESS_SERVICE_TYPE, AbfsServiceType.DFS.name());
    String blobUri = defaultUri.replace(ABFS_DFS_DOMAIN_NAME, ABFS_BLOB_DOMAIN_NAME);
    intercept(InvalidConfigurationValueException.class,
        INCORRECT_INGRESS_TYPE, () ->
            FileSystem.newInstance(new Path(blobUri).toUri(), configuration));
  }

  @Test
  public void testFileSystemInitFailsIfNotAbleToDetermineAccountType() throws Exception {
    AzureBlobFileSystem fs = ((AzureBlobFileSystem) FileSystem.newInstance(
        getRawConfiguration()));
    AzureBlobFileSystem mockedFs = Mockito.spy(fs);
    Mockito.doThrow(
        new AbfsRestOperationException(HTTP_UNAVAILABLE, "Throttled",
            "Throttled", null)).when(mockedFs).getIsNamespaceEnabled(any());

    intercept(AzureBlobFileSystemException.class,
        FS_AZURE_ACCOUNT_IS_HNS_ENABLED, () ->
            mockedFs.initialize(fs.getUri(), getRawConfiguration()));
  }
}
