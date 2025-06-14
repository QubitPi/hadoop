/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.s3a.auth.delegation;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.AccessDeniedException;

import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadBucketResponse;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.s3a.AWSCredentialProviderList;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.hadoop.fs.s3a.DefaultS3ClientFactory;
import org.apache.hadoop.fs.s3a.Invoker;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.S3ATestUtils;
import org.apache.hadoop.fs.s3a.S3ClientFactory;
import org.apache.hadoop.fs.s3a.Statistic;
import org.apache.hadoop.fs.s3a.statistics.impl.EmptyS3AStatisticsContext;
import org.apache.hadoop.hdfs.tools.DelegationTokenFetcher;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.DtUtilShell;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.service.ServiceOperations;
import org.apache.hadoop.service.ServiceStateException;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION;
import static org.apache.hadoop.fs.s3a.Constants.*;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.assumeSessionTestsEnabled;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.disableCreateSession;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.disableFilesystemCaching;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.getTestBucketName;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.isS3ExpressTestBucket;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.removeBaseAndBucketOverrides;
import static org.apache.hadoop.fs.s3a.S3ATestUtils.unsetHadoopCredentialProviders;
import static org.apache.hadoop.fs.s3a.S3AUtils.getEncryptionAlgorithm;
import static org.apache.hadoop.fs.s3a.S3AUtils.getS3EncryptionKey;
import static org.apache.hadoop.fs.s3a.auth.delegation.DelegationConstants.*;
import static org.apache.hadoop.fs.s3a.auth.delegation.DelegationTokenIOException.TOKEN_MISMATCH;
import static org.apache.hadoop.fs.s3a.auth.delegation.MiniKerberizedHadoopCluster.ALICE;
import static org.apache.hadoop.fs.s3a.auth.delegation.MiniKerberizedHadoopCluster.assertSecurityEnabled;
import static org.apache.hadoop.fs.s3a.auth.delegation.S3ADelegationTokens.lookupS3ADelegationToken;
import static org.apache.hadoop.fs.s3a.test.PublicDatasetTestUtils.requireAnonymousDataPath;
import static org.apache.hadoop.test.LambdaTestUtils.doAs;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests use of Hadoop delegation tokens within the FS itself.
 * This instantiates a MiniKDC as some of the operations tested require
 * UGI to be initialized with security enabled.
 */
@SuppressWarnings("StaticNonFinalField")
public class ITestSessionDelegationInFilesystem extends AbstractDelegationIT {

  private static final Logger LOG =
      LoggerFactory.getLogger(ITestSessionDelegationInFilesystem.class);

  private static MiniKerberizedHadoopCluster cluster;

  private UserGroupInformation bobUser;

  private UserGroupInformation aliceUser;

  private S3ADelegationTokens delegationTokens;

  /***
   * Set up a mini Cluster with two users in the keytab.
   */
  @BeforeAll
  public static void setupCluster() throws Exception {
    cluster = new MiniKerberizedHadoopCluster();
    cluster.init(new Configuration());
    cluster.start();
  }

  /**
   * Tear down the Cluster.
   */
  @SuppressWarnings("ThrowableNotThrown")
  @AfterAll
  public static void teardownCluster() throws Exception {
    ServiceOperations.stopQuietly(LOG, cluster);
  }

  protected static MiniKerberizedHadoopCluster getCluster() {
    return cluster;
  }

  /**
   * Get the delegation token binding for this test suite.
   * @return which DT binding to use.
   */
  protected String getDelegationBinding() {
    return DELEGATION_TOKEN_SESSION_BINDING;
  }

  /**
   * Get the kind of the tokens which are generated.
   * @return the kind of DT
   */
  public Text getTokenKind() {
    return SESSION_TOKEN_KIND;
  }

  @SuppressWarnings("deprecation")
  @Override
  protected Configuration createConfiguration() {
    Configuration conf = super.createConfiguration();
    // disable if assume role opts are off
    assumeSessionTestsEnabled(conf);
    disableFilesystemCaching(conf);
    final String bucket = getTestBucketName(conf);
    final boolean isS3Express = isS3ExpressTestBucket(conf);

    removeBaseAndBucketOverrides(conf,
        DELEGATION_TOKEN_BINDING,
        Constants.S3_ENCRYPTION_ALGORITHM,
        Constants.S3_ENCRYPTION_KEY,
        SERVER_SIDE_ENCRYPTION_ALGORITHM,
        SERVER_SIDE_ENCRYPTION_KEY,
        S3EXPRESS_CREATE_SESSION);
    conf.set(HADOOP_SECURITY_AUTHENTICATION,
        UserGroupInformation.AuthenticationMethod.KERBEROS.name());
    enableDelegationTokens(conf, getDelegationBinding());
    conf.set(AWS_CREDENTIALS_PROVIDER, " ");
    // switch to CSE-KMS(if specified) else SSE-KMS.
    if (!isS3Express && conf.getBoolean(KEY_ENCRYPTION_TESTS, true)) {
      String s3EncryptionMethod;
      try {
        s3EncryptionMethod =
            getEncryptionAlgorithm(bucket, conf).getMethod();
      } catch (IOException e) {
        throw new UncheckedIOException("Failed to lookup encryption algorithm.",
            e);
      }
      String s3EncryptionKey = getS3EncryptionKey(bucket, conf);

      conf.set(Constants.S3_ENCRYPTION_ALGORITHM, s3EncryptionMethod);
      // KMS key ID a must if CSE-KMS is being tested.
      conf.set(Constants.S3_ENCRYPTION_KEY, s3EncryptionKey);
    }
    // set the YARN RM up for YARN tests.
    conf.set(YarnConfiguration.RM_PRINCIPAL, YARN_RM);

    if (conf.getBoolean(KEY_ACL_TESTS_ENABLED, false) && !isS3Express) {
      // turn on ACLs so as to verify role DT permissions include
      // write access.
      conf.set(CANNED_ACL, LOG_DELIVERY_WRITE);
    }
    // disable create session so there's no need to
    // add a role policy for it.
    disableCreateSession(conf);
    return conf;
  }

  @BeforeEach
  @Override
  public void setup() throws Exception {
    // clear any existing tokens from the FS
    resetUGI();
    UserGroupInformation.setConfiguration(createConfiguration());

    aliceUser = cluster.createAliceUser();
    bobUser = cluster.createBobUser();

    UserGroupInformation.setLoginUser(aliceUser);
    assertSecurityEnabled();
    // only now do the setup, so that any FS created is secure
    super.setup();
    S3AFileSystem fs = getFileSystem();
    // make sure there aren't any tokens
    assertNull(lookupS3ADelegationToken(
        UserGroupInformation.getCurrentUser().getCredentials(),
        fs.getUri()), "Unexpectedly found an S3A token");

    // DTs are inited but not started.
    delegationTokens = instantiateDTSupport(getConfiguration());
  }

  @AfterEach
  @SuppressWarnings("ThrowableNotThrown")
  @Override
  public void teardown() throws Exception {
    super.teardown();
    ServiceOperations.stopQuietly(LOG, delegationTokens);
    FileSystem.closeAllForUGI(UserGroupInformation.getCurrentUser());
    MiniKerberizedHadoopCluster.closeUserFileSystems(aliceUser);
    MiniKerberizedHadoopCluster.closeUserFileSystems(bobUser);
    cluster.resetUGI();
  }

  /**
   * Are encryption tests enabled?
   * @return true if encryption is turned on.
   */
  protected boolean encryptionTestEnabled() {
    return getConfiguration().getBoolean(KEY_ENCRYPTION_TESTS, true);
  }

  @Test
  public void testGetDTfromFileSystem() throws Throwable {
    describe("Enable delegation tokens and request one");
    delegationTokens.start();
    S3AFileSystem fs = getFileSystem();
    assertNotNull(fs.getCanonicalServiceName(),
        "No tokens from " + fs);
    S3ATestUtils.MetricDiff invocationDiff = new S3ATestUtils.MetricDiff(fs,
        Statistic.INVOCATION_GET_DELEGATION_TOKEN);
    S3ATestUtils.MetricDiff issueDiff = new S3ATestUtils.MetricDiff(fs,
        Statistic.DELEGATION_TOKENS_ISSUED);
    Token<AbstractS3ATokenIdentifier> token =
        requireNonNull(fs.getDelegationToken(""),
            "no token from filesystem " + fs);
    assertEquals(getTokenKind(), token.getKind(), "token kind");
    assertTokenCreationCount(fs, 1);
    final String fsInfo = fs.toString();
    invocationDiff.assertDiffEquals("getDelegationToken() in " + fsInfo,
        1);
    issueDiff.assertDiffEquals("DTs issued in " + delegationTokens,
        1);

    Text service = delegationTokens.getService();
    assertEquals(service, token.getService(), "service name");
    Credentials creds = new Credentials();
    creds.addToken(service, token);
    assertEquals(token, creds.getToken(service),
        "retrieve token from " + creds);
  }

  @Test
  public void testAddTokensFromFileSystem() throws Throwable {
    describe("verify FileSystem.addDelegationTokens() collects tokens");
    S3AFileSystem fs = getFileSystem();
    Credentials cred = new Credentials();
    Token<?>[] tokens = fs.addDelegationTokens(YARN_RM, cred);
    assertEquals(1, tokens.length, "Number of tokens");
    Token<?> token = requireNonNull(tokens[0], "token");
    LOG.info("FS token is {}", token);
    Text service = delegationTokens.getService();
    Token<? extends TokenIdentifier> retrieved = requireNonNull(
        cred.getToken(service),
        "retrieved token with key " + service + "; expected " + token);
    delegationTokens.start();
    // this only sneaks in because there isn't a state check here
    delegationTokens.resetTokenBindingToDT(
        (Token<AbstractS3ATokenIdentifier>) retrieved);
    assertTrue(delegationTokens.isBoundToDT(), "bind to existing DT failed");
    AWSCredentialProviderList providerList = requireNonNull(
        delegationTokens.getCredentialProviders(), "providers");

    providerList.resolveCredentials();
  }

  @Test
  public void testCanRetrieveTokenFromCurrentUserCreds() throws Throwable {
    describe("Create a DT, add it to the current UGI credentials,"
        + " then retrieve");
    delegationTokens.start();
    Credentials cred = createDelegationTokens();
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    ugi.addCredentials(cred);
    Token<?>[] tokens = cred.getAllTokens().toArray(new Token<?>[0]);
    Token<?> token0 = tokens[0];
    Text service = token0.getService();
    LOG.info("Token = " + token0);
    Token<?> token1 = requireNonNull(
        ugi.getCredentials().getToken(service), "Token from " + service);
    assertEquals(token0, token1, "retrieved token");
    assertNotNull(token1.getIdentifier(),
        "token identifier of "  + token1);
  }

  @Test
  public void testDTCredentialProviderFromCurrentUserCreds() throws Throwable {
    describe("Add credentials to the current user, "
        + "then verify that they can be found when S3ADelegationTokens binds");
    Credentials cred = createDelegationTokens();
    assertThat(cred.getAllTokens()).hasSize(1).
        as("Token size");
    UserGroupInformation.getCurrentUser().addCredentials(cred);
    delegationTokens.start();
    assertTrue(delegationTokens.isBoundToDT(),
        "bind to existing DT failed");
  }

  /**
   * Create credentials with the DTs of the current FS.
   * @return a non-empty set of credentials.
   * @throws IOException failure to create.
   */
  protected Credentials createDelegationTokens() throws IOException {
    return mkTokens(getFileSystem());
  }

  /**
   * Create a FS with a delegated token, verify it works as a filesystem,
   * and that you can pick up the same DT from that FS too.
   */
  @Test
  public void testDelegatedFileSystem() throws Throwable {
    describe("Delegation tokens can be passed to a new filesystem;"
        + " if role restricted, permissions are tightened.");
    S3AFileSystem fs = getFileSystem();
    // force a probe of the remote FS to make sure its endpoint is valid
    // TODO: Check what should happen here. Calling headObject() on the root path fails in V2,
    // with the error that key cannot be empty.
   // fs.getObjectMetadata(new Path("/"));
    readExternalDatasetMetadata(fs);

    URI uri = fs.getUri();
    // create delegation tokens from the test suites FS.
    Credentials creds = createDelegationTokens();
    final Text tokenKind = getTokenKind();
    AbstractS3ATokenIdentifier origTokenId = requireNonNull(
        lookupToken(
            creds,
            uri,
            tokenKind), "original");
    // attach to the user, so that when tokens are looked for, they get picked
    // up
    final UserGroupInformation currentUser
        = UserGroupInformation.getCurrentUser();
    currentUser.addCredentials(creds);
    // verify that the tokens went over
    requireNonNull(lookupToken(
            currentUser.getCredentials(),
            uri,
            tokenKind), "user credentials");
    Configuration conf = new Configuration(getConfiguration());
    String bucket = fs.getBucket();
    disableFilesystemCaching(conf);
    unsetHadoopCredentialProviders(conf);
    // remove any secrets we don't want the delegated FS to accidentally
    // pick up.
    // this is to simulate better a remote deployment.
    removeBaseAndBucketOverrides(bucket, conf,
        ACCESS_KEY, SECRET_KEY, SESSION_TOKEN,
        Constants.S3_ENCRYPTION_ALGORITHM,
        Constants.S3_ENCRYPTION_KEY,
        SERVER_SIDE_ENCRYPTION_ALGORITHM,
        SERVER_SIDE_ENCRYPTION_KEY,
        DELEGATION_TOKEN_ROLE_ARN,
        DELEGATION_TOKEN_ENDPOINT);
    // this is done to make sure you cannot create an STS session no
    // matter how you pick up credentials.
    conf.set(DELEGATION_TOKEN_ENDPOINT, "http://localhost:8080/");
    bindProviderList(bucket, conf, CountInvocationsProvider.NAME);
    long originalCount = CountInvocationsProvider.getInvocationCount();

    // create a new FS instance, which is expected to pick up the
    // existing token
    Path testPath = path("testDTFileSystemClient");
    try (S3AFileSystem delegatedFS = newS3AInstance(uri, conf)) {
      LOG.info("Delegated filesystem is: {}", delegatedFS);
      assertBoundToDT(delegatedFS, tokenKind);
      if (encryptionTestEnabled()) {
        assertNotNull(delegatedFS.getS3EncryptionAlgorithm(),
            "Encryption propagation failed");
        assertEquals(fs.getS3EncryptionAlgorithm(),
            delegatedFS.getS3EncryptionAlgorithm(),
            "Encryption propagation failed");
      }
      verifyRestrictedPermissions(delegatedFS);

      executeDelegatedFSOperations(delegatedFS, testPath);
      delegatedFS.mkdirs(testPath);

      S3ATestUtils.MetricDiff issueDiff = new S3ATestUtils.MetricDiff(
          delegatedFS,
          Statistic.DELEGATION_TOKENS_ISSUED);

      // verify that the FS returns the existing token when asked
      // so that chained deployments will work
      AbstractS3ATokenIdentifier tokenFromDelegatedFS
          = requireNonNull(delegatedFS.getDelegationToken(""),
          "New token").decodeIdentifier();
      assertEquals(origTokenId,
          tokenFromDelegatedFS, "Newly issued token != old one");
      issueDiff.assertDiffEquals("DTs issued in " + delegatedFS,
          0);
    }
    // the DT auth chain should override the original one.
    assertEquals(originalCount,
        CountInvocationsProvider.getInvocationCount(), "invocation count");

    // create a second instance, which will pick up the same value
    try (S3AFileSystem secondDelegate = newS3AInstance(uri, conf)) {
      assertBoundToDT(secondDelegate, tokenKind);
      if (encryptionTestEnabled()) {
        assertNotNull(
           secondDelegate.getS3EncryptionAlgorithm(), "Encryption propagation failed");
        assertEquals(fs.getS3EncryptionAlgorithm(),
            secondDelegate.getS3EncryptionAlgorithm(),
            "Encryption propagation failed");
      }
      ContractTestUtils.assertDeleted(secondDelegate, testPath, true);
      assertNotNull(secondDelegate.getDelegationToken(""), "unbounded DT");
    }
  }

  /**
   * Override/extension point: run operations within a delegated FS.
   * @param delegatedFS filesystem.
   * @param testPath path to work on.
   * @throws IOException failures
   */
  protected void executeDelegatedFSOperations(final S3AFileSystem delegatedFS,
      final Path testPath) throws Exception {
    ContractTestUtils.assertIsDirectory(delegatedFS, new Path("/"));
    ContractTestUtils.touch(delegatedFS, testPath);
    ContractTestUtils.assertDeleted(delegatedFS, testPath, false);
    delegatedFS.mkdirs(testPath);
    ContractTestUtils.assertIsDirectory(delegatedFS, testPath);
    Path srcFile = new Path(testPath, "src.txt");
    Path destFile = new Path(testPath, "dest.txt");
    ContractTestUtils.touch(delegatedFS, srcFile);
    ContractTestUtils.rename(delegatedFS, srcFile, destFile);
    // this file is deleted afterwards, so leave alone
    ContractTestUtils.assertIsFile(delegatedFS, destFile);
    ContractTestUtils.assertDeleted(delegatedFS, testPath, true);
  }

  /**
   * Session tokens can read the external bucket without problems.
   * @param delegatedFS delegated FS
   * @throws Exception failure
   */
  protected void verifyRestrictedPermissions(final S3AFileSystem delegatedFS)
      throws Exception {
    readExternalDatasetMetadata(delegatedFS);
  }

  @Test
  public void testDelegationBindingMismatch1() throws Throwable {
    describe("Verify that when the DT client and remote bindings are different,"
        + " the failure is meaningful");
    S3AFileSystem fs = getFileSystem();
    URI uri = fs.getUri();
    UserGroupInformation.getCurrentUser().addCredentials(
        createDelegationTokens());

    // create the remote FS with a full credential binding
    Configuration conf = new Configuration(getConfiguration());
    String bucket = fs.getBucket();
    removeBaseAndBucketOverrides(bucket, conf,
        ACCESS_KEY, SECRET_KEY, SESSION_TOKEN);
    conf.set(ACCESS_KEY, "aaaaa");
    conf.set(SECRET_KEY, "bbbb");
    bindProviderList(bucket, conf, CountInvocationsProvider.NAME);
    conf.set(DELEGATION_TOKEN_BINDING,
        DELEGATION_TOKEN_FULL_CREDENTIALS_BINDING);
    ServiceStateException e = intercept(
        ServiceStateException.class,
        TOKEN_MISMATCH,
        () -> {
          S3AFileSystem remote = newS3AInstance(uri, conf);
          // if we get this far, provide info for the exception which will
          // be raised.
          String s = remote.toString();
          remote.close();
          return s;
        });
    if (!(e.getCause() instanceof DelegationTokenIOException)) {
      throw e;
    }
  }

  @Test
  public void testDelegationBindingMismatch2() throws Throwable {
    describe("assert mismatch reported when client DT is a "
        + "subclass of the remote one");
    S3AFileSystem fs = getFileSystem();
    URI uri = fs.getUri();

    // create the remote FS with a full credential binding
    Configuration conf = new Configuration(getConfiguration());
    String bucket = fs.getBucket();
    enableDelegationTokens(conf, DELEGATION_TOKEN_FULL_CREDENTIALS_BINDING);

    // create a new FS with Full tokens
    Credentials fullTokens;
    Token<AbstractS3ATokenIdentifier> firstDT;
    try (S3AFileSystem fullFS = newS3AInstance(uri, conf)) {
      // add the tokens to the user group
      fullTokens = mkTokens(fullFS);
      assertTokenCreationCount(fullFS, 1);
      firstDT = fullFS.getDelegationToken(
          "first");
      assertTokenCreationCount(fullFS, 2);
      Token<AbstractS3ATokenIdentifier> secondDT = fullFS.getDelegationToken(
          "second");
      assertTokenCreationCount(fullFS, 3);
      assertNotEquals(firstDT.getIdentifier(), secondDT.getIdentifier(),
          "DT identifiers");
    }

    // expect a token
    AbstractS3ATokenIdentifier origTokenId = requireNonNull(
        lookupToken(
            fullTokens,
            uri,
            FULL_TOKEN_KIND), "token from credentials");
    UserGroupInformation.getCurrentUser().addCredentials(
        fullTokens);

    // a remote FS with those tokens
    try (S3AFileSystem delegatedFS = newS3AInstance(uri, conf)) {
      assertBoundToDT(delegatedFS, FULL_TOKEN_KIND);
      delegatedFS.getFileStatus(new Path("/"));
      SessionTokenIdentifier tokenFromDelegatedFS
          = (SessionTokenIdentifier) requireNonNull(
              delegatedFS.getDelegationToken(""), "New token")
          .decodeIdentifier();
      assertTokenCreationCount(delegatedFS, 0);
      assertEquals(origTokenId,
          tokenFromDelegatedFS, "Newly issued token != old one");
    }

    // now create a configuration which expects a session token.
    Configuration conf2 = new Configuration(getConfiguration());
    removeBaseAndBucketOverrides(bucket, conf2,
        ACCESS_KEY, SECRET_KEY, SESSION_TOKEN);
    conf.set(DELEGATION_TOKEN_BINDING,
        getDelegationBinding());
    ServiceStateException e = intercept(ServiceStateException.class,
        TOKEN_MISMATCH,
        () -> {
          S3AFileSystem remote = newS3AInstance(uri, conf);
          // if we get this far, provide info for the exception which will
          // be raised.
          String s = remote.toString();
          remote.close();
          return s;
        });
    if (!(e.getCause() instanceof DelegationTokenIOException)) {
      throw e;
    }
  }

  /**
   * This verifies that the granted credentials only access the target bucket
   * by using the credentials in a new S3 client to query the external
   * bucket.
   * @param delegatedFS delegated FS with role-restricted access.
   * @throws AccessDeniedException if the delegated FS's credentials can't
   * access the bucket.
   * @return result of the HEAD
   * @throws Exception failure
   */
  protected HeadBucketResponse readExternalDatasetMetadata(final S3AFileSystem delegatedFS)
      throws Exception {
    AWSCredentialProviderList testingCreds
        = delegatedFS.getS3AInternals().shareCredentials("testing");

    URI external = requireAnonymousDataPath(getConfiguration()).toUri();
    DefaultS3ClientFactory factory
        = new DefaultS3ClientFactory();
    Configuration conf = delegatedFS.getConf();
    factory.setConf(conf);
    String host = external.getHost();
    S3ClientFactory.S3ClientCreationParameters parameters = null;
    parameters = new S3ClientFactory.S3ClientCreationParameters()
        .withCredentialSet(testingCreds)
        .withPathUri(new URI("s3a://localhost/"))
        .withMetrics(new EmptyS3AStatisticsContext()
            .newStatisticsFromAwsSdk())
        .withUserAgentSuffix("ITestSessionDelegationInFilesystem");

    S3Client s3 = factory.createS3Client(external, parameters);

    return Invoker.once("HEAD", host,
        () -> s3.headBucket(b -> b.bucket(host)));
  }

  /**
   * YARN job submission uses
   * {@link TokenCache#obtainTokensForNamenodes(Credentials, Path[], Configuration)}
   * for token retrieval: call it here to verify it works.
   */
  @Test
  public void testYarnCredentialPickup() throws Throwable {
    describe("Verify tokens are picked up by the YARN"
        + " TokenCache.obtainTokensForNamenodes() API Call");
    Credentials cred = new Credentials();
    Path yarnPath = path("testYarnCredentialPickup");
    Path[] paths = new Path[] {yarnPath};
    Configuration conf = getConfiguration();
    S3AFileSystem fs = getFileSystem();
    TokenCache.obtainTokensForNamenodes(cred, paths, conf);
    assertNotNull(lookupToken(cred, fs.getUri(), getTokenKind()),
        "No Token in credentials file");
  }

  /**
   * Test the {@code hdfs fetchdt} command works with S3A tokens.
   */
  @Test
  public void testHDFSFetchDTCommand() throws Throwable {
    describe("Use the HDFS fetchdt CLI to fetch a token");

    ExitUtil.disableSystemExit();
    S3AFileSystem fs = getFileSystem();
    Configuration conf = fs.getConf();

    URI fsUri = fs.getUri();
    String fsurl = fsUri.toString();
    File tokenfile = createTempTokenFile();

    // this will create (& leak) a new FS instance as caching is disabled.
    // but as teardown destroys all filesystems for this user, it
    // gets cleaned up at the end of the test
    String tokenFilePath = tokenfile.getAbsolutePath();


    // create the tokens as Bob.
    doAs(bobUser,
        () -> DelegationTokenFetcher.main(conf,
            args("--webservice", fsurl, tokenFilePath)));
    assertTrue(tokenfile.exists(),
        "token file was not created: " + tokenfile);

    // print to stdout
    String s = DelegationTokenFetcher.printTokensToString(conf,
        new Path(tokenfile.toURI()),
        false);
    LOG.info("Tokens: {}", s);
    DelegationTokenFetcher.main(conf,
        args("--print", tokenFilePath));
    DelegationTokenFetcher.main(conf,
        args("--print", "--verbose", tokenFilePath));

    // read in and retrieve token
    Credentials creds = Credentials.readTokenStorageFile(tokenfile, conf);
    AbstractS3ATokenIdentifier identifier = requireNonNull(
        lookupToken(
            creds,
            fsUri,
            getTokenKind()), "Token lookup");
    assertEquals(fs.getEncryptionSecrets(),
        identifier.getEncryptionSecrets(), "encryption secrets");
    assertEquals(bobUser.getUserName(), identifier.getUser().getUserName(),
        "Username of decoded token");

    // renew
    DelegationTokenFetcher.main(conf, args("--renew", tokenFilePath));

    // cancel
    DelegationTokenFetcher.main(conf, args("--cancel", tokenFilePath));
  }

  protected File createTempTokenFile() throws IOException {
    File tokenfile = File.createTempFile("tokens", ".bin",
        cluster.getWorkDir());
    tokenfile.delete();
    return tokenfile;
  }

  /**
   * Convert a vargs list to an array.
   * @param args vararg list of arguments
   * @return the generated array.
   */
  private String[] args(String...args) {
    return args;
  }

  /**
   * This test looks at the identity which goes with a DT.
   * It assumes that the username of a token == the user who created it.
   * Some tokens may change that in future (maybe use Role ARN?).
   */
  @Test
  public void testFileSystemBoundToCreator() throws Throwable {
    describe("Run tests to verify the DT Setup is bound to the creator");

    // quick sanity check to make sure alice and bob are different
    assertNotEquals(aliceUser.getUserName(), bobUser.getUserName(),
        "Alice and Bob logins");

    final S3AFileSystem fs = getFileSystem();
    assertEquals(ALICE,
        doAs(bobUser, () -> fs.getUsername()), "FS username in doAs()");

    UserGroupInformation fsOwner = doAs(bobUser,
        () -> fs.getDelegationTokens().get().getOwner());
    assertEquals(aliceUser.getUserName(), fsOwner.getUserName(),
        "username mismatch");

    Token<AbstractS3ATokenIdentifier> dt = fs.getDelegationToken(ALICE);
    AbstractS3ATokenIdentifier identifier
        = dt.decodeIdentifier();
    UserGroupInformation user = identifier.getUser();
    assertEquals(aliceUser.getUserName(), user.getUserName(), "User in DT");
  }


  protected String dtutil(int expected, String...args) throws Exception {
    final ByteArrayOutputStream dtUtilContent = new ByteArrayOutputStream();
    DtUtilShell dt = new DtUtilShell();
    dt.setOut(new PrintStream(dtUtilContent));
    dtUtilContent.reset();
    int r =  doAs(aliceUser,
        () ->ToolRunner.run(getConfiguration(), dt, args));
    String s = dtUtilContent.toString();
    LOG.info("\n{}", s);
    assertEquals(expected, r);
    return s;
  }

  @Test
  public void testDTUtilShell() throws Throwable {
    describe("Verify the dtutil shell command can fetch tokens");
    File tokenfile = createTempTokenFile();

    String tfs = tokenfile.toString();
    String fsURI = getFileSystem().getCanonicalUri().toString();
    dtutil(0,
        "get", fsURI,
        "-format", "protobuf",
        tfs);
    assertTrue(tokenfile.exists(), "not created: " + tokenfile);
    assertTrue(tokenfile.length() > 0, "File is empty" + tokenfile);
    assertTrue(tokenfile.length() > 6,
        "File only contains header" + tokenfile);

    String printed = dtutil(0, "print", tfs);
    assertThat(printed).contains(fsURI);
    assertThat(printed).contains(getTokenKind().toString());

  }

}
