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

package org.apache.hadoop.fs.s3a;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.impl.S3AEncryption;
import org.apache.hadoop.security.ProviderUtils;
import org.apache.hadoop.security.alias.CredentialProvider;
import org.apache.hadoop.security.alias.CredentialProviderFactory;
import org.apache.hadoop.util.StringUtils;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import static org.apache.hadoop.fs.s3a.Constants.*;
import static org.apache.hadoop.fs.s3a.S3AEncryptionMethods.*;
import static org.apache.hadoop.fs.s3a.S3ATestConstants.S3A_TEST_TIMEOUT;
import static org.apache.hadoop.fs.s3a.S3AUtils.*;
import static org.apache.hadoop.test.LambdaTestUtils.*;

/**
 * Test SSE setup operations and errors raised.
 * Tests related to secret providers and AWS credentials are also
 * included, as they share some common setup operations.
 */
@Timeout(value = S3A_TEST_TIMEOUT, unit = TimeUnit.MILLISECONDS)
public class TestSSEConfiguration extends Assertions {

  /** Bucket to use for per-bucket options. */
  public static final String BUCKET = "dataset-1";

  /** Valid set of key/value pairs for the encryption context. */
  private static final String VALID_ENCRYPTION_CONTEXT = "key1=value1, key2=value2, key3=value3";

  @TempDir
  private Path tempDir;

  @Test
  public void testSSECNoKey() throws Throwable {
    assertGetAlgorithmFails(SSE_C_NO_KEY_ERROR, SSE_C.getMethod(), null, null);
  }

  @Test
  public void testSSECBlankKey() throws Throwable {
    assertGetAlgorithmFails(SSE_C_NO_KEY_ERROR, SSE_C.getMethod(), "", null);
  }

  @Test
  public void testSSECGoodKey() throws Throwable {
    assertEquals(SSE_C, getAlgorithm(SSE_C, "sseckey", null));
  }

  @Test
  public void testKMSGoodKey() throws Throwable {
    assertEquals(SSE_KMS, getAlgorithm(SSE_KMS, "kmskey", null));
  }

  @Test
  public void testAESKeySet() throws Throwable {
    assertGetAlgorithmFails(SSE_S3_WITH_KEY_ERROR,
        SSE_S3.getMethod(), "setkey", null);
  }

  @Test
  public void testSSEEmptyKey() {
    // test the internal logic of the test setup code
    Configuration c = buildConf(SSE_C.getMethod(), "", null);
    assertEquals("", getS3EncryptionKey(BUCKET, c));
  }

  @Test
  public void testSSEKeyNull() throws Throwable {
    // test the internal logic of the test setup code
    final Configuration c = buildConf(SSE_C.getMethod(), null, null);
    assertEquals("", getS3EncryptionKey(BUCKET, c));

    intercept(IOException.class, SSE_C_NO_KEY_ERROR,
        () -> getEncryptionAlgorithm(BUCKET, c));
  }

  @Test
  public void testSSEKeyFromCredentialProvider() throws Exception {
    // set up conf to have a cred provider
    final Configuration conf = confWithProvider();
    String key = "provisioned";
    setProviderOption(conf, Constants.S3_ENCRYPTION_KEY, key);
    // let's set the password in config and ensure that it uses the credential
    // provider provisioned value instead.
    conf.set(Constants.S3_ENCRYPTION_KEY, "keyInConfObject");

    String sseKey = getS3EncryptionKey(BUCKET, conf);
    assertNotNull(sseKey, "Proxy password should not retrun null.");
    assertEquals(key, sseKey, "Proxy password override did NOT work.");
  }

  /**
   * Add a temp file provider to the config.
   * @param conf config
   * @throws Exception failure
   */
  private void addFileProvider(Configuration conf)
      throws Exception {
    final File file = tempDir.resolve("test.jks").toFile();
    final URI jks = ProviderUtils.nestURIForLocalJavaKeyStoreProvider(
        file.toURI());
    conf.set(CredentialProviderFactory.CREDENTIAL_PROVIDER_PATH,
        jks.toString());
  }

  /**
   * Set the an option under the configuration via the
   * {@link CredentialProviderFactory} APIs.
   * @param conf config
   * @param option option name
   * @param value value to set option to.
   * @throws Exception failure
   */
  void setProviderOption(final Configuration conf,
      String option, String value) throws Exception {
    // add our password to the provider
    final CredentialProvider provider =
        CredentialProviderFactory.getProviders(conf).get(0);
    provider.createCredentialEntry(option,
        value.toCharArray());
    provider.flush();
  }

  /**
   * Assert that the exception text from {@link #getAlgorithm(String, String, String)}
   * is as expected.
   * @param expected expected substring in error
   * @param alg algorithm to ask for
   * @param key optional key value
   * @param context optional encryption context value
   * @throws Exception anything else which gets raised
   */
  public void assertGetAlgorithmFails(String expected,
      final String alg, final String key, final String context) throws Exception {
    intercept(IOException.class, expected,
        () -> getAlgorithm(alg, key, context));
  }

  private S3AEncryptionMethods getAlgorithm(S3AEncryptionMethods algorithm,
      String key,
      String encryptionContext)
      throws IOException {
    return getAlgorithm(algorithm.getMethod(), key, encryptionContext);
  }

  private S3AEncryptionMethods getAlgorithm(String algorithm, String key, String encryptionContext)
      throws IOException {
    return getEncryptionAlgorithm(BUCKET, buildConf(algorithm, key, encryptionContext));
  }

  /**
   * Build a new configuration with the given S3-SSE algorithm
   * and key.
   * @param algorithm  algorithm to use, may be null
   * @param key key, may be null
   * @param encryptionContext encryption context, may be null
   * @return the new config.
   */
  @SuppressWarnings("deprecation")
  private Configuration buildConf(String algorithm, String key, String encryptionContext) {
    Configuration conf = emptyConf();
    if (algorithm != null) {
      conf.set(Constants.S3_ENCRYPTION_ALGORITHM, algorithm);
    } else {
      conf.unset(SERVER_SIDE_ENCRYPTION_ALGORITHM);
      conf.unset(Constants.S3_ENCRYPTION_ALGORITHM);
    }
    if (key != null) {
      conf.set(Constants.S3_ENCRYPTION_KEY, key);
    } else {
      conf.unset(SERVER_SIDE_ENCRYPTION_KEY);
      conf.unset(Constants.S3_ENCRYPTION_KEY);
    }
    if (encryptionContext != null) {
      conf.set(S3_ENCRYPTION_CONTEXT, encryptionContext);
    } else {
      conf.unset(S3_ENCRYPTION_CONTEXT);
    }
    return conf;
  }

  /**
   * Create an empty conf: no -default or -site values.
   * @return an empty configuration
   */
  private Configuration emptyConf() {
    return new Configuration(false);
  }

  /**
   * Create a configuration with no defaults and bonded to a file
   * provider, so that
   * {@link #setProviderOption(Configuration, String, String)}
   * can be used to set a secret.
   * @return the configuration
   * @throws Exception any failure
   */
  private Configuration confWithProvider() throws Exception {
    final Configuration conf = emptyConf();
    addFileProvider(conf);
    return conf;
  }


  private static final String SECRET = "*secret*";

  private static final String BUCKET_PATTERN = FS_S3A_BUCKET_PREFIX + "%s.%s";

  @Test
  public void testGetPasswordFromConf() throws Throwable {
    final Configuration conf = emptyConf();
    conf.set(SECRET_KEY, SECRET);
    assertEquals(SECRET, lookupPassword(conf, SECRET_KEY, ""));
    assertEquals(SECRET, lookupPassword(conf, SECRET_KEY, "defVal"));
  }

  @Test
  public void testGetPasswordFromProvider() throws Throwable {
    final Configuration conf = confWithProvider();
    setProviderOption(conf, SECRET_KEY, SECRET);
    assertEquals(SECRET, lookupPassword(conf, SECRET_KEY, ""));
    assertSecretKeyEquals(conf, null, SECRET, "");
    assertSecretKeyEquals(conf, null, "overidden", "overidden");
  }

  @Test
  public void testGetBucketPasswordFromProvider() throws Throwable {
    final Configuration conf = confWithProvider();
    URI bucketURI = new URI("s3a://"+ BUCKET +"/");
    setProviderOption(conf, SECRET_KEY, "unbucketed");

    String bucketedKey = String.format(BUCKET_PATTERN, BUCKET, SECRET_KEY);
    setProviderOption(conf, bucketedKey, SECRET);
    String overrideVal;
    overrideVal = "";
    assertSecretKeyEquals(conf, BUCKET, SECRET, overrideVal);
    assertSecretKeyEquals(conf, bucketURI.getHost(), SECRET, "");
    assertSecretKeyEquals(conf, bucketURI.getHost(), "overidden", "overidden");
  }

  /**
   * Assert that a secret key is as expected.
   * @param conf configuration to examine
   * @param bucket bucket name
   * @param expected expected value
   * @param overrideVal override value in {@code S3AUtils.lookupPassword()}
   * @throws IOException IO problem
   */
  private void assertSecretKeyEquals(Configuration conf,
      String bucket,
      String expected, String overrideVal) throws IOException {
    assertEquals(expected,
        S3AUtils.lookupPassword(bucket, conf, SECRET_KEY, overrideVal, null));
  }

  @Test
  public void testGetBucketPasswordFromProviderShort() throws Throwable {
    final Configuration conf = confWithProvider();
    URI bucketURI = new URI("s3a://"+ BUCKET +"/");
    setProviderOption(conf, SECRET_KEY, "unbucketed");

    String bucketedKey = String.format(BUCKET_PATTERN, BUCKET, "secret.key");
    setProviderOption(conf, bucketedKey, SECRET);
    assertSecretKeyEquals(conf, BUCKET, SECRET, "");
    assertSecretKeyEquals(conf, bucketURI.getHost(), SECRET, "");
    assertSecretKeyEquals(conf, bucketURI.getHost(), "overidden", "overidden");
  }

  @Test
  public void testUnknownEncryptionMethod() throws Throwable {
    intercept(IOException.class, UNKNOWN_ALGORITHM,
        () -> S3AEncryptionMethods.getMethod("SSE-ROT13"));
  }

  @Test
  public void testClientEncryptionMethod() throws Throwable {
    S3AEncryptionMethods method = getMethod("CSE-KMS");
    assertEquals(CSE_KMS, method);
    assertFalse(method.isServerSide(), "shouldn't be server side " + method);
  }

  @Test
  public void testCSEKMSEncryptionMethod() throws Throwable {
    S3AEncryptionMethods method = getMethod("CSE-CUSTOM");
    assertEquals(CSE_CUSTOM, method);
    assertFalse(method.isServerSide(), "shouldn't be server side " + method);
  }

  @Test
  public void testNoEncryptionMethod() throws Throwable {
    assertEquals(NONE, getMethod(" "));
  }

  @Test
  public void testGoodEncryptionContext() throws Throwable {
    assertEquals(SSE_KMS, getAlgorithm(SSE_KMS, "kmskey", VALID_ENCRYPTION_CONTEXT));
  }

  @Test
  public void testSSEEmptyEncryptionContext() throws Throwable {
    // test the internal logic of the test setup code
    Configuration c = buildConf(SSE_KMS.getMethod(), "kmskey", "");
    assertEquals("", S3AEncryption.getS3EncryptionContext(BUCKET, c));
  }

  @Test
  public void testSSEEncryptionContextNull() throws Throwable {
    // test the internal logic of the test setup code
    final Configuration c = buildConf(SSE_KMS.getMethod(), "kmskey", null);
    assertEquals("", S3AEncryption.getS3EncryptionContext(BUCKET, c));
  }

  @Test
  public void testSSEInvalidEncryptionContext() throws Throwable {
    intercept(IllegalArgumentException.class,
        StringUtils.STRING_COLLECTION_SPLIT_EQUALS_INVALID_ARG,
        () -> getAlgorithm(SSE_KMS.getMethod(), "kmskey", "invalid context"));
  }

}
