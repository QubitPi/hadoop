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
package org.apache.hadoop.hdfs;

import java.io.IOException;
import java.net.URI;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.key.KeyProvider;
import org.apache.hadoop.crypto.key.KeyProviderFactory;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestKeyProviderCache {

  public static class DummyKeyProvider extends KeyProvider {

    public static int CLOSE_CALL_COUNT = 0;

    public DummyKeyProvider(Configuration conf) {
      super(conf);
    }

    @Override
    public KeyVersion getKeyVersion(String versionName) throws IOException {
      return null;
    }

    @Override
    public List<String> getKeys() throws IOException {
      return null;
    }

    @Override
    public List<KeyVersion> getKeyVersions(String name) throws IOException {
      return null;
    }

    @Override
    public Metadata getMetadata(String name) throws IOException {
      return null;
    }

    @Override
    public KeyVersion createKey(String name, byte[] material, Options options)
        throws IOException {
      return null;
    }

    @Override
    public void deleteKey(String name) throws IOException {
    }

    @Override
    public KeyVersion rollNewVersion(String name, byte[] material)
        throws IOException {
      return null;
    }

    @Override
    public void flush() throws IOException {
    }

    @Override
    public void close() {
      CLOSE_CALL_COUNT += 1;
    }
  }

  public static class Factory extends KeyProviderFactory {

    @Override
    public KeyProvider createProvider(URI providerName, Configuration conf)
        throws IOException {
      if ("dummy".equals(providerName.getScheme())) {
        return new DummyKeyProvider(conf);
      }
      return null;
    }
  }

  @Test
  public void testCache() throws Exception {
    KeyProviderCache kpCache = new KeyProviderCache(10000);
    Configuration conf = new Configuration();
    conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_KEY_PROVIDER_PATH,
        "dummy://foo:bar@test_provider1");
    KeyProvider keyProvider1 = kpCache.get(conf,
        getKeyProviderUriFromConf(conf));
    assertNotNull(keyProvider1, "Returned Key Provider is null !!");

    conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_KEY_PROVIDER_PATH,
        "dummy://foo:bar@test_provider1");
    KeyProvider keyProvider2 = kpCache.get(conf,
        getKeyProviderUriFromConf(conf));

    assertTrue(keyProvider1 == keyProvider2, "Different KeyProviders returned !!");

    conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_KEY_PROVIDER_PATH,
        "dummy://test_provider3");
    KeyProvider keyProvider3 = kpCache.get(conf,
        getKeyProviderUriFromConf(conf));

    assertFalse(keyProvider1 == keyProvider3, "Same KeyProviders returned !!");

    conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_KEY_PROVIDER_PATH,
        "dummy://hello:there@test_provider1");
    KeyProvider keyProvider4 = kpCache.get(conf,
        getKeyProviderUriFromConf(conf));

    assertFalse(keyProvider1 == keyProvider4, "Same KeyProviders returned !!");

    kpCache.invalidateCache();
    assertEquals(3, DummyKeyProvider.CLOSE_CALL_COUNT,
        "Expected number of closing calls doesn't match");
  }

  private URI getKeyProviderUriFromConf(Configuration conf) {
    String providerUriStr = conf.get(
        CommonConfigurationKeysPublic.HADOOP_SECURITY_KEY_PROVIDER_PATH);
    if (providerUriStr == null || providerUriStr.isEmpty()) {
      return null;
    }
    return URI.create(providerUriStr);
  }
}
