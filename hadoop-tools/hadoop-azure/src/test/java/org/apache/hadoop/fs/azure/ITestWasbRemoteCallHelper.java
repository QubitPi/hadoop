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

package org.apache.hadoop.fs.azure;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.io.retry.RetryUtils;
import org.apache.http.Header;
import org.apache.http.HttpResponse;
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.StatusLine;
import org.apache.http.ProtocolVersion;
import org.apache.http.ParseException;
import org.apache.http.HeaderElement;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatcher;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;

import static org.apache.hadoop.fs.azure.AzureNativeFileSystemStore.KEY_USE_SECURE_MODE;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.times;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Test class to hold all WasbRemoteCallHelper tests.
 */
public class ITestWasbRemoteCallHelper
    extends AbstractWasbTestBase {
  public static final String EMPTY_STRING = "";
  private static final int INVALID_HTTP_STATUS_CODE_999 = 999;

  @Override
  protected AzureBlobStorageTestAccount createTestAccount() throws Exception {
    Configuration conf = new Configuration();
    conf.set(NativeAzureFileSystem.KEY_AZURE_AUTHORIZATION, "true");
    conf.set(RemoteWasbAuthorizerImpl.KEY_REMOTE_AUTH_SERVICE_URLS, "http://localhost1/,http://localhost2/,http://localhost:8080");
    return AzureBlobStorageTestAccount.create(conf);
  }

  @BeforeEach
  @Override
  public void setUp() throws Exception {
    super.setUp();
    boolean useSecureMode = fs.getConf().getBoolean(KEY_USE_SECURE_MODE, false);
    boolean useAuthorization = fs.getConf()
        .getBoolean(NativeAzureFileSystem.KEY_AZURE_AUTHORIZATION, false);
    assumeTrue(useSecureMode && useAuthorization,
        "Test valid when both SecureMode and Authorization are enabled .. skipping");
  }

  /**
   * Test invalid status-code.
   * @throws Throwable
   */
  @Test // (expected = WasbAuthorizationException.class)
  public void testInvalidStatusCode() throws Throwable {

    assertThrows(WasbAuthorizationException.class, () -> {
      // set up mocks
      HttpClient mockHttpClient = Mockito.mock(HttpClient.class);
      HttpResponse mockHttpResponse = Mockito.mock(HttpResponse.class);
      Mockito.when(mockHttpClient.execute(Mockito.<HttpGet>any()))
          .thenReturn(mockHttpResponse);
      Mockito.when(mockHttpResponse.getStatusLine())
          .thenReturn(newStatusLine(INVALID_HTTP_STATUS_CODE_999));
      // finished setting up mocks

      performop(mockHttpClient);
    });
  }

  /**
   * Test invalid Content-Type.
   * @throws Throwable
   */
  @Test // (expected = WasbAuthorizationException.class)
  public void testInvalidContentType() throws Throwable {
    assertThrows(WasbAuthorizationException.class, () -> {
      // set up mocks
      HttpClient mockHttpClient = Mockito.mock(HttpClient.class);
      HttpResponse mockHttpResponse = Mockito.mock(HttpResponse.class);
      Mockito.when(mockHttpClient.execute(Mockito.<HttpGet>any())).thenReturn(mockHttpResponse);
      Mockito.when(mockHttpResponse.getStatusLine()).thenReturn(newStatusLine(HttpStatus.SC_OK));
      Mockito.when(mockHttpResponse.getFirstHeader("Content-Type"))
          .thenReturn(newHeader("Content-Type", "text/plain"));
      // finished setting up mocks
      performop(mockHttpClient);
    });
  }

  /**
   * Test missing Content-Length.
   * @throws Throwable
   */
  @Test // (expected = WasbAuthorizationException.class)
  public void testMissingContentLength() throws Throwable {

    assertThrows(WasbAuthorizationException.class, () -> {
      // set up mocks
      HttpClient mockHttpClient = Mockito.mock(HttpClient.class);
      HttpResponse mockHttpResponse = Mockito.mock(HttpResponse.class);
      Mockito.when(mockHttpClient.execute(Mockito.<HttpGet>any())).thenReturn(mockHttpResponse);
      Mockito.when(mockHttpResponse.getStatusLine()).thenReturn(newStatusLine(HttpStatus.SC_OK));
      Mockito.when(mockHttpResponse.getFirstHeader("Content-Type"))
          .thenReturn(newHeader("Content-Type", "application/json"));
      // finished setting up mocks

      performop(mockHttpClient);
    });
  }

  /**
   * Test Content-Length exceeds max.
   * @throws Throwable
   */
  @Test // (expected = WasbAuthorizationException.class)
  public void testContentLengthExceedsMax() throws Throwable {

    assertThrows(WasbAuthorizationException.class, () -> {
      // set up mocks
      HttpClient mockHttpClient = Mockito.mock(HttpClient.class);
      HttpResponse mockHttpResponse = Mockito.mock(HttpResponse.class);
      Mockito.when(mockHttpClient.execute(Mockito.<HttpGet>any())).thenReturn(mockHttpResponse);
      Mockito.when(mockHttpResponse.getStatusLine()).thenReturn(newStatusLine(HttpStatus.SC_OK));
      Mockito.when(mockHttpResponse.getFirstHeader("Content-Type"))
          .thenReturn(newHeader("Content-Type", "application/json"));
      Mockito.when(mockHttpResponse.getFirstHeader("Content-Length"))
          .thenReturn(newHeader("Content-Length", "2048"));
      // finished setting up mocks
      performop(mockHttpClient);
    });
  }

  /**
   * Test invalid Content-Length value
   * @throws Throwable
   */
  @Test // (expected = WasbAuthorizationException.class)
  public void testInvalidContentLengthValue() throws Throwable {

    assertThrows(WasbAuthorizationException.class, () -> {
      // set up mocks
      HttpClient mockHttpClient = Mockito.mock(HttpClient.class);
      HttpResponse mockHttpResponse = Mockito.mock(HttpResponse.class);
      Mockito.when(mockHttpClient.execute(Mockito.<HttpGet>any())).thenReturn(mockHttpResponse);
      Mockito.when(mockHttpResponse.getStatusLine()).thenReturn(newStatusLine(HttpStatus.SC_OK));
      Mockito.when(mockHttpResponse.getFirstHeader("Content-Type"))
          .thenReturn(newHeader("Content-Type", "application/json"));
      Mockito.when(mockHttpResponse.getFirstHeader("Content-Length"))
          .thenReturn(newHeader("Content-Length", "20abc48"));
      // finished setting up mocks

      performop(mockHttpClient);
    });
  }

  /**
   * Test valid JSON response.
   * @throws Throwable
   */
  @Test
  public void testValidJSONResponse() throws Throwable {

    // set up mocks
    HttpClient mockHttpClient = Mockito.mock(HttpClient.class);

    HttpResponse mockHttpResponse = Mockito.mock(HttpResponse.class);
    HttpEntity mockHttpEntity = Mockito.mock(HttpEntity.class);

    Mockito.when(mockHttpClient.execute(Mockito.<HttpGet>any())).thenReturn(mockHttpResponse);
    Mockito.when(mockHttpResponse.getStatusLine()).thenReturn(newStatusLine(HttpStatus.SC_OK));
    Mockito.when(mockHttpResponse.getFirstHeader("Content-Type"))
        .thenReturn(newHeader("Content-Type", "application/json"));
    Mockito.when(mockHttpResponse.getFirstHeader("Content-Length"))
        .thenReturn(newHeader("Content-Length", "1024"));
    Mockito.when(mockHttpResponse.getEntity()).thenReturn(mockHttpEntity);
    Mockito.when(mockHttpEntity.getContent())
        .thenReturn(new ByteArrayInputStream(validJsonResponse().getBytes(StandardCharsets.UTF_8)))
        .thenReturn(new ByteArrayInputStream(validJsonResponse().getBytes(StandardCharsets.UTF_8)))
        .thenReturn(new ByteArrayInputStream(validJsonResponse().getBytes(StandardCharsets.UTF_8)));
    // finished setting up mocks

    performop(mockHttpClient);
  }

  /**
   * Test malformed JSON response.
   * @throws Throwable
   */
  @Test // (expected = WasbAuthorizationException.class)
  public void testMalFormedJSONResponse() throws Throwable {

    String errorMsg =
        "com.fasterxml.jackson.core.JsonParseException: Unexpected end-of-input in FIELD_NAME";
    assertThrows(WasbAuthorizationException.class, () -> {
      // set up mocks
      HttpClient mockHttpClient = Mockito.mock(HttpClient.class);

      HttpResponse mockHttpResponse = Mockito.mock(HttpResponse.class);
      HttpEntity mockHttpEntity = Mockito.mock(HttpEntity.class);

      Mockito.when(mockHttpClient.execute(Mockito.<HttpGet>any())).thenReturn(mockHttpResponse);
      Mockito.when(mockHttpResponse.getStatusLine()).thenReturn(newStatusLine(HttpStatus.SC_OK));
      Mockito.when(mockHttpResponse.getFirstHeader("Content-Type"))
          .thenReturn(newHeader("Content-Type", "application/json"));
      Mockito.when(mockHttpResponse.getFirstHeader("Content-Length"))
          .thenReturn(newHeader("Content-Length", "1024"));
      Mockito.when(mockHttpResponse.getEntity()).thenReturn(mockHttpEntity);
      Mockito.when(mockHttpEntity.getContent())
          .thenReturn(new ByteArrayInputStream(malformedJsonResponse().getBytes(StandardCharsets.UTF_8)));
      // finished setting up mocks

      performop(mockHttpClient);
    }, errorMsg);

  }

  /**
   * Test valid JSON response failure response code.
   * @throws Throwable
   */
  @Test // (expected = WasbAuthorizationException.class)
  public void testFailureCodeJSONResponse() throws Throwable {
    String errorMsg = "Remote authorization service encountered an error Unauthorized";

    assertThrows(WasbAuthorizationException.class, () -> {
      // set up mocks
      HttpClient mockHttpClient = Mockito.mock(HttpClient.class);

      HttpResponse mockHttpResponse = Mockito.mock(HttpResponse.class);
      HttpEntity mockHttpEntity = Mockito.mock(HttpEntity.class);

      Mockito.when(mockHttpClient.execute(Mockito.<HttpGet>any())).thenReturn(mockHttpResponse);
      Mockito.when(mockHttpResponse.getStatusLine()).thenReturn(newStatusLine(HttpStatus.SC_OK));
      Mockito.when(mockHttpResponse.getFirstHeader("Content-Type"))
          .thenReturn(newHeader("Content-Type", "application/json"));
      Mockito.when(mockHttpResponse.getFirstHeader("Content-Length"))
          .thenReturn(newHeader("Content-Length", "1024"));
      Mockito.when(mockHttpResponse.getEntity()).thenReturn(mockHttpEntity);
      Mockito.when(mockHttpEntity.getContent())
          .thenReturn(new ByteArrayInputStream(failureCodeJsonResponse()
          .getBytes(StandardCharsets.UTF_8)));
      // finished setting up mocks
      performop(mockHttpClient);
    }, errorMsg);
  }

  @Test
  public void testWhenOneInstanceIsDown() throws Throwable {

    boolean isAuthorizationCachingEnabled = fs.getConf().getBoolean(CachingAuthorizer.KEY_AUTH_SERVICE_CACHING_ENABLE, false);

    // set up mocks
    HttpClient mockHttpClient = Mockito.mock(HttpClient.class);
    HttpEntity mockHttpEntity = Mockito.mock(HttpEntity.class);

    HttpResponse mockHttpResponseService1 = Mockito.mock(HttpResponse.class);
    Mockito.when(mockHttpResponseService1.getStatusLine())
        .thenReturn(newStatusLine(HttpStatus.SC_INTERNAL_SERVER_ERROR));
    Mockito.when(mockHttpResponseService1.getFirstHeader("Content-Type"))
        .thenReturn(newHeader("Content-Type", "application/json"));
    Mockito.when(mockHttpResponseService1.getFirstHeader("Content-Length"))
        .thenReturn(newHeader("Content-Length", "1024"));
    Mockito.when(mockHttpResponseService1.getEntity())
        .thenReturn(mockHttpEntity);

    HttpResponse mockHttpResponseService2 = Mockito.mock(HttpResponse.class);
    Mockito.when(mockHttpResponseService2.getStatusLine())
        .thenReturn(newStatusLine(HttpStatus.SC_OK));
    Mockito.when(mockHttpResponseService2.getFirstHeader("Content-Type"))
        .thenReturn(newHeader("Content-Type", "application/json"));
    Mockito.when(mockHttpResponseService2.getFirstHeader("Content-Length"))
        .thenReturn(newHeader("Content-Length", "1024"));
    Mockito.when(mockHttpResponseService2.getEntity())
        .thenReturn(mockHttpEntity);

    HttpResponse mockHttpResponseServiceLocal = Mockito.mock(HttpResponse.class);
    Mockito.when(mockHttpResponseServiceLocal.getStatusLine())
        .thenReturn(newStatusLine(HttpStatus.SC_INTERNAL_SERVER_ERROR));
    Mockito.when(mockHttpResponseServiceLocal.getFirstHeader("Content-Type"))
        .thenReturn(newHeader("Content-Type", "application/json"));
    Mockito.when(mockHttpResponseServiceLocal.getFirstHeader("Content-Length"))
        .thenReturn(newHeader("Content-Length", "1024"));
    Mockito.when(mockHttpResponseServiceLocal.getEntity())
        .thenReturn(mockHttpEntity);

    Mockito.when(mockHttpClient.execute(argThat(new HttpGetForService1())))
        .thenReturn(mockHttpResponseService1);
    Mockito.when(mockHttpClient.execute(argThat(new HttpGetForService2())))
        .thenReturn(mockHttpResponseService2);
    Mockito.when(mockHttpClient.execute(argThat(new HttpGetForServiceLocal())))
        .thenReturn(mockHttpResponseServiceLocal);

    //Need 2 times because performop()  does 2 fs operations.
    Mockito.when(mockHttpEntity.getContent())
        .thenReturn(new ByteArrayInputStream(validJsonResponse()
            .getBytes(StandardCharsets.UTF_8)))
        .thenReturn(new ByteArrayInputStream(validJsonResponse()
            .getBytes(StandardCharsets.UTF_8)))
        .thenReturn(new ByteArrayInputStream(validJsonResponse()
            .getBytes(StandardCharsets.UTF_8)));
    // finished setting up mocks

    performop(mockHttpClient);

    int expectedNumberOfInvocations = isAuthorizationCachingEnabled ? 2 : 3;
    Mockito.verify(mockHttpClient, times(expectedNumberOfInvocations)).execute(Mockito.argThat(new HttpGetForServiceLocal()));
    Mockito.verify(mockHttpClient, times(expectedNumberOfInvocations)).execute(Mockito.argThat(new HttpGetForService2()));
  }

  @Test
  public void testWhenServiceInstancesAreDown() throws Throwable {
    //expectedEx.expect(WasbAuthorizationException.class);
    // set up mocks
    HttpClient mockHttpClient = Mockito.mock(HttpClient.class);
    HttpEntity mockHttpEntity = Mockito.mock(HttpEntity.class);

    HttpResponse mockHttpResponseService1 = Mockito.mock(HttpResponse.class);
    Mockito.when(mockHttpResponseService1.getStatusLine())
        .thenReturn(newStatusLine(HttpStatus.SC_INTERNAL_SERVER_ERROR));
    Mockito.when(mockHttpResponseService1.getFirstHeader("Content-Type"))
        .thenReturn(newHeader("Content-Type", "application/json"));
    Mockito.when(mockHttpResponseService1.getFirstHeader("Content-Length"))
        .thenReturn(newHeader("Content-Length", "1024"));
    Mockito.when(mockHttpResponseService1.getEntity())
        .thenReturn(mockHttpEntity);

    HttpResponse mockHttpResponseService2 = Mockito.mock(HttpResponse.class);
    Mockito.when(mockHttpResponseService2.getStatusLine())
        .thenReturn(newStatusLine(
        HttpStatus.SC_INTERNAL_SERVER_ERROR));
    Mockito.when(mockHttpResponseService2.getFirstHeader("Content-Type"))
        .thenReturn(newHeader("Content-Type", "application/json"));
    Mockito.when(mockHttpResponseService2.getFirstHeader("Content-Length"))
        .thenReturn(newHeader("Content-Length", "1024"));
    Mockito.when(mockHttpResponseService2.getEntity())
        .thenReturn(mockHttpEntity);

    HttpResponse mockHttpResponseService3 = Mockito.mock(HttpResponse.class);
    Mockito.when(mockHttpResponseService3.getStatusLine())
        .thenReturn(newStatusLine(
            HttpStatus.SC_INTERNAL_SERVER_ERROR));
    Mockito.when(mockHttpResponseService3.getFirstHeader("Content-Type"))
        .thenReturn(newHeader("Content-Type", "application/json"));
    Mockito.when(mockHttpResponseService3.getFirstHeader("Content-Length"))
        .thenReturn(newHeader("Content-Length", "1024"));
    Mockito.when(mockHttpResponseService3.getEntity())
        .thenReturn(mockHttpEntity);

    Mockito.when(mockHttpClient.execute(argThat(new HttpGetForService1())))
        .thenReturn(mockHttpResponseService1);
    Mockito.when(mockHttpClient.execute(argThat(new HttpGetForService2())))
        .thenReturn(mockHttpResponseService2);
    Mockito.when(mockHttpClient.execute(argThat(new HttpGetForServiceLocal())))
        .thenReturn(mockHttpResponseService3);

    //Need 3 times because performop()  does 3 fs operations.
    Mockito.when(mockHttpEntity.getContent())
        .thenReturn(new ByteArrayInputStream(
            validJsonResponse().getBytes(StandardCharsets.UTF_8)))
        .thenReturn(new ByteArrayInputStream(
            validJsonResponse().getBytes(StandardCharsets.UTF_8)))
        .thenReturn(new ByteArrayInputStream(
            validJsonResponse().getBytes(StandardCharsets.UTF_8)));
    // finished setting up mocks
    try {
      performop(mockHttpClient);
    }catch (WasbAuthorizationException e){
      e.printStackTrace();
      Mockito.verify(mockHttpClient, atLeast(2))
          .execute(argThat(new HttpGetForService1()));
      Mockito.verify(mockHttpClient, atLeast(2))
          .execute(argThat(new HttpGetForService2()));
      Mockito.verify(mockHttpClient, atLeast(3))
          .execute(argThat(new HttpGetForServiceLocal()));
      Mockito.verify(mockHttpClient, times(7)).execute(Mockito.<HttpGet>any());
    }
  }

  private void performop(HttpClient mockHttpClient) throws Throwable {

    Path testPath = new Path("/", "test.dat");

    RemoteWasbAuthorizerImpl authorizer = new RemoteWasbAuthorizerImpl();
    authorizer.init(fs.getConf());
    WasbRemoteCallHelper mockWasbRemoteCallHelper = new WasbRemoteCallHelper(
        RetryUtils.getMultipleLinearRandomRetry(new Configuration(),
            EMPTY_STRING, true,
            EMPTY_STRING, "1000,3,10000,2"));
    mockWasbRemoteCallHelper.updateHttpClient(mockHttpClient);
    authorizer.updateWasbRemoteCallHelper(mockWasbRemoteCallHelper);
    fs.updateWasbAuthorizer(authorizer);

    fs.create(testPath);
    ContractTestUtils.assertPathExists(fs, "testPath was not created", testPath);
    fs.delete(testPath, false);
  }

  private String validJsonResponse() {
    return "{"
        + "\"responseCode\": 0,"
        + "\"authorizationResult\": true,"
        + "\"responseMessage\": \"Authorized\""
        + "}";
  }

  private String malformedJsonResponse() {
    return "{"
        + "\"responseCode\": 0,"
        + "\"authorizationResult\": true,"
        + "\"responseMessage\":";
  }

  private String failureCodeJsonResponse() {
    return "{"
        + "\"responseCode\": 1,"
        + "\"authorizationResult\": false,"
        + "\"responseMessage\": \"Unauthorized\""
        + "}";
  }

  private StatusLine newStatusLine(int statusCode) {
    return new StatusLine() {
      @Override
      public ProtocolVersion getProtocolVersion() {
        return new ProtocolVersion("HTTP", 1, 1);
      }

      @Override
      public int getStatusCode() {
        return statusCode;
      }

      @Override
      public String getReasonPhrase() {
        return "Reason Phrase";
      }
    };
  }

  private Header newHeader(String name, String value) {
    return new Header() {
      @Override
      public String getName() {
        return name;
      }

      @Override
      public String getValue() {
        return value;
      }

      @Override
      public HeaderElement[] getElements() throws ParseException {
        return new HeaderElement[0];
      }
    };
  }

  /** Check that a HttpGet request is with given remote host. */
  private static boolean checkHttpGetMatchHost(HttpGet g, String h) {
    return g != null && g.getURI().getHost().equals(h);
  }

  private class HttpGetForService1 implements ArgumentMatcher<HttpGet>{
    @Override
    public boolean matches(HttpGet httpGet) {
      return checkHttpGetMatchHost(httpGet, "localhost1");
    }
  }
  private class HttpGetForService2 implements ArgumentMatcher<HttpGet>{
    @Override
    public boolean matches(HttpGet httpGet) {
      return checkHttpGetMatchHost(httpGet, "localhost2");
    }
  }
  private class HttpGetForServiceLocal implements ArgumentMatcher<HttpGet>{
    @Override
    public boolean matches(HttpGet httpGet) {
      try {
        return checkHttpGetMatchHost(httpGet,
            InetAddress.getLocalHost().getCanonicalHostName());
      } catch (UnknownHostException e) {
        return checkHttpGetMatchHost(httpGet, "localhost");
      }
    }
  }
}
