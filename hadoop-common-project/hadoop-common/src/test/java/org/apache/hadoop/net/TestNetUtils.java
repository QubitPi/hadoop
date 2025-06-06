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
package org.apache.hadoop.net;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.EOFException;
import java.io.IOException;
import java.net.BindException;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.UnknownHostException;
import java.nio.charset.CharacterCodingException;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.zip.ZipException;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.KerberosAuthException;
import org.apache.hadoop.security.NetUtilsTestResolver;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.LambdaTestUtils;
import org.apache.hadoop.util.Shell;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestNetUtils {

  private static final Logger LOG = LoggerFactory.getLogger(TestNetUtils.class);
  private static final int DEST_PORT = 4040;
  private static final String DEST_PORT_NAME = Integer.toString(DEST_PORT);
  private static final int LOCAL_PORT = 8080;
  private static final String LOCAL_PORT_NAME = Integer.toString(LOCAL_PORT);

  /**
   * Some slop around expected times when making sure timeouts behave
   * as expected. We assume that they will be accurate to within
   * this threshold.
   */
  static final long TIME_FUDGE_MILLIS = 200;

  /**
   * Test that we can't accidentally connect back to the connecting socket due
   * to a quirk in the TCP spec.
   *
   * This is a regression test for HADOOP-6722.
   */
  @Test
  public void testAvoidLoopbackTcpSockets() throws Throwable {
    Configuration conf = new Configuration();

    Socket socket = NetUtils.getDefaultSocketFactory(conf)
      .createSocket();
    socket.bind(new InetSocketAddress("127.0.0.1", 0));
    System.err.println("local address: " + socket.getLocalAddress());
    System.err.println("local port: " + socket.getLocalPort());
    try {
      NetUtils.connect(socket,
        new InetSocketAddress(socket.getLocalAddress(), socket.getLocalPort()),
        20000);
      socket.close();
      fail("Should not have connected");
    } catch (ConnectException ce) {
      System.err.println("Got exception: " + ce);
      assertInException(ce, "resulted in a loopback");
    } catch (SocketException se) {
      // Some TCP stacks will actually throw their own Invalid argument exception
      // here. This is also OK.
      assertInException(se, "Invalid argument");
    }
  }

  @Test
  public void testInvalidAddress() throws Throwable {
    Configuration conf = new Configuration();

    Socket socket = NetUtils.getDefaultSocketFactory(conf)
        .createSocket();
    socket.bind(new InetSocketAddress("127.0.0.1", 0));
    try {
      NetUtils.connect(socket,
          new InetSocketAddress("invalid-test-host",
              0), 20000);
      socket.close();
      fail("Should not have connected");
    } catch (UnknownHostException uhe) {
      LOG.info("Got exception: ", uhe);
      if (Shell.isJavaVersionAtLeast(17)) {
        GenericTestUtils
            .assertExceptionContains("invalid-test-host/<unresolved>:0", uhe);
      } else {
        GenericTestUtils.assertExceptionContains("invalid-test-host:0", uhe);
      }
    }
  }

  @Test
  public void testSocketReadTimeoutWithChannel() throws Exception {
    doSocketReadTimeoutTest(true);
  }
  
  @Test
  public void testSocketReadTimeoutWithoutChannel() throws Exception {
    doSocketReadTimeoutTest(false);
  }

  
  private void doSocketReadTimeoutTest(boolean withChannel)
      throws IOException {
    // Binding a ServerSocket is enough to accept connections.
    // Rely on the backlog to accept for us.
    ServerSocket ss = new ServerSocket(0);
    
    Socket s;
    if (withChannel) {
      s = NetUtils.getDefaultSocketFactory(new Configuration())
          .createSocket();
      assumeTrue(s.getChannel() != null);
    } else {
      s = new Socket();
      assertNull(s.getChannel());
    }
    
    SocketInputWrapper stm = null;
    try {
      NetUtils.connect(s, ss.getLocalSocketAddress(), 1000);

      stm = NetUtils.getInputStream(s, 1000);
      assertReadTimeout(stm, 1000);

      // Change timeout, make sure it applies.
      stm.setTimeout(1);
      assertReadTimeout(stm, 1);
      
      // If there is a channel, then setting the socket timeout
      // should not matter. If there is not a channel, it will
      // take effect.
      s.setSoTimeout(1000);
      if (withChannel) {
        assertReadTimeout(stm, 1);
      } else {
        assertReadTimeout(stm, 1000);        
      }
    } finally {
      IOUtils.closeStream(stm);
      IOUtils.closeSocket(s);
      ss.close();
    }
  }
  
  private void assertReadTimeout(SocketInputWrapper stm, int timeoutMillis)
      throws IOException {
    long st = System.nanoTime();
    try {
      stm.read();
      fail("Didn't time out");
    } catch (SocketTimeoutException ste) {
      assertTimeSince(st, timeoutMillis);
    }
  }

  private void assertTimeSince(long startNanos, int expectedMillis) {
    long durationNano = System.nanoTime() - startNanos;
    long millis = TimeUnit.MILLISECONDS.convert(
        durationNano, TimeUnit.NANOSECONDS);
    assertTrue(Math.abs(millis - expectedMillis) < TIME_FUDGE_MILLIS,
        "Expected " + expectedMillis + "ms, but took " + millis);
  }
  
  /**
   * Test for {
   * @throws UnknownHostException @link NetUtils#getLocalInetAddress(String)
   * @throws SocketException 
   */
  @Test
  public void testGetLocalInetAddress() throws Exception {
    assertNotNull(NetUtils.getLocalInetAddress("127.0.0.1"));
    assertNull(NetUtils.getLocalInetAddress("invalid-address-for-test"));
    assertNull(NetUtils.getLocalInetAddress(null));
  }

  @Test
  public void testVerifyHostnamesException() throws UnknownHostException {
    assertThrows(UnknownHostException.class, ()->{
      String[] names = {"valid.host.com", "1.com", "invalid host here"};
      NetUtils.verifyHostnames(names);
    });
  }  

  @Test
  public void testVerifyHostnamesNoException() throws UnknownHostException {
    String[] names = {"valid.host.com", "1.com"};
    NetUtils.verifyHostnames(names);
  }

  /** 
   * Test for {@link NetUtils#isLocalAddress(java.net.InetAddress)}
   */
  @Test
  public void testIsLocalAddress() throws Exception {
    // Test - local host is local address
    assertTrue(NetUtils.isLocalAddress(InetAddress.getLocalHost()));
    
    // Test - all addresses bound network interface is local address
    Enumeration<NetworkInterface> interfaces = NetworkInterface
        .getNetworkInterfaces();
    if (interfaces != null) { // Iterate through all network interfaces
      while (interfaces.hasMoreElements()) {
        NetworkInterface i = interfaces.nextElement();
        Enumeration<InetAddress> addrs = i.getInetAddresses();
        if (addrs == null) {
          continue;
        }
        // Iterate through all the addresses of a network interface
        while (addrs.hasMoreElements()) {
          InetAddress addr = addrs.nextElement();
          assertTrue(NetUtils.isLocalAddress(addr));
        }
      }
    }
    assertFalse(NetUtils.isLocalAddress(InetAddress.getByName("8.8.8.8")));
  }

  @Test
  public void testWrapConnectException() throws Throwable {
    IOException e = new ConnectException("failed");
    IOException wrapped = verifyExceptionClass(e, ConnectException.class);
    assertInException(wrapped, "failed");
    assertWikified(wrapped);
    assertInException(wrapped, "localhost");
    assertRemoteDetailsIncluded(wrapped);
    assertInException(wrapped, "/ConnectionRefused");
  }

  @Test
  public void testWrapBindException() throws Throwable {
    IOException e = new BindException("failed");
    IOException wrapped = verifyExceptionClass(e, BindException.class);
    assertInException(wrapped, "failed");
    assertLocalDetailsIncluded(wrapped);
    assertNotInException(wrapped, DEST_PORT_NAME);
    assertInException(wrapped, "/BindException");
  }

  @Test
  public void testWrapUnknownHostException() throws Throwable {
    IOException e = new UnknownHostException("failed");
    IOException wrapped = verifyExceptionClass(e, UnknownHostException.class);
    assertInException(wrapped, "failed");
    assertWikified(wrapped);
    assertInException(wrapped, "localhost");
    assertRemoteDetailsIncluded(wrapped);
    assertInException(wrapped, "/UnknownHost");
  }
  
  @Test
  public void testWrapEOFException() throws Throwable {
    IOException e = new EOFException("eof");
    IOException wrapped = verifyExceptionClass(e, EOFException.class);
    assertInException(wrapped, "eof");
    assertWikified(wrapped);
    assertInException(wrapped, "localhost");
    assertRemoteDetailsIncluded(wrapped);
    assertInException(wrapped, "/EOFException");
  }

  @Test
  public void testWrapKerbAuthException() throws Throwable {
    IOException e = new KerberosAuthException("socket timeout on connection");
    IOException wrapped = verifyExceptionClass(e, KerberosAuthException.class);
    assertInException(wrapped, "socket timeout on connection");
    assertInException(wrapped, "localhost");
    assertInException(wrapped, "DestHost:destPort ");
    assertInException(wrapped, "LocalHost:localPort");
    assertRemoteDetailsIncluded(wrapped);
    assertInException(wrapped, "KerberosAuthException");
  }

  @Test
  public void testWrapIOEWithNoStringConstructor() throws Throwable {
    IOException e = new CharacterCodingException();
    IOException wrapped =
        verifyExceptionClass(e, CharacterCodingException.class);
    assertEquals(null, wrapped.getMessage());
  }

  @Test
  public void testWrapIOEWithPrivateStringConstructor() throws Throwable {
    class TestIOException extends CharacterCodingException{
      private  TestIOException(String cause){
      }
      TestIOException(){
      }
    }
    IOException e = new TestIOException();
    IOException wrapped = verifyExceptionClass(e, TestIOException.class);
    assertEquals(null, wrapped.getMessage());
  }

  @Test
  public void testWrapSocketException() throws Throwable {
    IOException wrapped = verifyExceptionClass(new SocketException("failed"),
        SocketException.class);
    assertInException(wrapped, "failed");
    assertWikified(wrapped);
    assertInException(wrapped, "localhost");
    assertRemoteDetailsIncluded(wrapped);
    assertInException(wrapped, "/SocketException");
  }

  @Test
  public void testGetConnectAddress() throws IOException {
    NetUtils.addStaticResolution("host", "127.0.0.1");
    InetSocketAddress addr = NetUtils.createSocketAddrForHost("host", 1);
    InetSocketAddress connectAddr = NetUtils.getConnectAddress(addr);
    assertEquals(addr.getHostName(), connectAddr.getHostName());
    
    addr = new InetSocketAddress(1);
    connectAddr = NetUtils.getConnectAddress(addr);
    assertEquals(InetAddress.getLocalHost().getHostName(),
                 connectAddr.getHostName());
  }

  @Test
  public void testCreateSocketAddress() throws Throwable {
    InetSocketAddress addr = NetUtils.createSocketAddr(
        "127.0.0.1:12345", 1000, "myconfig");
    assertEquals("127.0.0.1", addr.getAddress().getHostAddress());
    assertEquals(12345, addr.getPort());
    
    addr = NetUtils.createSocketAddr(
        "127.0.0.1", 1000, "myconfig");
    assertEquals("127.0.0.1", addr.getAddress().getHostAddress());
    assertEquals(1000, addr.getPort());

    try {
      NetUtils.createSocketAddr(
          "127.0.0.1:blahblah", 1000, "myconfig");
      fail("Should have failed to parse bad port");
    } catch (IllegalArgumentException iae) {
      assertInException(iae, "myconfig");
    }
  }

  @Test
  public void testCreateSocketAddressWithURICache() throws Throwable {
    InetSocketAddress addr = NetUtils.createSocketAddr(
        "127.0.0.1:12345", 1000, "myconfig", true);
    assertEquals("127.0.0.1", addr.getAddress().getHostAddress());
    assertEquals(12345, addr.getPort());

    addr = NetUtils.createSocketAddr(
        "127.0.0.1:12345", 1000, "myconfig", true);
    assertEquals("127.0.0.1", addr.getAddress().getHostAddress());
    assertEquals(12345, addr.getPort());

    // ----------------------------------------------------

    addr = NetUtils.createSocketAddr(
        "127.0.0.1", 1000, "myconfig", true);
    assertEquals("127.0.0.1", addr.getAddress().getHostAddress());
    assertEquals(1000, addr.getPort());

    addr = NetUtils.createSocketAddr(
        "127.0.0.1", 1000, "myconfig", true);
    assertEquals("127.0.0.1", addr.getAddress().getHostAddress());
    assertEquals(1000, addr.getPort());

    // ----------------------------------------------------

    try {
      NetUtils.createSocketAddr(
          "127.0.0.1:blahblah", 1000, "myconfig", true);
      fail("Should have failed to parse bad port");
    } catch (IllegalArgumentException iae) {
      assertInException(iae, "myconfig");
    }

    try {
      NetUtils.createSocketAddr(
          "127.0.0.1:blahblah", 1000, "myconfig", true);
      fail("Should have failed to parse bad port");
    } catch (IllegalArgumentException iae) {
      assertInException(iae, "myconfig");
    }
  }

  private void assertRemoteDetailsIncluded(IOException wrapped)
      throws Throwable {
    assertInException(wrapped, "desthost");
    assertInException(wrapped, DEST_PORT_NAME);
  }

  private void assertLocalDetailsIncluded(IOException wrapped)
      throws Throwable {
    assertInException(wrapped, "localhost");
    assertInException(wrapped, LOCAL_PORT_NAME);
  }

  private void assertWikified(Exception e) throws Throwable {
    assertInException(e, NetUtils.HADOOP_WIKI);
  }

  private void assertInException(Exception e, String text) throws Throwable {
    String message = extractExceptionMessage(e);
    if (!(message.contains(text))) {
      throw new AssertionError("Wrong text in message "
        + "\"" + message + "\""
        + " expected \"" + text + "\"")
          .initCause(e);
    }
  }

  private String extractExceptionMessage(Exception e) throws Throwable {
    assertNotNull(e, "Null Exception");
    String message = e.getMessage();
    if (message == null) {
      throw new AssertionError("Empty text in exception " + e)
          .initCause(e);
    }
    return message;
  }

  private void assertNotInException(Exception e, String text)
      throws Throwable{
    String message = extractExceptionMessage(e);
    if (message.contains(text)) {
      throw new AssertionError("Wrong text in message "
           + "\"" + message + "\""
           + " did not expect \"" + text + "\"")
          .initCause(e);
    }
  }

  private IOException verifyExceptionClass(IOException e,
                                           Class expectedClass)
      throws Throwable {
    assertNotNull(e, "Null Exception");
    IOException wrapped = NetUtils.wrapException("desthost", DEST_PORT,
         "localhost", LOCAL_PORT, e);
    LOG.info(wrapped.toString(), wrapped);
    if(!(wrapped.getClass().equals(expectedClass))) {
      throw new AssertionError("Wrong exception class; expected "
         + expectedClass
         + " got " + wrapped.getClass() + ": " + wrapped).initCause(wrapped);
    }
    return wrapped;
  }

  static NetUtilsTestResolver resolver;
  static Configuration config;
  
  @BeforeAll
  public static void setupResolver() {
    resolver = NetUtilsTestResolver.install();
  }
  
  @BeforeEach
  public void resetResolver() {
    resolver.reset();
    config = new Configuration();
  }

  // getByExactName
  
  private void verifyGetByExactNameSearch(String host, String ... searches) {
    assertNull(resolver.getByExactName(host));
    assertBetterArrayEquals(searches, resolver.getHostSearches());
  }

  @Test
  public void testResolverGetByExactNameUnqualified() {
    verifyGetByExactNameSearch("unknown", "unknown.");
  }

  @Test
  public void testResolverGetByExactNameUnqualifiedWithDomain() {
    verifyGetByExactNameSearch("unknown.domain", "unknown.domain.");
  }

  @Test
  public void testResolverGetByExactNameQualified() {
    verifyGetByExactNameSearch("unknown.", "unknown.");
  }
  
  @Test
  public void testResolverGetByExactNameQualifiedWithDomain() {
    verifyGetByExactNameSearch("unknown.domain.", "unknown.domain.");
  }

  // getByNameWithSearch
  
  private void verifyGetByNameWithSearch(String host, String ... searches) {
    assertNull(resolver.getByNameWithSearch(host));
    assertBetterArrayEquals(searches, resolver.getHostSearches());
  }

  @Test
  public void testResolverGetByNameWithSearchUnqualified() {
    String host = "unknown";
    verifyGetByNameWithSearch(host, host+".a.b.", host+".b.", host+".c.");
  }

  @Test
  public void testResolverGetByNameWithSearchUnqualifiedWithDomain() {
    String host = "unknown.domain";
    verifyGetByNameWithSearch(host, host+".a.b.", host+".b.", host+".c.");
  }

  @Test
  public void testResolverGetByNameWithSearchQualified() {
    String host = "unknown.";
    verifyGetByNameWithSearch(host, host);
  }

  @Test
  public void testResolverGetByNameWithSearchQualifiedWithDomain() {
    String host = "unknown.domain.";
    verifyGetByNameWithSearch(host, host);
  }

  // getByName

  private void verifyGetByName(String host, String ... searches) {
    InetAddress addr = null;
    try {
      addr = resolver.getByName(host);
    } catch (UnknownHostException e) {} // ignore
    assertNull(addr);
    assertBetterArrayEquals(searches, resolver.getHostSearches());
  }
  
  @Test
  public void testResolverGetByNameQualified() {
    String host = "unknown.";
    verifyGetByName(host, host);
  }

  @Test
  public void testResolverGetByNameQualifiedWithDomain() {
    verifyGetByName("unknown.domain.", "unknown.domain.");
  }

  @Test
  public void testResolverGetByNameUnqualified() {
    String host = "unknown";
    verifyGetByName(host, host+".a.b.", host+".b.", host+".c.", host+".");
  }

  @Test
  public void testResolverGetByNameUnqualifiedWithDomain() {
    String host = "unknown.domain";
    verifyGetByName(host, host+".", host+".a.b.", host+".b.", host+".c.");
  }
  
  // resolving of hosts

  private InetAddress verifyResolve(String host, String ... searches) {
    InetAddress addr = null;
    try {
      addr = resolver.getByName(host);
    } catch (UnknownHostException e) {} // ignore
    assertNotNull(addr);
    assertBetterArrayEquals(searches, resolver.getHostSearches());
    return addr;
  }

  private void
  verifyInetAddress(InetAddress addr, String host, String ip) {
    assertNotNull(addr);
    assertEquals(host, addr.getHostName());
    assertEquals(ip, addr.getHostAddress());
  }
  
  @Test
  public void testResolverUnqualified() {
    String host = "host";
    InetAddress addr = verifyResolve(host, host+".a.b.");
    verifyInetAddress(addr, "host.a.b", "1.1.1.1");
  }

  @Test
  public void testResolverUnqualifiedWithDomain() {
    String host = "host.a";
    InetAddress addr = verifyResolve(host, host+".", host+".a.b.", host+".b.");
    verifyInetAddress(addr, "host.a.b", "1.1.1.1");
  }

  @Test
  public void testResolverUnqualifedFull() {
    String host = "host.a.b";
    InetAddress addr = verifyResolve(host, host+".");
    verifyInetAddress(addr, host, "1.1.1.1");
  }
  
  @Test
  public void testResolverQualifed() {
    String host = "host.a.b.";
    InetAddress addr = verifyResolve(host, host);
    verifyInetAddress(addr, host, "1.1.1.1");
  }
  
  // localhost
  
  @Test
  public void testResolverLoopback() {
    String host = "Localhost";
    InetAddress addr = verifyResolve(host); // no lookup should occur
    verifyInetAddress(addr, "Localhost", "127.0.0.1");
  }

  @Test
  public void testResolverIP() {
    String host = "1.1.1.1";
    InetAddress addr = verifyResolve(host); // no lookup should occur for ips
    verifyInetAddress(addr, host, host);
  }

  //
    
  @Test
  public void testCanonicalUriWithPort() {
    URI uri;

    uri = NetUtils.getCanonicalUri(URI.create("scheme://host:123"), 456);
    assertEquals("scheme://host.a.b:123", uri.toString());

    uri = NetUtils.getCanonicalUri(URI.create("scheme://host:123/"), 456);
    assertEquals("scheme://host.a.b:123/", uri.toString());

    uri = NetUtils.getCanonicalUri(URI.create("scheme://host:123/path"), 456);
    assertEquals("scheme://host.a.b:123/path", uri.toString());

    uri = NetUtils.getCanonicalUri(URI.create("scheme://host:123/path?q#frag"), 456);
    assertEquals("scheme://host.a.b:123/path?q#frag", uri.toString());
  }

  @Test
  public void testCanonicalUriWithDefaultPort() {
    URI uri;
    
    uri = NetUtils.getCanonicalUri(URI.create("scheme://host"), 123);
    assertEquals("scheme://host.a.b:123", uri.toString());

    uri = NetUtils.getCanonicalUri(URI.create("scheme://host/"), 123);
    assertEquals("scheme://host.a.b:123/", uri.toString());

    uri = NetUtils.getCanonicalUri(URI.create("scheme://host/path"), 123);
    assertEquals("scheme://host.a.b:123/path", uri.toString());

    uri = NetUtils.getCanonicalUri(URI.create("scheme://host/path?q#frag"), 123);
    assertEquals("scheme://host.a.b:123/path?q#frag", uri.toString());
  }

  @Test
  public void testCanonicalUriWithPath() {
    URI uri;

    uri = NetUtils.getCanonicalUri(URI.create("path"), 2);
    assertEquals("path", uri.toString());

    uri = NetUtils.getCanonicalUri(URI.create("/path"), 2);
    assertEquals("/path", uri.toString());
  }

  @Test
  public void testCanonicalUriWithNoAuthority() {
    URI uri;

    uri = NetUtils.getCanonicalUri(URI.create("scheme:/"), 2);
    assertEquals("scheme:/", uri.toString());

    uri = NetUtils.getCanonicalUri(URI.create("scheme:/path"), 2);
    assertEquals("scheme:/path", uri.toString());

    uri = NetUtils.getCanonicalUri(URI.create("scheme:///"), 2);
    assertEquals("scheme:///", uri.toString());

    uri = NetUtils.getCanonicalUri(URI.create("scheme:///path"), 2);
    assertEquals("scheme:///path", uri.toString());
  }

  @Test
  public void testCanonicalUriWithNoHost() {
    URI uri = NetUtils.getCanonicalUri(URI.create("scheme://:123/path"), 2);
    assertEquals("scheme://:123/path", uri.toString());
  }

  @Test
  public void testCanonicalUriWithNoPortNoDefaultPort() {
    URI uri = NetUtils.getCanonicalUri(URI.create("scheme://host/path"), -1);
    assertEquals("scheme://host.a.b/path", uri.toString());
  }
  
  /** 
   * Test for {@link NetUtils#normalizeHostNames}
   */
  @Test
  public void testNormalizeHostName() {
    String oneHost = "1.kanyezone.appspot.com";
    try {
      InetAddress.getByName(oneHost);
    } catch (UnknownHostException e) {
      assumeTrue(false, "Network not resolving " + oneHost);
    }
    List<String> hosts = Arrays.asList("127.0.0.1",
        "localhost", oneHost, "UnknownHost123");
    List<String> normalizedHosts = NetUtils.normalizeHostNames(hosts);
    String summary = "original [" + StringUtils.join(hosts, ", ") + "]"
        + " normalized [" + StringUtils.join(normalizedHosts, ", ") + "]";
    // when ipaddress is normalized, same address is expected in return
    assertEquals(hosts.get(0), normalizedHosts.get(0), summary);
    // for normalizing a resolvable hostname, resolved ipaddress is expected in return
    assertFalse(normalizedHosts.get(1).equals(hosts.get(1)),
        "Element 1 equal "+ summary);
    assertEquals(hosts.get(0), normalizedHosts.get(1), summary);
    // this address HADOOP-8372: when normalizing a valid resolvable hostname start with numeric, 
    // its ipaddress is expected to return
    assertFalse(normalizedHosts.get(2).equals(hosts.get(2)),
        "Element 2 equal " + summary);
    // return the same hostname after normalizing a irresolvable hostname.
    assertEquals(hosts.get(3), normalizedHosts.get(3), summary);
  }
  
  @Test
  public void testGetHostNameOfIP() {
    assertNull(NetUtils.getHostNameOfIP(null));
    assertNull(NetUtils.getHostNameOfIP(""));
    assertNull(NetUtils.getHostNameOfIP("crazytown"));
    assertNull(NetUtils.getHostNameOfIP("127.0.0.1:"));   // no port
    assertNull(NetUtils.getHostNameOfIP("127.0.0.1:-1")); // bogus port
    assertNull(NetUtils.getHostNameOfIP("127.0.0.1:A"));  // bogus port
    assertNotNull(NetUtils.getHostNameOfIP("127.0.0.1"));
    assertNotNull(NetUtils.getHostNameOfIP("127.0.0.1:1"));
  }

  @Test
  public void testTrimCreateSocketAddress() {
    Configuration conf = new Configuration();
    NetUtils.addStaticResolution("host", "127.0.0.1");
    final String defaultAddr = "host:1  ";

    InetSocketAddress addr = NetUtils.createSocketAddr(defaultAddr);
    conf.setSocketAddr("myAddress", addr);
    assertEquals(defaultAddr.trim(), NetUtils.getHostPortString(addr));
  }

  @Test
  public void testGetPortFromHostPortString() throws Exception {

    assertEquals(1002, NetUtils.getPortFromHostPortString("testHost:1002"));

    LambdaTestUtils.intercept(IllegalArgumentException.class,
        () ->  NetUtils.getPortFromHostPortString("testHost"));

    LambdaTestUtils.intercept(IllegalArgumentException.class,
        () ->  NetUtils.getPortFromHostPortString("testHost:randomString"));
  }

  @Test
  public void testBindToLocalAddress() throws Exception {
    assertNotNull(NetUtils
        .bindToLocalAddress(NetUtils.getLocalInetAddress("127.0.0.1"), false));
    assertNull(NetUtils
        .bindToLocalAddress(NetUtils.getLocalInetAddress("127.0.0.1"), true));
  }

  public static class WrappedIOException extends IOException {
    public WrappedIOException(String msg, Throwable cause) {
      super(msg, cause);
    }
  }

  private static class PrivateIOException extends IOException {
    PrivateIOException(String msg, Throwable cause) {
      super(msg, cause);
    }
  }

  @Test
  public void testAddNodeNameToIOException() {
    IOException e0 = new IOException("test123");
    assertNullCause(e0);
    IOException new0 = NetUtils.addNodeNameToIOException(e0, "node123");
    assertNullCause(new0);
    assertEquals("node123: test123", new0.getMessage());

    IOException e1 = new IOException("test456", new IllegalStateException("deliberate"));
    IOException new1 = NetUtils.addNodeNameToIOException(e1, "node456");
    assertSame(e1.getCause(), new1.getCause());
    assertEquals("node456: test456", new1.getMessage());

    ZipException e2 = new ZipException("test789");
    assertNullCause(e2);
    IOException new2 = NetUtils.addNodeNameToIOException(e2, "node789");
    assertNullCause(new2);
    assertEquals("node789: test789", new2.getMessage());

    WrappedIOException e3 = new WrappedIOException("test987",
        new IllegalStateException("deliberate"));
    IOException new3 = NetUtils.addNodeNameToIOException(e3, "node987");
    assertSame(e3.getCause(), new3.getCause());
    assertEquals("node987: test987", new3.getMessage());

    // addNodeNameToIOException will return the original exception if the class is not accessible
    PrivateIOException e4 = new PrivateIOException("test654",
        new IllegalStateException("deliberate"));
    IOException new4 = NetUtils.addNodeNameToIOException(e4, "node654");
    assertSame(e4, new4);
  }

  private <T> void assertBetterArrayEquals(T[] expect, T[]got) {
    String expectStr = StringUtils.join(expect, ", ");
    String gotStr = StringUtils.join(got, ", ");
    assertEquals(expectStr, gotStr);
  }

  private void assertNullCause(Exception e) {
    assertNull(e.getCause(), "Expected exception to have null cause");
  }
}
