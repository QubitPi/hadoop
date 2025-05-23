/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.registry.server.dns;

import org.apache.commons.net.util.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.registry.client.api.RegistryConstants;
import org.apache.hadoop.registry.client.binding.RegistryUtils;
import org.apache.hadoop.registry.client.types.ServiceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.xbill.DNS.AAAARecord;
import org.xbill.DNS.ARecord;
import org.xbill.DNS.CNAMERecord;
import org.xbill.DNS.DClass;
import org.xbill.DNS.DNSKEYRecord;
import org.xbill.DNS.DNSSEC;
import org.xbill.DNS.Flags;
import org.xbill.DNS.Message;
import org.xbill.DNS.Name;
import org.xbill.DNS.OPTRecord;
import org.xbill.DNS.PTRRecord;
import org.xbill.DNS.RRSIGRecord;
import org.xbill.DNS.RRset;
import org.xbill.DNS.Rcode;
import org.xbill.DNS.Record;
import org.xbill.DNS.SRVRecord;
import org.xbill.DNS.Section;
import org.xbill.DNS.Type;

import java.io.IOException;
import java.math.BigInteger;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.security.KeyFactory;
import java.security.PrivateKey;
import java.security.spec.RSAPrivateKeySpec;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.registry.client.api.RegistryConstants.*;

/**
 *
 */
public class TestRegistryDNS extends Assertions {

  private RegistryDNS registryDNS;
  private RegistryUtils.ServiceRecordMarshal marshal;

  private static final String APPLICATION_RECORD = "{\n"
      + "  \"type\" : \"JSONServiceRecord\",\n"
      + "  \"description\" : \"Slider Application Master\",\n"
      + "  \"external\" : [ {\n"
      + "    \"api\" : \"classpath:org.apache.hadoop.yarn.service.appmaster.ipc"
      + "\",\n"
      + "    \"addressType\" : \"host/port\",\n"
      + "    \"protocolType\" : \"hadoop/IPC\",\n"
      + "    \"addresses\" : [ {\n"
      + "      \"host\" : \"192.168.1.5\",\n"
      + "      \"port\" : \"1026\"\n"
      + "    } ]\n"
      + "  }, {\n"
      + "    \"api\" : \"http://\",\n"
      + "    \"addressType\" : \"uri\",\n"
      + "    \"protocolType\" : \"webui\",\n"
      + "    \"addresses\" : [ {\n"
      + "      \"uri\" : \"http://192.168.1.5:1027\"\n"
      + "    } ]\n"
      + "  }, {\n"
      + "    \"api\" : \"classpath:org.apache.hadoop.yarn.service.management\""
      + ",\n"
      + "    \"addressType\" : \"uri\",\n"
      + "    \"protocolType\" : \"REST\",\n"
      + "    \"addresses\" : [ {\n"
      + "      \"uri\" : \"http://192.168.1.5:1027/ws/v1/slider/mgmt\"\n"
      + "    } ]\n"
      + "  } ],\n"
      + "  \"internal\" : [ {\n"
      + "    \"api\" : \"classpath:org.apache.hadoop.yarn.service.agents.secure"
      + "\",\n"
      + "    \"addressType\" : \"uri\",\n"
      + "    \"protocolType\" : \"REST\",\n"
      + "    \"addresses\" : [ {\n"
      + "      \"uri\" : \"https://192.168.1.5:47700/ws/v1/slider/agents\"\n"
      + "    } ]\n"
      + "  }, {\n"
      + "    \"api\" : \"classpath:org.apache.hadoop.yarn.service.agents.oneway"
      + "\",\n"
      + "    \"addressType\" : \"uri\",\n"
      + "    \"protocolType\" : \"REST\",\n"
      + "    \"addresses\" : [ {\n"
      + "      \"uri\" : \"https://192.168.1.5:35531/ws/v1/slider/agents\"\n"
      + "    } ]\n"
      + "  } ],\n"
      + "  \"yarn:id\" : \"application_1451931954322_0016\",\n"
      + "  \"yarn:persistence\" : \"application\"\n"
      + "}\n";
  static final String CONTAINER_RECORD = "{\n"
      + "  \"type\" : \"JSONServiceRecord\",\n"
      + "  \"description\" : \"httpd-1\",\n"
      + "  \"external\" : [ ],\n"
      + "  \"internal\" : [ ],\n"
      + "  \"yarn:id\" : \"container_e50_1451931954322_0016_01_000002\",\n"
      + "  \"yarn:persistence\" : \"container\",\n"
      + "  \"yarn:ip\" : \"172.17.0.19\",\n"
      + "  \"yarn:hostname\" : \"host1\",\n"
      + "  \"yarn:component\" : \"httpd\"\n"
      + "}\n";

  static final String CONTAINER_RECORD2 = "{\n"
      + "  \"type\" : \"JSONServiceRecord\",\n"
      + "  \"description\" : \"httpd-2\",\n"
      + "  \"external\" : [ ],\n"
      + "  \"internal\" : [ ],\n"
      + "  \"yarn:id\" : \"container_e50_1451931954322_0016_01_000003\",\n"
      + "  \"yarn:persistence\" : \"container\",\n"
      + "  \"yarn:ip\" : \"172.17.0.20\",\n"
      + "  \"yarn:hostname\" : \"host2\",\n"
      + "  \"yarn:component\" : \"httpd\"\n"
      + "}\n";

  private static final String CONTAINER_RECORD_NO_IP = "{\n"
      + "  \"type\" : \"JSONServiceRecord\",\n"
      + "  \"description\" : \"httpd-1\",\n"
      + "  \"external\" : [ ],\n"
      + "  \"internal\" : [ ],\n"
      + "  \"yarn:id\" : \"container_e50_1451931954322_0016_01_000002\",\n"
      + "  \"yarn:persistence\" : \"container\",\n"
      + "  \"yarn:component\" : \"httpd\"\n"
      + "}\n";

  private static final String CONTAINER_RECORD_YARN_PERSISTANCE_ABSENT = "{\n"
      + "  \"type\" : \"JSONServiceRecord\",\n"
      + "  \"description\" : \"httpd-1\",\n"
      + "  \"external\" : [ ],\n"
      + "  \"internal\" : [ ],\n"
      + "  \"yarn:id\" : \"container_e50_1451931954322_0016_01_000003\",\n"
      + "  \"yarn:ip\" : \"172.17.0.19\",\n"
      + "  \"yarn:hostname\" : \"0a134d6329bb\",\n"
      + "  \"yarn:component\" : \"httpd\""
      + "}\n";

  @BeforeEach
  public void initialize() throws Exception {
    setRegistryDNS(new RegistryDNS("TestRegistry"));
    Configuration conf = createConfiguration();

    getRegistryDNS().setDomainName(conf);
    getRegistryDNS().initializeZones(conf);

    setMarshal(new RegistryUtils.ServiceRecordMarshal());
  }

  protected Configuration createConfiguration() {
    Configuration conf = new Configuration();
    conf.set(RegistryConstants.KEY_DNS_DOMAIN, "dev.test");
    conf.set(RegistryConstants.KEY_DNS_ZONE_SUBNET, "172.17.0");
    conf.setTimeDuration(RegistryConstants.KEY_DNS_TTL, 30L, TimeUnit.SECONDS);
    return conf;
  }

  protected boolean isSecure() {
    return false;
  }

  @AfterEach
  public void closeRegistry() throws Exception {
    getRegistryDNS().stopExecutor();
  }

  @Test
  public void testAppRegistration() throws Exception {
    ServiceRecord record = getMarshal().fromBytes("somepath",
        APPLICATION_RECORD.getBytes());
    getRegistryDNS().register(
        "/registry/users/root/services/org-apache-slider/test1/", record);

    // start assessing whether correct records are available
    List<Record> recs = assertDNSQuery("test1.root.dev.test.");
    assertEquals("192.168.1.5",
        ((ARecord) recs.get(0)).getAddress().getHostAddress(), "wrong result");

    recs = assertDNSQuery("management-api.test1.root.dev.test.", 2);
    assertEquals("test1.root.dev.test.",
        ((CNAMERecord) recs.get(0)).getTarget().toString(), "wrong target name");
    assertTrue(recs.get(isSecure() ? 2 : 1) instanceof ARecord, "not an ARecord");

    recs = assertDNSQuery("appmaster-ipc-api.test1.root.dev.test.",
        Type.SRV, 1);
    assertTrue(recs.get(0) instanceof SRVRecord, "not an SRV record");
    assertEquals(1026, ((SRVRecord) recs.get(0)).getPort(), "wrong port");

    recs = assertDNSQuery("appmaster-ipc-api.test1.root.dev.test.", 2);
    assertEquals("test1.root.dev.test.",
        ((CNAMERecord) recs.get(0)).getTarget().toString(), "wrong target name");
    assertTrue(recs.get(isSecure() ? 2 : 1) instanceof ARecord, "not an ARecord");

    recs = assertDNSQuery("http-api.test1.root.dev.test.", 2);
    assertEquals("test1.root.dev.test.",
        ((CNAMERecord) recs.get(0)).getTarget().toString(), "wrong target name");
    assertTrue(recs.get(isSecure() ? 2 : 1) instanceof ARecord, "not an ARecord");

    recs = assertDNSQuery("http-api.test1.root.dev.test.", Type.SRV,
        1);
    assertTrue(recs.get(0) instanceof SRVRecord, "not an SRV record");
    assertEquals(1027, ((SRVRecord) recs.get(0)).getPort(), "wrong port");

    assertDNSQuery("test1.root.dev.test.", Type.TXT, 3);
    assertDNSQuery("appmaster-ipc-api.test1.root.dev.test.", Type.TXT, 1);
    assertDNSQuery("http-api.test1.root.dev.test.", Type.TXT, 1);
    assertDNSQuery("management-api.test1.root.dev.test.", Type.TXT, 1);
  }

  @Test
  public void testContainerRegistration() throws Exception {
    ServiceRecord record = getMarshal().fromBytes("somepath",
        CONTAINER_RECORD.getBytes());
    getRegistryDNS().register(
        "/registry/users/root/services/org-apache-slider/test1/components/"
            + "ctr-e50-1451931954322-0016-01-000002",
        record);

    // start assessing whether correct records are available
    List<Record> recs =
        assertDNSQuery("ctr-e50-1451931954322-0016-01-000002.dev.test.");
    assertEquals("172.17.0.19",
        ((ARecord) recs.get(0)).getAddress().getHostAddress(), "wrong result");

    recs = assertDNSQuery("httpd-1.test1.root.dev.test.", 1);
    assertTrue(recs.get(0) instanceof ARecord, "not an ARecord");
  }

  @Test
  public void testContainerRegistrationPersistanceAbsent() throws Exception {
    ServiceRecord record = marshal.fromBytes("somepath",
        CONTAINER_RECORD_YARN_PERSISTANCE_ABSENT.getBytes());
    registryDNS.register(
        "/registry/users/root/services/org-apache-slider/test1/components/"
            + "ctr-e50-1451931954322-0016-01-000003",
         record);

    Name name =
        Name.fromString("ctr-e50-1451931954322-0016-01-000002.dev.test.");
    Record question = Record.newRecord(name, Type.A, DClass.IN);
    Message query = Message.newQuery(question);
    byte[] responseBytes = registryDNS.generateReply(query, null);
    Message response = new Message(responseBytes);
    assertEquals(Rcode.NXDOMAIN, response.getRcode(),
        "Excepting NXDOMAIN as Record must not have regsisterd wrong");
  }

  @Test
  public void testRecordTTL() throws Exception {
    ServiceRecord record = getMarshal().fromBytes("somepath",
        CONTAINER_RECORD.getBytes());
    getRegistryDNS().register(
        "/registry/users/root/services/org-apache-slider/test1/components/"
            + "ctr-e50-1451931954322-0016-01-000002",
        record);

    // start assessing whether correct records are available
    List<Record> recs = assertDNSQuery(
        "ctr-e50-1451931954322-0016-01-000002.dev.test.");
    assertEquals("172.17.0.19",
        ((ARecord) recs.get(0)).getAddress().getHostAddress(), "wrong result");
    assertEquals(30L, recs.get(0).getTTL(), "wrong ttl");

    recs = assertDNSQuery("httpd-1.test1.root.dev.test.", 1);
    assertTrue(recs.get(0) instanceof ARecord, "not an ARecord");

    assertEquals(30L, recs.get(0).getTTL(), "wrong ttl");
  }

  @Test
  public void testReverseLookup() throws Exception {
    ServiceRecord record = getMarshal().fromBytes("somepath",
        CONTAINER_RECORD.getBytes());
    getRegistryDNS().register(
        "/registry/users/root/services/org-apache-slider/test1/components/"
            + "ctr-e50-1451931954322-0016-01-000002",
        record);

    // start assessing whether correct records are available
    List<Record> recs = assertDNSQuery(
        "19.0.17.172.in-addr.arpa.", Type.PTR, 1);
    assertEquals("httpd-1.test1.root.dev.test.",
        ((PTRRecord) recs.get(0)).getTarget().toString(), "wrong result");
  }

  @Test
  public void testReverseLookupInLargeNetwork() throws Exception {
    setRegistryDNS(new RegistryDNS("TestRegistry"));
    Configuration conf = createConfiguration();
    conf.set(RegistryConstants.KEY_DNS_DOMAIN, "dev.test");
    conf.set(KEY_DNS_ZONE_SUBNET, "172.17.0.0");
    conf.set(KEY_DNS_ZONE_MASK, "255.255.224.0");
    conf.setTimeDuration(RegistryConstants.KEY_DNS_TTL, 30L, TimeUnit.SECONDS);

    getRegistryDNS().setDomainName(conf);
    getRegistryDNS().initializeZones(conf);

    ServiceRecord record = getMarshal().fromBytes("somepath",
        CONTAINER_RECORD.getBytes());
    getRegistryDNS().register(
        "/registry/users/root/services/org-apache-slider/test1/components/"
            + "ctr-e50-1451931954322-0016-01-000002",
        record);

    // start assessing whether correct records are available
    List<Record> recs = assertDNSQuery(
        "19.0.17.172.in-addr.arpa.", Type.PTR, 1);
    assertEquals("httpd-1.test1.root.dev.test.",
        ((PTRRecord) recs.get(0)).getTarget().toString(), "wrong result");
  }

  @Test
  public void testMissingReverseLookup() throws Exception {
    ServiceRecord record = getMarshal().fromBytes("somepath",
        CONTAINER_RECORD.getBytes());
    getRegistryDNS().register(
        "/registry/users/root/services/org-apache-slider/test1/components/"
            + "ctr-e50-1451931954322-0016-01-000002",
        record);

    // start assessing whether correct records are available
    Name name = Name.fromString("19.1.17.172.in-addr.arpa.");
    Record question = Record.newRecord(name, Type.PTR, DClass.IN);
    Message query = Message.newQuery(question);
    OPTRecord optRecord = new OPTRecord(4096, 0, 0, Flags.DO);
    query.addRecord(optRecord, Section.ADDITIONAL);
    byte[] responseBytes = getRegistryDNS().generateReply(query, null);
    Message response = new Message(responseBytes);
    assertEquals(Rcode.NXDOMAIN, response.getRcode(),
        "Missing record should be: ");
  }

  @Test
  public void testNoContainerIP() throws Exception {
    ServiceRecord record = getMarshal().fromBytes("somepath",
        CONTAINER_RECORD_NO_IP.getBytes());
    getRegistryDNS().register(
        "/registry/users/root/services/org-apache-slider/test1/components/"
            + "ctr-e50-1451931954322-0016-01-000002",
        record);

    // start assessing whether correct records are available
    Name name =
        Name.fromString("ctr-e50-1451931954322-0016-01-000002.dev.test.");
    Record question = Record.newRecord(name, Type.A, DClass.IN);
    Message query = Message.newQuery(question);

    byte[] responseBytes = getRegistryDNS().generateReply(query, null);
    Message response = new Message(responseBytes);
    assertEquals(Rcode.NXDOMAIN, response.getRcode(), "wrong status");
  }

  private List<Record> assertDNSQuery(String lookup) throws IOException {
    return assertDNSQuery(lookup, Type.A, 1);
  }

  private List<Record> assertDNSQuery(String lookup, int numRecs)
      throws IOException {
    return assertDNSQuery(lookup, Type.A, numRecs);
  }

  private List<Record> assertDNSQuery(String lookup, int type, int numRecs)
      throws IOException {
    Name name = Name.fromString(lookup);
    Record question = Record.newRecord(name, type, DClass.IN);
    Message query = Message.newQuery(question);
    OPTRecord optRecord = new OPTRecord(4096, 0, 0, Flags.DO);
    query.addRecord(optRecord, Section.ADDITIONAL);
    byte[] responseBytes = getRegistryDNS().generateReply(query, null);
    Message response = new Message(responseBytes);
    assertEquals(Rcode.NOERROR, response.getRcode(), "not successful");
    assertNotNull(response, "Null response");
    assertEquals(query.getQuestion(),
        response.getQuestion(), "Questions do not match");
    List<Record> recs = response.getSection(Section.ANSWER);
    assertEquals(isSecure() ? numRecs * 2 : numRecs, recs.size(),
        "wrong number of answer records");
    if (isSecure()) {
      boolean signed = false;
      for (Record record : recs) {
        signed = record.getType() == Type.RRSIG;
        if (signed) {
          break;
        }
      }
      assertTrue(signed, "No signatures found");
    }
    return recs;
  }

  private List<Record> assertDNSQueryNotNull(
      String lookup, int type, int answerCount) throws IOException {
    Name name = Name.fromString(lookup);
    Record question = Record.newRecord(name, type, DClass.IN);
    Message query = Message.newQuery(question);
    OPTRecord optRecord = new OPTRecord(4096, 0, 0, Flags.DO);
    query.addRecord(optRecord, Section.ADDITIONAL);
    byte[] responseBytes = getRegistryDNS().generateReply(query, null);
    Message response = new Message(responseBytes);
    assertEquals(Rcode.NOERROR, response.getRcode(), "not successful");
    assertNotNull(response, "Null response");
    assertEquals(query.getQuestion(), response.getQuestion(),
        "Questions do not match");
    List<Record> recs = response.getSection(Section.ANSWER);
    assertEquals(answerCount, recs.size());
    assertEquals(type, recs.get(0).getType());
    return recs;
  }

  @Test
  public void testDNSKEYRecord() throws Exception {
    String publicK =
        "AwEAAe1Jev0Az1khlQCvf0nud1/CNHQwwPEu8BNchZthdDxKPVn29yrD "
            + "CHoAWjwiGsOSw3SzIPrawSbHzyJsjn0oLBhGrH6QedFGnydoxjNsw3m/ "
            + "SCmOjR/a7LGBAMDFKqFioi4gOyuN66svBeY+/5uw72+0ei9AQ20gqf6q "
            + "l9Ozs5bV";
    //    byte[] publicBytes = Base64.decodeBase64(publicK);
    //    X509EncodedKeySpec keySpec = new X509EncodedKeySpec(publicBytes);
    //    KeyFactory keyFactory = KeyFactory.getInstance("RSA");
    //    PublicKey pubKey = keyFactory.generatePublic(keySpec);
    DNSKEYRecord dnskeyRecord =
        new DNSKEYRecord(Name.fromString("dev.test."), DClass.IN, 0,
            DNSKEYRecord.Flags.ZONE_KEY,
            DNSKEYRecord.Protocol.DNSSEC,
            DNSSEC.Algorithm.RSASHA256,
            Base64.decodeBase64(publicK.getBytes()));
    assertNotNull(dnskeyRecord);
    RSAPrivateKeySpec privateSpec = new RSAPrivateKeySpec(new BigInteger(1,
        Base64.decodeBase64(
            "7Ul6/QDPWSGVAK9/Se53X8I0dDDA8S7wE1yFm2F0PEo9Wfb3KsMIegBaPCIaw5LDd"
                + "LMg+trBJsfPImyOfSgsGEasfpB50UafJ2jGM2zDeb9IKY6NH9rssYEAwMUq"
                + "oWKiLiA7K43rqy8F5j7/m7Dvb7R6L0BDbSCp/qqX07OzltU=")),
        new BigInteger(1, Base64.decodeBase64(
            "MgbQ6DBYhskeufNGGdct0cGG/4wb0X183ggenwCv2dopDyOTPq+5xMb4Pz9Ndzgk/"
                + "yCY7mpaWIu9rttGOzrR+LBRR30VobPpMK1bMnzu2C0x08oYAguVwZB79DLC"
                + "705qmZpiaaFB+LnhG7VtpPiOBm3UzZxdrBfeq/qaKrXid60=")));
    KeyFactory factory = KeyFactory.getInstance("RSA");
    PrivateKey priv = factory.generatePrivate(privateSpec);

    ARecord aRecord = new ARecord(Name.fromString("some.test."), DClass.IN, 0,
        InetAddress.getByName("192.168.0.1"));
    Instant inception = Instant.now();
    Instant expiration = inception.plus(365, ChronoUnit.DAYS);
    RRset rrset = new RRset(aRecord);
    RRSIGRecord rrsigRecord = DNSSEC.sign(rrset,
        dnskeyRecord,
        priv,
        inception,
        expiration);
    DNSSEC.verify(rrset, rrsigRecord, dnskeyRecord);

  }

  @Test
  public void testIpv4toIpv6() throws Exception {
    InetAddress address =
        BaseServiceRecordProcessor
            .getIpv6Address(InetAddress.getByName("172.17.0.19"));
    assertTrue(address instanceof Inet6Address, "not an ipv6 address");
    assertEquals("172.17.0.19",
        InetAddress.getByAddress(address.getAddress()).getHostAddress(), "wrong IP");
  }

  @Test
  public void testAAAALookup() throws Exception {
    ServiceRecord record = getMarshal().fromBytes("somepath",
        CONTAINER_RECORD.getBytes());
    getRegistryDNS().register(
        "/registry/users/root/services/org-apache-slider/test1/components/"
            + "ctr-e50-1451931954322-0016-01-000002",
        record);

    // start assessing whether correct records are available
    List<Record> recs = assertDNSQuery(
        "ctr-e50-1451931954322-0016-01-000002.dev.test.", Type.AAAA, 1);
    assertEquals("172.17.0.19",
        ((AAAARecord) recs.get(0)).getAddress().getHostAddress(), "wrong result");

    recs = assertDNSQuery("httpd-1.test1.root.dev.test.", Type.AAAA, 1);
    assertTrue(recs.get(0) instanceof AAAARecord, "not an ARecord");
  }

  @Test
  public void testNegativeLookup() throws Exception {
    ServiceRecord record = getMarshal().fromBytes("somepath",
        CONTAINER_RECORD.getBytes());
    getRegistryDNS().register(
        "/registry/users/root/services/org-apache-slider/test1/components/"
            + "ctr-e50-1451931954322-0016-01-000002",
        record);

    // start assessing whether correct records are available
    Name name = Name.fromString("missing.dev.test.");
    Record question = Record.newRecord(name, Type.A, DClass.IN);
    Message query = Message.newQuery(question);

    byte[] responseBytes = getRegistryDNS().generateReply(query, null);
    Message response = new Message(responseBytes);
    assertEquals(Rcode.NXDOMAIN, response.getRcode(), "not successful");
    assertNotNull(response, "Null response");
    assertEquals(query.getQuestion(),
        response.getQuestion(), "Questions do not match");
    List<Record> sectionArray = response.getSection(Section.AUTHORITY);
    assertEquals(isSecure() ? 2 : 1,
        sectionArray.size(), "Wrong number of recs in AUTHORITY");
    boolean soaFound = false;
    for (Record rec : sectionArray) {
      soaFound = rec.getType() == Type.SOA;
      if (soaFound) {
        break;
      }
    }
    assertTrue(soaFound, "wrong record type");

  }

  @Test
  public void testReadMasterFile() throws Exception {
    setRegistryDNS(new RegistryDNS("TestRegistry"));
    Configuration conf = new Configuration();
    conf.set(RegistryConstants.KEY_DNS_DOMAIN, "dev.test");
    conf.set(RegistryConstants.KEY_DNS_ZONE_SUBNET, "172.17.0");
    conf.setTimeDuration(RegistryConstants.KEY_DNS_TTL, 30L, TimeUnit.SECONDS);
    conf.set(RegistryConstants.KEY_DNS_ZONES_DIR,
        getClass().getResource("/").getFile());
    if (isSecure()) {
      conf.setBoolean(RegistryConstants.KEY_DNSSEC_ENABLED, true);
      conf.set(RegistryConstants.KEY_DNSSEC_PUBLIC_KEY,
          "AwEAAe1Jev0Az1khlQCvf0nud1/CNHQwwPEu8BNchZthdDxKPVn29yrD "
              + "CHoAWjwiGsOSw3SzIPrawSbHzyJsjn0oLBhGrH6QedFGnydoxjNsw3m/ "
              + "SCmOjR/a7LGBAMDFKqFioi4gOyuN66svBeY+/5uw72+0ei9AQ20gqf6q "
              + "l9Ozs5bV");
      conf.set(RegistryConstants.KEY_DNSSEC_PRIVATE_KEY_FILE,
          getClass().getResource("/test.private").getFile());
    }

    getRegistryDNS().setDomainName(conf);
    getRegistryDNS().initializeZones(conf);

    ServiceRecord record = getMarshal().fromBytes("somepath",
        CONTAINER_RECORD.getBytes());
    getRegistryDNS().register(
        "/registry/users/root/services/org-apache-slider/test1/components/"
            + "ctr-e50-1451931954322-0016-01-000002",
        record);

    // start assessing whether correct records are available
    List<Record> recs =
        assertDNSQuery("ctr-e50-1451931954322-0016-01-000002.dev.test.");
    assertEquals("172.17.0.19",
        ((ARecord) recs.get(0)).getAddress().getHostAddress(), "wrong result");

    recs = assertDNSQuery("httpd-1.test1.root.dev.test.", 1);
    assertTrue(recs.get(0) instanceof ARecord, "not an ARecord");

    // lookup dyanmic reverse records
    recs = assertDNSQuery("19.0.17.172.in-addr.arpa.", Type.PTR, 1);
    assertEquals(
        "httpd-1.test1.root.dev.test.",
        ((PTRRecord) recs.get(0)).getTarget().toString(), "wrong result");

    // now lookup static reverse records
    Name name = Name.fromString("5.0.17.172.in-addr.arpa.");
    Record question = Record.newRecord(name, Type.PTR, DClass.IN);
    Message query = Message.newQuery(question);
    OPTRecord optRecord = new OPTRecord(4096, 0, 0, Flags.DO);
    query.addRecord(optRecord, Section.ADDITIONAL);
    byte[] responseBytes = getRegistryDNS().generateReply(query, null);
    Message response = new Message(responseBytes);
    recs = response.getSection(Section.ANSWER);
    assertEquals("cn005.dev.test.",
        ((PTRRecord) recs.get(0)).getTarget().toString(), "wrong result");
  }

  @Test
  public void testReverseZoneNames() throws Exception {
    Configuration conf = new Configuration();
    conf.set(KEY_DNS_ZONE_SUBNET, "172.26.32.0");
    conf.set(KEY_DNS_ZONE_MASK, "255.255.224.0");

    Name name = getRegistryDNS().getReverseZoneName(conf);
    assertEquals("26.172.in-addr.arpa.", name.toString(), "wrong name");
  }

  @Test
  public void testSplitReverseZoneNames() throws Exception {
    Configuration conf = new Configuration();
    registryDNS = new RegistryDNS("TestRegistry");
    conf.set(RegistryConstants.KEY_DNS_DOMAIN, "example.com");
    conf.set(KEY_DNS_SPLIT_REVERSE_ZONE, "true");
    conf.set(KEY_DNS_SPLIT_REVERSE_ZONE_RANGE, "256");
    conf.set(KEY_DNS_ZONE_SUBNET, "172.26.32.0");
    conf.set(KEY_DNS_ZONE_MASK, "255.255.224.0");
    conf.setTimeDuration(RegistryConstants.KEY_DNS_TTL, 30L, TimeUnit.SECONDS);
    conf.set(RegistryConstants.KEY_DNS_ZONES_DIR,
        getClass().getResource("/").getFile());
    if (isSecure()) {
      conf.setBoolean(RegistryConstants.KEY_DNSSEC_ENABLED, true);
      conf.set(RegistryConstants.KEY_DNSSEC_PUBLIC_KEY,
          "AwEAAe1Jev0Az1khlQCvf0nud1/CNHQwwPEu8BNchZthdDxKPVn29yrD "
              + "CHoAWjwiGsOSw3SzIPrawSbHzyJsjn0oLBhGrH6QedFGnydoxjNsw3m/ "
              + "SCmOjR/a7LGBAMDFKqFioi4gOyuN66svBeY+/5uw72+0ei9AQ20gqf6q "
              + "l9Ozs5bV");
      conf.set(RegistryConstants.KEY_DNSSEC_PRIVATE_KEY_FILE,
          getClass().getResource("/test.private").getFile());
    }
    registryDNS.setDomainName(conf);
    registryDNS.setDNSSECEnabled(conf);
    registryDNS.addSplitReverseZones(conf, 4);
    assertEquals(4, registryDNS.getZoneCount());
  }

  @Test
  public void testExampleDotCom() throws Exception {
    Name name = Name.fromString("example.com.");
    Record[] records = getRegistryDNS().getRecords(name, Type.SOA);
    assertNotNull(records, "example.com exists:");
  }

  @Test
  public void testExternalCNAMERecord() throws Exception {
    setRegistryDNS(new RegistryDNS("TestRegistry"));
    Configuration conf = new Configuration();
    conf.set(RegistryConstants.KEY_DNS_DOMAIN, "dev.test");
    conf.set(RegistryConstants.KEY_DNS_ZONE_SUBNET, "172.17.0");
    conf.setTimeDuration(RegistryConstants.KEY_DNS_TTL, 30L, TimeUnit.SECONDS);
    conf.set(RegistryConstants.KEY_DNS_ZONES_DIR,
        getClass().getResource("/").getFile());
    getRegistryDNS().setDomainName(conf);
    getRegistryDNS().initializeZones(conf);

    // start assessing whether correct records are available
    assertDNSQueryNotNull("mail.yahoo.com.", Type.CNAME, 1);
  }

  @Test
  public void testRootLookup() throws Exception {
    setRegistryDNS(new RegistryDNS("TestRegistry"));
    Configuration conf = new Configuration();
    conf.set(RegistryConstants.KEY_DNS_DOMAIN, "dev.test");
    conf.set(RegistryConstants.KEY_DNS_ZONE_SUBNET, "172.17.0");
    conf.setTimeDuration(RegistryConstants.KEY_DNS_TTL, 30L, TimeUnit.SECONDS);
    conf.set(RegistryConstants.KEY_DNS_ZONES_DIR,
        getClass().getResource("/").getFile());
    getRegistryDNS().setDomainName(conf);
    getRegistryDNS().initializeZones(conf);

    // start assessing whether correct records are available
    assertDNSQueryNotNull(".", Type.NS, 13);
  }

  @Test
  public void testMultiARecord() throws Exception {
    ServiceRecord record = getMarshal().fromBytes("somepath",
        CONTAINER_RECORD.getBytes());
    ServiceRecord record2 = getMarshal().fromBytes("somepath",
        CONTAINER_RECORD2.getBytes());
    getRegistryDNS().register(
        "/registry/users/root/services/org-apache-slider/test1/components/"
            + "ctr-e50-1451931954322-0016-01-000002",
        record);
    getRegistryDNS().register(
        "/registry/users/root/services/org-apache-slider/test1/components/"
            + "ctr-e50-1451931954322-0016-01-000003",
        record2);

    // start assessing whether correct records are available
    List<Record> recs =
        assertDNSQuery("httpd.test1.root.dev.test.", 2);
    assertTrue(recs.get(0) instanceof ARecord, "not an ARecord");
    assertTrue(recs.get(1) instanceof ARecord, "not an ARecord");
  }

  @Test
  @Timeout(value = 5)
  public void testUpstreamFault() throws Exception {
    Name name = Name.fromString("19.0.17.172.in-addr.arpa.");
    Record[] recs = getRegistryDNS().getRecords(name, Type.CNAME);
    assertNull(recs, "Record is not null");
  }

  public RegistryDNS getRegistryDNS() {
    return registryDNS;
  }

  public void setRegistryDNS(
      RegistryDNS registryDNS) {
    this.registryDNS = registryDNS;
  }

  public RegistryUtils.ServiceRecordMarshal getMarshal() {
    return marshal;
  }

  public void setMarshal(
      RegistryUtils.ServiceRecordMarshal marshal) {
    this.marshal = marshal;
  }
}
