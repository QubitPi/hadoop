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

package org.apache.hadoop.hdfs.web;

import static org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod.KERBEROS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.WebHdfs;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenSecretManager;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.web.resources.DelegationParam;
import org.apache.hadoop.hdfs.web.resources.DoAsParam;
import org.apache.hadoop.hdfs.web.resources.GetOpParam;
import org.apache.hadoop.hdfs.web.resources.PutOpParam;
import org.apache.hadoop.hdfs.web.resources.StartAfterParam;
import org.apache.hadoop.hdfs.web.resources.TokenArgumentParam;
import org.apache.hadoop.hdfs.web.resources.UserParam;
import org.apache.hadoop.hdfs.web.resources.FsActionParam;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

public class TestWebHdfsUrl {
  // NOTE: port is never used 
  final URI uri = URI.create(WebHdfsConstants.WEBHDFS_SCHEME + "://" + "127.0.0.1:0");

  @BeforeEach
  public void resetUGI() {
    UserGroupInformation.setConfiguration(new Configuration());
  }
  
  @Test
  @Timeout(value = 60)
  public void testEncodedPathUrl() throws IOException, URISyntaxException{
    Configuration conf = new Configuration();

    final WebHdfsFileSystem webhdfs = (WebHdfsFileSystem) FileSystem.get(
        uri, conf);

    // Construct a file path that contains percentage-encoded string
    String pathName = "/hdtest010%2C60020%2C1371000602151.1371058984668+";
    Path fsPath = new Path(pathName);
    URL encodedPathUrl = webhdfs.toUrl(PutOpParam.Op.CREATE, fsPath);
    // We should get back the original file path after cycling back and decoding
    assertEquals(WebHdfsFileSystem.PATH_PREFIX + pathName,
        encodedPathUrl.toURI().getPath());
  }

  @Test
  @Timeout(value = 60)
  public void testSimpleAuthParamsInUrl() throws IOException {
    Configuration conf = new Configuration();

    UserGroupInformation ugi =
        UserGroupInformation.createRemoteUser("test-user");
    UserGroupInformation.setLoginUser(ugi);

    WebHdfsFileSystem webhdfs = getWebHdfsFileSystem(ugi, conf);
    Path fsPath = new Path("/");

    // send user+token
    URL fileStatusUrl = webhdfs.toUrl(GetOpParam.Op.GETFILESTATUS, fsPath);
    checkQueryParams(
        new String[]{
            GetOpParam.Op.GETFILESTATUS.toQueryString(),
            new UserParam(ugi.getShortUserName()).toString()
        },
        fileStatusUrl);
  }

  @Test
  @Timeout(value = 60)
  public void testSimpleProxyAuthParamsInUrl() throws IOException {
    Configuration conf = new Configuration();

    UserGroupInformation ugi =
        UserGroupInformation.createRemoteUser("test-user");
    ugi = UserGroupInformation.createProxyUser("test-proxy-user", ugi);
    UserGroupInformation.setLoginUser(ugi);

    WebHdfsFileSystem webhdfs = getWebHdfsFileSystem(ugi, conf);
    Path fsPath = new Path("/");

    // send real+effective
    URL fileStatusUrl = webhdfs.toUrl(GetOpParam.Op.GETFILESTATUS, fsPath);
    checkQueryParams(
        new String[]{
            GetOpParam.Op.GETFILESTATUS.toQueryString(),
            new UserParam(ugi.getRealUser().getShortUserName()).toString(),
            new DoAsParam(ugi.getShortUserName()).toString()
    },
        fileStatusUrl);
  }

  @Test
  @Timeout(value = 60)
  public void testSecureAuthParamsInUrl() throws IOException {
    Configuration conf = new Configuration();
    // fake turning on security so api thinks it should use tokens
    SecurityUtil.setAuthenticationMethod(KERBEROS, conf);
    UserGroupInformation.setConfiguration(conf);

    UserGroupInformation ugi =
        UserGroupInformation.createRemoteUser("test-user");
    ugi.setAuthenticationMethod(KERBEROS);
    UserGroupInformation.setLoginUser(ugi);

    WebHdfsFileSystem webhdfs = getWebHdfsFileSystem(ugi, conf);
    Path fsPath = new Path("/");
    String tokenString = webhdfs.getDelegationToken().encodeToUrlString();

    String userParam = new UserParam(ugi.getShortUserName()).toString();

    // send user
    URL getTokenUrl = webhdfs.toUrl(GetOpParam.Op.GETDELEGATIONTOKEN, fsPath);
    assertTrue(getTokenUrl.toString().indexOf(userParam) == -1,
        "secure webhdfs SHOULD NOT use user.name parameter");
    checkQueryParams(
        new String[]{
            GetOpParam.Op.GETDELEGATIONTOKEN.toQueryString(),
        },
        getTokenUrl);



    // send user
    URL renewTokenUrl = webhdfs.toUrl(PutOpParam.Op.RENEWDELEGATIONTOKEN,
        fsPath, new TokenArgumentParam(tokenString));
    assertTrue(renewTokenUrl.toString().indexOf(userParam) == -1,
        "secure webhdfs SHOULD NOT use user.name parameter");
    checkQueryParams(
        new String[]{
            PutOpParam.Op.RENEWDELEGATIONTOKEN.toQueryString(),
            new TokenArgumentParam(tokenString).toString(),
        },
        renewTokenUrl);



    // send token
    URL cancelTokenUrl = webhdfs.toUrl(PutOpParam.Op.CANCELDELEGATIONTOKEN,
        fsPath, new TokenArgumentParam(tokenString));
    assertTrue(cancelTokenUrl.toString().indexOf(userParam) == -1,
        "secure webhdfs SHOULD NOT use user.name parameter");
    checkQueryParams(
        new String[]{
            PutOpParam.Op.CANCELDELEGATIONTOKEN.toQueryString(),
            new TokenArgumentParam(tokenString).toString(),
        },
        cancelTokenUrl);


    // send token
    URL fileStatusUrl = webhdfs.toUrl(GetOpParam.Op.GETFILESTATUS, fsPath);
    checkQueryParams(
        new String[]{
            GetOpParam.Op.GETFILESTATUS.toQueryString(),
            new DelegationParam(tokenString).toString()
        },
        fileStatusUrl);

    // wipe out internal token to simulate auth always required
    webhdfs.setDelegationToken(null);

    // send user
    cancelTokenUrl = webhdfs.toUrl(PutOpParam.Op.CANCELDELEGATIONTOKEN,
        fsPath, new TokenArgumentParam(tokenString));
    assertTrue(cancelTokenUrl.toString().indexOf(userParam) == -1,
        "secure webhdfs SHOULD NOT use user.name parameter");
    checkQueryParams(
        new String[]{
            PutOpParam.Op.CANCELDELEGATIONTOKEN.toQueryString(),
            new TokenArgumentParam(tokenString).toString(),
        },
        cancelTokenUrl);


    // send user
    fileStatusUrl = webhdfs.toUrl(GetOpParam.Op.GETFILESTATUS, fsPath);
    checkQueryParams(
        new String[]{
            GetOpParam.Op.GETFILESTATUS.toQueryString(),
            new DelegationParam(tokenString).toString()
        },
        fileStatusUrl);    
  }

  @Test
  @Timeout(value = 60)
  public void testSecureProxyAuthParamsInUrl() throws IOException {
    Configuration conf = new Configuration();
    // fake turning on security so api thinks it should use tokens
    SecurityUtil.setAuthenticationMethod(KERBEROS, conf);
    UserGroupInformation.setConfiguration(conf);

    UserGroupInformation ugi =
        UserGroupInformation.createRemoteUser("test-user");
    ugi.setAuthenticationMethod(KERBEROS);
    ugi = UserGroupInformation.createProxyUser("test-proxy-user", ugi);
    UserGroupInformation.setLoginUser(ugi);

    WebHdfsFileSystem webhdfs = getWebHdfsFileSystem(ugi, conf);
    Path fsPath = new Path("/");
    String tokenString = webhdfs.getDelegationToken().encodeToUrlString();

    String userParam = new UserParam(ugi.getRealUser().
        getShortUserName()).toString();

    // send real+effective
    URL getTokenUrl = webhdfs.toUrl(GetOpParam.Op.GETDELEGATIONTOKEN, fsPath);
    assertTrue(getTokenUrl.toString().indexOf(userParam) == -1,
        "secure webhdfs SHOULD NOT use user.name parameter");
    checkQueryParams(
        new String[]{
            GetOpParam.Op.GETDELEGATIONTOKEN.toQueryString(),
            new DoAsParam(ugi.getShortUserName()).toString()
        },
        getTokenUrl);



    // send real+effective
    URL renewTokenUrl = webhdfs.toUrl(PutOpParam.Op.RENEWDELEGATIONTOKEN,
        fsPath, new TokenArgumentParam(tokenString));
    assertTrue(renewTokenUrl.toString().indexOf(userParam) == -1,
        "secure webhdfs SHOULD NOT use user.name parameter");
    checkQueryParams(
        new String[]{
            PutOpParam.Op.RENEWDELEGATIONTOKEN.toQueryString(),
            new DoAsParam(ugi.getShortUserName()).toString(),
            new TokenArgumentParam(tokenString).toString(),
        },
        renewTokenUrl);


    // send token
    URL cancelTokenUrl = webhdfs.toUrl(PutOpParam.Op.CANCELDELEGATIONTOKEN,
        fsPath, new TokenArgumentParam(tokenString));
    assertTrue(cancelTokenUrl.toString().indexOf(userParam) == -1,
        "secure webhdfs SHOULD NOT use user.name parameter");
    checkQueryParams(
        new String[]{
            PutOpParam.Op.CANCELDELEGATIONTOKEN.toQueryString(),
            new DoAsParam(ugi.getShortUserName()).toString(),
            new TokenArgumentParam(tokenString).toString(),
        },
        cancelTokenUrl);


    // send token
    URL fileStatusUrl = webhdfs.toUrl(GetOpParam.Op.GETFILESTATUS, fsPath);
    checkQueryParams(
        new String[]{
            GetOpParam.Op.GETFILESTATUS.toQueryString(),
            new DelegationParam(tokenString).toString()
        },
        fileStatusUrl);

    // wipe out internal token to simulate auth always required
    webhdfs.setDelegationToken(null);
    
    // send real+effective
    cancelTokenUrl = webhdfs.toUrl(PutOpParam.Op.CANCELDELEGATIONTOKEN,
        fsPath, new TokenArgumentParam(tokenString));
    assertTrue(cancelTokenUrl.toString().indexOf(userParam) == -1,
        "secure webhdfs SHOULD NOT use user.name parameter");
    checkQueryParams(
        new String[]{
            PutOpParam.Op.CANCELDELEGATIONTOKEN.toQueryString(),
            new DoAsParam(ugi.getShortUserName()).toString(),
            new TokenArgumentParam(tokenString).toString()
        },
        cancelTokenUrl);


    // send real+effective
    fileStatusUrl = webhdfs.toUrl(GetOpParam.Op.GETFILESTATUS, fsPath);
    checkQueryParams(
        new String[]{
            GetOpParam.Op.GETFILESTATUS.toQueryString(),
            new DelegationParam(tokenString).toString()
        },
        fileStatusUrl);    
  }

  @Test
  @Timeout(value = 60)
  public void testCheckAccessUrl() throws IOException {
    Configuration conf = new Configuration();

    UserGroupInformation ugi =
        UserGroupInformation.createRemoteUser("test-user");
    UserGroupInformation.setLoginUser(ugi);

    WebHdfsFileSystem webhdfs = getWebHdfsFileSystem(ugi, conf);
    Path fsPath = new Path("/p1");

    URL checkAccessUrl = webhdfs.toUrl(GetOpParam.Op.CHECKACCESS,
        fsPath, new FsActionParam(FsAction.READ_WRITE));
    checkQueryParams(
        new String[]{
            GetOpParam.Op.CHECKACCESS.toQueryString(),
            new UserParam(ugi.getShortUserName()).toString(),
            FsActionParam.NAME + "=" + FsAction.READ_WRITE.SYMBOL
        },
        checkAccessUrl);
  }

  @Test
  @Timeout(value = 60)
  public void testBatchedListingUrl() throws Exception {
    Configuration conf = new Configuration();

    UserGroupInformation ugi =
        UserGroupInformation.createRemoteUser("test-user");
    UserGroupInformation.setLoginUser(ugi);

    WebHdfsFileSystem webhdfs = getWebHdfsFileSystem(ugi, conf);
    Path fsPath = new Path("/p1");

    final StartAfterParam startAfter =
        new StartAfterParam("last");
    URL url = webhdfs.toUrl(GetOpParam.Op.LISTSTATUS_BATCH,
        fsPath, startAfter);
    checkQueryParams(
        new String[]{
            GetOpParam.Op.LISTSTATUS_BATCH.toQueryString(),
            new UserParam(ugi.getShortUserName()).toString(),
            StartAfterParam.NAME + "=" + "last"
        },
        url);
  }
  
  private void checkQueryParams(String[] expected, URL url) {
    Arrays.sort(expected);
    String[] query = url.getQuery().split("&");
    Arrays.sort(query);
    assertEquals(Arrays.toString(expected), Arrays.toString(query));
  }

  private WebHdfsFileSystem getWebHdfsFileSystem(UserGroupInformation ugi,
      Configuration conf) throws IOException {
    if (UserGroupInformation.isSecurityEnabled()) {
      DelegationTokenIdentifier dtId = new DelegationTokenIdentifier(new Text(
          ugi.getUserName()), null, null);
      FSNamesystem namesystem = mock(FSNamesystem.class);
      DelegationTokenSecretManager dtSecretManager = new DelegationTokenSecretManager(
          86400000, 86400000, 86400000, 86400000, namesystem);
      dtSecretManager.startThreads();
      Token<DelegationTokenIdentifier> token = new Token<DelegationTokenIdentifier>(
          dtId, dtSecretManager);
      SecurityUtil.setTokenService(
          token, NetUtils.createSocketAddr(uri.getAuthority()));
      token.setKind(WebHdfsConstants.WEBHDFS_TOKEN_KIND);
      ugi.addToken(token);
    }
    return (WebHdfsFileSystem) FileSystem.get(uri, conf);
  }

  private static final String SPECIAL_CHARACTER_FILENAME =
          "specialFile ?\"\\()[]_-=&+;,{}#%'`~!@$^*|<>.";

  @Test
  public void testWebHdfsSpecialCharacterFile() throws Exception {
    UserGroupInformation ugi =
            UserGroupInformation.createRemoteUser("test-user");
    ugi.setAuthenticationMethod(KERBEROS);
    UserGroupInformation.setLoginUser(ugi);

    final Configuration conf = WebHdfsTestUtil.createConf();
    final Path dir = new Path("/testWebHdfsSpecialCharacterFile");

    final short numDatanodes = 1;
    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
            .numDataNodes(numDatanodes)
            .build();
    try {
      cluster.waitActive();
      final FileSystem fs = WebHdfsTestUtil
              .getWebHdfsFileSystem(conf, WebHdfs.SCHEME);

      //create a file
      final long length = 1L << 10;
      final Path file1 = new Path(dir, SPECIAL_CHARACTER_FILENAME);

      DFSTestUtil.createFile(fs, file1, length, numDatanodes, 20120406L);

      //get file status and check that it was written properly.
      final FileStatus s1 = fs.getFileStatus(file1);
      assertEquals(length, s1.getLen(), "Write failed for file " + file1);

      boolean found = false;
      RemoteIterator<LocatedFileStatus> statusRemoteIterator =
              fs.listFiles(dir, false);
      while (statusRemoteIterator.hasNext()) {
        LocatedFileStatus locatedFileStatus = statusRemoteIterator.next();
        if (locatedFileStatus.isFile() &&
                SPECIAL_CHARACTER_FILENAME
                        .equals(locatedFileStatus.getPath().getName())) {
          found = true;
        }
      }
      assertFalse(!found, "Could not find file with special character");
    } finally {
      cluster.shutdown();
    }
  }

  private static final String BACKWARD_COMPATIBLE_SPECIAL_CHARACTER_FILENAME =
          "specialFile ?\"\\()[]_-=&,{}#'`~!@$^*|<>.+%";

  @Test
  public void testWebHdfsBackwardCompatibleSpecialCharacterFile()
          throws Exception {
    UserGroupInformation ugi =
            UserGroupInformation.createRemoteUser("test-user");
    ugi.setAuthenticationMethod(KERBEROS);
    UserGroupInformation.setLoginUser(ugi);

    final Configuration conf = WebHdfsTestUtil.createConf();
    final Path dir = new Path("/testWebHdfsSpecialCharacterFile");

    final short numDatanodes = 1;
    final MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf)
            .numDataNodes(numDatanodes)
            .build();
    try {
      cluster.waitActive();
      final FileSystem fs = WebHdfsTestUtil
              .getWebHdfsFileSystem(conf, WebHdfs.SCHEME);

      //create a file
      final long length = 1L << 10;
      final Path file1 = new Path(dir,
              BACKWARD_COMPATIBLE_SPECIAL_CHARACTER_FILENAME);

      DFSTestUtil.createFile(fs, file1, length, numDatanodes, 20120406L);

      //get file status and check that it was written properly.
      final FileStatus s1 = fs.getFileStatus(file1);
      assertEquals(length, s1.getLen(), "Write failed for file " + file1);

      boolean found = false;
      RemoteIterator<LocatedFileStatus> statusRemoteIterator =
              fs.listFiles(dir, false);
      while (statusRemoteIterator.hasNext()) {
        LocatedFileStatus locatedFileStatus = statusRemoteIterator.next();
        if (locatedFileStatus.isFile() &&
                BACKWARD_COMPATIBLE_SPECIAL_CHARACTER_FILENAME
                        .equals(locatedFileStatus.getPath().getName())) {
          found = true;
        }
      }
      assertFalse(!found, "Could not find file with special character");
    } finally {
      cluster.shutdown();
    }
  }

  @Test
  public void testWebHdfsUrlEncoding() throws Exception {
    final WebHdfsFileSystem fs =
        (WebHdfsFileSystem) FileSystem.get(uri, WebHdfsTestUtil.createConf());

    // characters which should not be urlencoded.
    final String unreserved = "_-!.~'()*";
    final String punct = ",:$&=";
    String path = "/testWebHdfsUrlEncoding" + unreserved + punct;
    URL url =
        WebHdfsTestUtil.toUrl(fs, GetOpParam.Op.LISTSTATUS, new Path(path));
    WebHdfsTestUtil.LOG.info(url.getPath());
    assertEquals(WebHdfsFileSystem.PATH_PREFIX + path, url.getPath());
  }

  @Test
  public void testWebHdfsPathWithSemicolon() throws Exception {
    try (MiniDFSCluster cluster =
        new MiniDFSCluster.Builder(WebHdfsTestUtil.createConf())
            .numDataNodes(1)
            .build()) {
      cluster.waitActive();

      // regression test for HDFS-14423.
      final Path semicolon = new Path("/a;b");
      final Path plus = new Path("/a+b");
      final Path percent = new Path("/a%b");

      final WebHdfsFileSystem webhdfs = WebHdfsTestUtil.getWebHdfsFileSystem(
          cluster.getConfiguration(0), WebHdfs.SCHEME);
      webhdfs.create(semicolon).close();
      webhdfs.create(plus).close();
      webhdfs.create(percent).close();

      final DistributedFileSystem dfs = cluster.getFileSystem();
      assertEquals(semicolon.getName(),
          dfs.getFileStatus(semicolon).getPath().getName());
      assertEquals(plus.getName(),
          dfs.getFileStatus(plus).getPath().getName());
      assertEquals(percent.getName(),
          dfs.getFileStatus(percent).getPath().getName());
    }
  }
}
