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

package org.apache.hadoop.fs.tosfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.DelegateToFileSystem;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * The implementation class of the raw AbstractFileSystem. If you want to use object storage as
 * YARN’s resource storage dir via the fs.defaultFS configuration property in Hadoop’s
 * core-site.xml, you should add this configuration to Hadoop's core-site.xml.
 * <pre>
 *  fs.AbstractFileSystem.{scheme}.impl=io.proton.fs.RawFS.
 * </pre>
 */
public class RawFS extends DelegateToFileSystem {
  private static final int TOS_DEFAULT_PORT = -1;

  public RawFS(URI uri, Configuration conf) throws IOException, URISyntaxException {
    super(uri, new RawFileSystem(), conf, uri.getScheme(), false);
  }

  @Override
  public int getUriDefaultPort() {
    return TOS_DEFAULT_PORT;
  }
}
