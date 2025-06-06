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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.tosfs.object.tos.TOS;
import org.apache.hadoop.util.Preconditions;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class TosFS extends RawFS {
  public TosFS(URI uri, Configuration conf) throws IOException, URISyntaxException {
    super(verifyURI(uri, conf), conf);
  }

  private static URI verifyURI(URI uri, Configuration conf) {
    Preconditions.checkNotNull(uri);

    String scheme = uri.getScheme();
    if (scheme == null || scheme.isEmpty()) {
      scheme = FileSystem.getDefaultUri(conf).getScheme();
    }
    Preconditions.checkArgument(scheme.equals(TOS.TOS_SCHEME),
        "Unsupported scheme %s, expected scheme is %s.", scheme, TOS.TOS_SCHEME);

    return uri;
  }
}
