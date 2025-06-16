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

package org.apache.hadoop.fs.s3a.statistics;

import java.util.Arrays;
import java.util.Collection;

import org.apache.hadoop.fs.s3a.statistics.impl.StatisticsFromAwsSdkImpl;
import org.apache.hadoop.test.AbstractHadoopTestBase;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static org.apache.hadoop.fs.s3a.impl.InternalConstants.SC_400_BAD_REQUEST;
import static org.apache.hadoop.fs.s3a.impl.InternalConstants.SC_404_NOT_FOUND;
import static org.apache.hadoop.fs.s3a.impl.InternalConstants.SC_429_TOO_MANY_REQUESTS_GCS;
import static org.apache.hadoop.fs.s3a.impl.InternalConstants.SC_500_INTERNAL_SERVER_ERROR;
import static org.apache.hadoop.fs.s3a.impl.InternalConstants.SC_503_SERVICE_UNAVAILABLE;
import static org.apache.hadoop.fs.s3a.statistics.impl.StatisticsFromAwsSdkImpl.mapErrorStatusCodeToStatisticName;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.HTTP_RESPONSE_400;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.HTTP_RESPONSE_4XX;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.HTTP_RESPONSE_500;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.HTTP_RESPONSE_503;
import static org.apache.hadoop.fs.statistics.StoreStatisticNames.HTTP_RESPONSE_5XX;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test mapping logic of {@link StatisticsFromAwsSdkImpl}.
 */
public class TestErrorCodeMapping extends AbstractHadoopTestBase {

  /**
   * Parameterization.
   */
  public static Collection<Object[]> params() {
    return Arrays.asList(new Object[][]{
        {200, null},
        {302, null},
        {SC_400_BAD_REQUEST, HTTP_RESPONSE_400},
        {SC_404_NOT_FOUND, null},
        {416, HTTP_RESPONSE_4XX},
        {SC_429_TOO_MANY_REQUESTS_GCS, HTTP_RESPONSE_503},
        {SC_500_INTERNAL_SERVER_ERROR, HTTP_RESPONSE_500},
        {SC_503_SERVICE_UNAVAILABLE, HTTP_RESPONSE_503},
        {510, HTTP_RESPONSE_5XX},
    });
  }

  private int code;

  private String name;

  public void initTestErrorCodeMapping(final int pCode, final String pName) {
    this.code = pCode;
    this.name = pName;
  }

  @ParameterizedTest(name = "http {0} to {1}")
  @MethodSource("params")
  public void testMapping(int pCode, String pName) throws Throwable {
    initTestErrorCodeMapping(pCode, pName);
    assertThat(mapErrorStatusCodeToStatisticName(code))
        .describedAs("Mapping of status code %d", code)
        .isEqualTo(name);
  }
}
