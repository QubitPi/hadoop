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
package org.apache.hadoop.hdfs.server.balancer;

import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MutableGaugeInt;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;

/**
 * Metrics for individual Balancer.
 */
@Metrics(about="Balancer metrics", context="dfs")
final class BalancerMetrics {

  private final Balancer balancer;

  @Metric("If a balancer iterate is running")
  private MutableGaugeInt iterateRunning;

  @Metric("Bytes left to move to make cluster balanced")
  private MutableGaugeLong bytesLeftToMove;

  @Metric("Number of under utilized nodes")
  private MutableGaugeInt numOfUnderUtilizedNodes;

  @Metric("Number of over utilized nodes")
  private MutableGaugeInt numOfOverUtilizedNodes;

  @Metric(value = {"BlockPoolID", "Current BlockPoolID"}, type = Metric.Type.TAG)
  public String getBlockPoolID() {
    return balancer.getNnc().getBlockpoolID();
  }

  private BalancerMetrics(Balancer b) {
    this.balancer = b;
  }

  public static BalancerMetrics create(Balancer b) {
    BalancerMetrics m = new BalancerMetrics(b);
    return DefaultMetricsSystem.instance().register(
        m.getName(), null, m);
  }

  String getName() {
    return "Balancer-" + balancer.getNnc().getBlockpoolID();
  }

  @Metric("Bytes that already moved in current doBalance run.")
  public long getBytesMovedInCurrentRun() {
    return balancer.getNnc().getBytesMoved().get();
  }

  void setIterateRunning(boolean iterateRunning) {
    this.iterateRunning.set(iterateRunning ? 1 : 0);
  }

  void setBytesLeftToMove(long bytesLeftToMove) {
    this.bytesLeftToMove.set(bytesLeftToMove);
  }

  void setNumOfUnderUtilizedNodes(int numOfUnderUtilizedNodes) {
    this.numOfUnderUtilizedNodes.set(numOfUnderUtilizedNodes);
  }

  void setNumOfOverUtilizedNodes(int numOfOverUtilizedNodes) {
    this.numOfOverUtilizedNodes.set(numOfOverUtilizedNodes);
  }
}