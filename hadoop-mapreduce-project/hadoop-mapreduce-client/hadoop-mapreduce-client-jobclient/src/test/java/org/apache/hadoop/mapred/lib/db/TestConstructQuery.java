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
package org.apache.hadoop.mapred.lib.db;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class TestConstructQuery {
  private String[] fieldNames = new String[] { "id", "name", "value" };
  private String[] nullFieldNames = new String[] { null, null, null };
  private String expected = "INSERT INTO hadoop_output (id,name,value) VALUES (?,?,?);";
  private String nullExpected = "INSERT INTO hadoop_output VALUES (?,?,?);"; 
  
  private DBOutputFormat<DBWritable, NullWritable> format
    = new DBOutputFormat<DBWritable, NullWritable>();
  @Test
  public void testConstructQuery() {
    String actual = format.constructQuery("hadoop_output", fieldNames);
    assertEquals(expected, actual);

    actual = format.constructQuery("hadoop_output", nullFieldNames);
    assertEquals(nullExpected, actual);
  }
  @Test
  public void testSetOutput() throws IOException {
    JobConf job = new JobConf();
    DBOutputFormat.setOutput(job, "hadoop_output", fieldNames);
    
    DBConfiguration dbConf = new DBConfiguration(job);
    String actual = format.constructQuery(dbConf.getOutputTableName()
        , dbConf.getOutputFieldNames());
    
    assertEquals(expected, actual);
    
    job = new JobConf();
    dbConf = new DBConfiguration(job);
    DBOutputFormat.setOutput(job, "hadoop_output", nullFieldNames.length);
    assertNull(dbConf.getOutputFieldNames());
    assertEquals(nullFieldNames.length, dbConf.getOutputFieldCount());
    
    actual = format.constructQuery(dbConf.getOutputTableName()
        , new String[dbConf.getOutputFieldCount()]);
    
    assertEquals(nullExpected, actual);
  }
  
}
