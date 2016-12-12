/*
 * Copyright (C) 2016 Google Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.solutions.samples.timeseries.workpackets;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.solutions.samples.timeseries.proto.TimeSeriesProtos.WorkPacketConfig;


/**
 * Utilities used for work packet creation
 */
public class WorkPacketUtils {

  private static final Logger LOG = LoggerFactory.getLogger(WorkPacketUtils.class);


  // What we want to do is take a 2D matrix and partition it into work units of size x
  public static WorkPacketConfig CreateOrderedList(List<String> keys, int numPartitions) {
    return WorkPacketConfig.newBuilder().addAllKeys(keys)
        .setPartitionLength(numPartitions).build();
  }


  public static int getMyPartitions(WorkPacketConfig partitionSpace, String key) {

    // Hash to a partition space
    int i = Math.abs(key.hashCode() % partitionSpace.getPartitionLength());

    return i;
  }

  /**
   * This methods simply concatenates the two values together. In this pipeline if the string value
   * of key1==key2 then the results are also ==.
   * 
   * @param key1
   * @param key2
   * @return Concatenated string of Key1 and Key2 with delimiter "::"
   */
  public static String createKey(String key1, String key2) {
    if(key1.compareTo(key2)<=0){
      return String.format("%s :: %s", key1, key2);     
    }else{
      return String.format("%s :: %s", key2, key1);
    }
 
  }

}
