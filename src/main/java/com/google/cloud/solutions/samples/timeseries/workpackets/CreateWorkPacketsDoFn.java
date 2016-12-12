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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.cloud.solutions.samples.timeseries.proto.TimeSeriesProtos.WorkDataPoint;
import com.google.cloud.solutions.samples.timeseries.proto.TimeSeriesProtos.WorkPacket;
import com.google.cloud.solutions.samples.timeseries.proto.TimeSeriesProtos.WorkPacketConfig;
import com.google.cloud.solutions.samples.timeseries.proto.TimeSeriesProtos.WorkPackets;
import com.google.cloud.solutions.samples.timeseries.proto.TimeSeriesProtos.WorkPartition;


@SuppressWarnings("serial")
/**
 * Creates a series of work packets based on the matrix of aggregated values in each window
 * For the set of values {{key1,v1},{key1,v2},{key2,v3},{key2,v4}} we will output
 * {key1-key2,{v1,v2},{v3,v4}}
 * We need to be careful of memory in this class as we do create a List containing all the data
 * points per key coupled with other keys. 
 * 
 */
public class CreateWorkPacketsDoFn extends
    DoFn<KV<WorkPartition, Iterable<WorkDataPoint>>, WorkPackets> implements
    DoFn.RequiresWindowAccess, Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(CreateWorkPacketsDoFn.class);

  WorkPacketConfig workPacketView;

  public CreateWorkPacketsDoFn(WorkPacketConfig workPacketView) {
    this.workPacketView = workPacketView;
  }

  @Override
  public void processElement(
      DoFn<KV<WorkPartition, Iterable<WorkDataPoint>>, WorkPackets>.ProcessContext c)
      throws Exception {

     // Build the map (this is in-memory so we need to take care) of all the data points and then do
     // a cartiesian join between the two

    Map<String, WorkPacket.Builder> dataPoints = new HashMap<>();

    int myPartition = c.element().getKey().getPartition();

    for (WorkDataPoint data : c.element().getValue()) {

      // If the List does not yet contain the Key then add it else just append
      if (!dataPoints.containsKey(data.getKey())) {

        List<WorkPacket.Builder> list = new ArrayList<>();
        WorkPacket.Builder builder = WorkPacket.newBuilder();

        builder.setKey(data.getKey());
        builder.addDataPoints(data);

        dataPoints.put(data.getKey(), builder);

      } else {

        dataPoints.get(data.getKey()).addDataPoints(data);
      }
    }



     // Now we build the cartisian join and emit each pair forward, we ignore self joins and
     // transitive joins {a,b,c} become {a-b,a-c,b-c}

    List<String> transList = new ArrayList<String>();

    for (String outer : dataPoints.keySet()) {

      for (String inner : dataPoints.keySet()) {

        String key = WorkPacketUtils.createKey(outer, inner);
        String reversekey = WorkPacketUtils.createKey(inner, outer);
        
        if (!outer.equals(inner) && !transList.contains(key)) {

           // Only do work if this key combination hashs to this workers partition
          
          if (WorkPacketUtils.getMyPartitions(workPacketView, key) == myPartition) {
            
            WorkPackets.Builder packets = WorkPackets.newBuilder();
            packets.addWorkPackets(dataPoints.get(outer));
            packets.addWorkPackets(dataPoints.get(inner));
            
            // Add the reverse to the list of keys to ignore
            transList.add(reversekey);
            c.outputWithTimestamp(packets.build(), c.timestamp());
          }
        }
      }
    }
  }
}
