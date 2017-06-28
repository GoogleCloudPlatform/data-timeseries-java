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

package com.google.cloud.solutions.samples.timeseries.dataflow.application.workpackets;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.cloud.solutions.samples.timeseries.dataflow.application.proto.TimeSeriesProtos.WorkPacket;
import com.google.cloud.solutions.samples.timeseries.dataflow.application.proto.TimeSeriesProtos.WorkPacketConfig;
import com.google.cloud.solutions.samples.timeseries.dataflow.application.proto.TimeSeriesProtos.WorkPackets;


@SuppressWarnings("serial")
/**
 * Create the work packets if the partition matches this threads partition 
 */
public class CreatePartitionWorkPacketsDoFn extends DoFn<KV<Integer,Iterable<Integer>>, WorkPackets>
    implements DoFn.RequiresWindowAccess, Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(CreatePartitionWorkPacketsDoFn.class);

  WorkPacketConfig workPacketView;
  PCollectionView<Map<String,WorkPacket>> sideInput;
  
  public CreatePartitionWorkPacketsDoFn(WorkPacketConfig workPacketView, PCollectionView<Map<String,WorkPacket>>  sideInput) {
    this.workPacketView = workPacketView;
    this.sideInput=sideInput;
  }

  @Override
  public void processElement(DoFn<KV<Integer,Iterable<Integer>>, WorkPackets>.ProcessContext c)
      throws Exception {

    // Build the map (this is in-memory so we need to take care) of all the data points and then do
    // a Cartesian join between the two

    // TODO Rewrite this so that we are not doing work that is not required for this section
    Map<String, WorkPacket> dataPoints = c.sideInput(sideInput);

    int myPartition = c.element().getKey();

    LOG.info(String.format("I Am dealing with Partition %s ", c.element().getKey()));


    long workdone = 0;

    // Now we build the Cartesian join and emit each pair forward, we ignore self joins and
    // transitive joins {a,b,c} become {a-b,a-c,b-c}

    List<String> transList = new ArrayList<String>();


    for (String outer : dataPoints.keySet()) {

      for (String inner : dataPoints.keySet()) {

        String key = WorkPacketUtils.createKey(outer, inner);
        String reversekey = WorkPacketUtils.createKey(inner, outer);

        if (!outer.equals(inner) && !transList.contains(key)) {

          // Only do work if this key combination hashs to this workers partition

          if (WorkPacketUtils.getMyPartitions(workPacketView, key) == myPartition) {
            workdone++;
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
    LOG.info(String.format("Work Done by partition %s was %s", myPartition, workdone));
  }
}
