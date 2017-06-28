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
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import com.google.cloud.solutions.samples.timeseries.dataflow.application.proto.TimeSeriesProtos.WorkDataPoint;
import com.google.cloud.solutions.samples.timeseries.dataflow.application.proto.TimeSeriesProtos.WorkPacket;
import com.google.cloud.solutions.samples.timeseries.dataflow.application.proto.TimeSeriesProtos.WorkPacketConfig;


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
    DoFn<KV<String, Iterable<WorkDataPoint>>, Map<String, WorkPacket>> implements
    DoFn.RequiresWindowAccess, Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(CreateWorkPacketsDoFn.class);

  WorkPacketConfig workPacketView;
  TupleTag<Integer> counter;

  public CreateWorkPacketsDoFn(WorkPacketConfig workPacketView, TupleTag<Integer> counter) {
    this.workPacketView = workPacketView;
    this.counter = counter;
  }

  @Override
  public void processElement(
      DoFn<KV<String, Iterable<WorkDataPoint>>, Map<String, WorkPacket>>.ProcessContext c)
      throws Exception {

    // Discard Key as it is no longer of interest, it only holds the window info used to make sure the content does not always land on one machine
    
    // Build the map (this is in-memory so we need to take care) of all the data points and then do
    // a Cartesian join between the two

    Map<String, WorkPacket.Builder> dataPoints = new HashMap<>();

    for (WorkDataPoint data : c.element().getValue()) {

    	String key = data.getKey();
    	
      // If the List does not yet contain the Key then add it else just append
      if (!dataPoints.containsKey(key)) {

        WorkPacket.Builder builder = WorkPacket.newBuilder();

        builder.setKey(key);
        builder.addDataPoints(data);

        dataPoints.put(key, builder);

      } else {

        dataPoints.get(key).addDataPoints(data);
      }
    }
    
    Map<String, WorkPacket> builtDataPoints = new HashMap<>();

    for (String key : dataPoints.keySet()){
      builtDataPoints.put(key, dataPoints.get(key).build());
    }
    
    c.output(builtDataPoints);
    
    // This value should generate 1 counter per window
    c.sideOutput(counter, 1);
  }
}
