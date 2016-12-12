/*
 * Copyright (C) 2016 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.solutions.samples.timeseries.workpackets;

import java.io.Serializable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.cloud.solutions.samples.timeseries.proto.TimeSeriesProtos.TSAggValueProto;
import com.google.cloud.solutions.samples.timeseries.proto.TimeSeriesProtos.TSProto;
import com.google.cloud.solutions.samples.timeseries.proto.TimeSeriesProtos.WorkDataPoint;
import com.google.cloud.solutions.samples.timeseries.proto.TimeSeriesProtos.WorkPacketConfig;
import com.google.cloud.solutions.samples.timeseries.proto.TimeSeriesProtos.WorkPartition;


/**
 * This transform has two functions:
 * 1- Re-Attach timestamp based on maximum value in candle
 * 2- Create copies of the data based on the number of partitions desired for parallel distribution 
 */
@SuppressWarnings("serial")
public class DistributeWorkDataDoFn extends
    DoFn<KV<String, TSAggValueProto>, KV<WorkPartition, WorkDataPoint>> implements
    DoFn.RequiresWindowAccess, Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(DistributeWorkDataDoFn.class);

  WorkPacketConfig workPacketView;

  public DistributeWorkDataDoFn(WorkPacketConfig workPacketView) {
    this.workPacketView = workPacketView;
  }

  @Override
  public void processElement(
      DoFn<KV<String, TSAggValueProto>, KV<WorkPartition, WorkDataPoint>>.ProcessContext c)
      throws Exception {

    // Extract only the needed data to do the work and create Proto

    TSProto openFX = c.element().getValue().getOpenState();
    TSProto closeFX = c.element().getValue().getCloseState();

    double diff = closeFX.getAskPrice() / openFX.getAskPrice();

    double value = Math.log(diff);

    WorkDataPoint data =
        WorkDataPoint.newBuilder().setKey(c.element().getKey()).setTime(closeFX.getTime())
            .setValue(value).build();


    WorkPacketConfig partitionSpace =
        WorkPacketUtils.CreateOrderedList(workPacketView.getKeysList(),
            workPacketView.getPartitionLength());

    for (int i = 0; i < partitionSpace.getPartitionLength(); i++) {

      WorkPartition partition = WorkPartition.newBuilder().setPartition(i).build();

      KV<WorkPartition, WorkDataPoint> bars = KV.of(partition, data);
      
      c.outputWithTimestamp(bars, c.timestamp());

    }

  }
}
