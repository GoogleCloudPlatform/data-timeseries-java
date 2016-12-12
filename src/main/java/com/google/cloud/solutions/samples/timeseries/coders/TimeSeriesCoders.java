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

package com.google.cloud.solutions.samples.timeseries.coders;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.protobuf.ProtoCoder;
import com.google.cloud.solutions.samples.timeseries.proto.TimeSeriesProtos.Correlation;
import com.google.cloud.solutions.samples.timeseries.proto.TimeSeriesProtos.TSAggValueProto;
import com.google.cloud.solutions.samples.timeseries.proto.TimeSeriesProtos.TSProto;
import com.google.cloud.solutions.samples.timeseries.proto.TimeSeriesProtos.WorkDataPoint;
import com.google.cloud.solutions.samples.timeseries.proto.TimeSeriesProtos.WorkPacketConfig;
import com.google.cloud.solutions.samples.timeseries.proto.TimeSeriesProtos.WorkPacketKey;



/**
 * Register all coders used by this pipeline
 *
 */
public class TimeSeriesCoders {
  private static final Logger LOG = LoggerFactory.getLogger(TimeSeriesCoders.class);

  public static void registerCoders(Pipeline pipeline) {

    LOG.debug("Register TSProto coder");
    pipeline.getCoderRegistry().registerCoder(TSProto.class, ProtoCoder.of(TSProto.class));

    LOG.debug("Register TSAggValueProto coder");
    pipeline.getCoderRegistry().registerCoder(TSAggValueProto.class,
        ProtoCoder.of(TSAggValueProto.class));
    LOG.debug("Register WorkPacketConfig coder");
    pipeline.getCoderRegistry().registerCoder(WorkPacketConfig.class,
        ProtoCoder.of(WorkPacketConfig.class));
    LOG.debug("Register WorkPacketKey coder");
    pipeline.getCoderRegistry().registerCoder(WorkPacketKey.class,
        ProtoCoder.of(WorkPacketKey.class));
    LOG.debug("Register WorkDataPoint coder");
    pipeline.getCoderRegistry().registerCoder(WorkDataPoint.class,
        ProtoCoder.of(WorkDataPoint.class));
    LOG.debug("Register Correlation coder");
    pipeline.getCoderRegistry().registerCoder(Correlation.class, ProtoCoder.of(Correlation.class));

  }
}
