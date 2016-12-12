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

package com.google.cloud.solutions.samples.timeseries.aggregation;

import java.io.Serializable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.dataflow.sdk.transforms.Combine.KeyedCombineFn;
import com.google.cloud.solutions.samples.timeseries.proto.TimeSeriesProtos.TSAggValueProto;
import com.google.cloud.solutions.samples.timeseries.proto.TimeSeriesProtos.TSProto;



@SuppressWarnings("serial")
/**
 * Build aggregations for each key per window that provides the: 
 * Min / Max values seen within the window
 * First / Last values seen within this window (based on timestamp) 
 *
 */
public class PartialTimeSeriesAggCombiner extends
    KeyedCombineFn<String, TSProto, TSAggValueProto, TSAggValueProto> implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(PartialTimeSeriesAggCombiner.class);

  @Override
  public TSAggValueProto createAccumulator(String key) {
    return TSAggValueProto.newBuilder().build();
  }

  @Override
  public TSAggValueProto addInput(String key, TSAggValueProto accum, TSProto input) {
    return TimeseriesUtils.addTSValue(accum, input);
  }

  @Override
  public TSAggValueProto mergeAccumulators(String key, Iterable<TSAggValueProto> accums) {
    TSAggValueProto merged = createAccumulator(key);

    return TimeseriesUtils.addAggValue(merged, accums);
  }

  @Override
  public TSAggValueProto extractOutput(String key, TSAggValueProto accum) {
    return accum;

  }

}
