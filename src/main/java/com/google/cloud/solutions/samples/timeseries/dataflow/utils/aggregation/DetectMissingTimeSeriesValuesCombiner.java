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

package com.google.cloud.solutions.samples.timeseries.dataflow.utils.aggregation;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.dataflow.sdk.transforms.Combine.CombineFn;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.solutions.samples.timeseries.dataflow.application.proto.TimeSeriesProtos.TSProto;
import com.google.cloud.solutions.samples.timeseries.dataflow.application.proto.TimeSeriesProtos.WorkPacketConfig;

@SuppressWarnings("serial")
/**
 * Create a list of all unique keys seen in this window and then takes all the keys found in this window and does a diff 
 * against all possible values to create a set of NOOP records for the missing keys
 *
 */
public class DetectMissingTimeSeriesValuesCombiner extends
    CombineFn<KV<String, TSProto>, Set<String>, List<String>> {

  private static final Logger LOG = LoggerFactory
      .getLogger(DetectMissingTimeSeriesValuesCombiner.class);

  WorkPacketConfig workPacketConfig = null;

  public DetectMissingTimeSeriesValuesCombiner(WorkPacketConfig workPacketConfig) {
    this.workPacketConfig = workPacketConfig;
  }


  @Override
  public Set<String> createAccumulator() {
    return new HashSet<String>();
  }

  @Override
  public Set<String> addInput(Set<String> accumulator, KV<String, TSProto> input) {
    accumulator.add(input.getKey());

    return accumulator;
  }

  @Override
  public Set<String> mergeAccumulators(Iterable<Set<String>> accumulators) {
    Set<String> accum = new HashSet<String>();

    for (Set<String> c : accumulators) {
      accum.addAll(c);
    }
    return accum;
  }

  @Override
  public List<String> extractOutput(Set<String> accumulator) {

    List<String> diff = new ArrayList<String>();

    for (String s : workPacketConfig.getKeysList()) {
      if (!accumulator.contains(s)) {
        diff.add(s);
      }
    }

    return diff;
  }
}
