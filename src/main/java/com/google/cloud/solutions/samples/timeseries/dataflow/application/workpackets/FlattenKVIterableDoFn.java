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

package com.google.cloud.solutions.samples.timeseries.dataflow.application.workpackets;

import java.io.Serializable;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.solutions.samples.timeseries.dataflow.application.proto.TimeSeriesProtos.TSAggValueProto;

/**
 * TODO : There was a standard transform to do this I think... This transform takes a KV<String,
 * List<TSAggValueProto> and returns a flattened KV<String, TSAggValueProto>
 *
 */
@SuppressWarnings("serial")
public class FlattenKVIterableDoFn extends
    DoFn<KV<String, List<TSAggValueProto>>, KV<String, TSAggValueProto>> implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(FlattenKVIterableDoFn.class);

  @Override
  public void processElement(
      DoFn<KV<String, List<TSAggValueProto>>, KV<String, TSAggValueProto>>.ProcessContext c)
      throws Exception {
    for (TSAggValueProto candle : c.element().getValue()) {

      c.output(KV.of(c.element().getKey(), candle));

    }

  }

}
