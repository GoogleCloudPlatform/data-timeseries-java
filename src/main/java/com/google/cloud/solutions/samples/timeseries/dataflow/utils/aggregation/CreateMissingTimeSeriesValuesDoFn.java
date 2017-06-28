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

import java.io.Serializable;
import java.util.List;

import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.solutions.samples.timeseries.dataflow.application.proto.TimeSeriesProtos.TSProto;


@SuppressWarnings("serial")
/**
 * Converts the list of missing 'ticks' into generated TSProto's for this time window
 *
 */
public class CreateMissingTimeSeriesValuesDoFn extends DoFn<List<String>, KV<String, TSProto>>
    implements com.google.cloud.dataflow.sdk.transforms.DoFn.RequiresWindowAccess, Serializable {

  private static final Logger LOG = LoggerFactory
      .getLogger(CreateMissingTimeSeriesValuesDoFn.class);

  @Override
  public void processElement(DoFn<List<String>, KV<String, TSProto>>.ProcessContext c)
      throws Exception {
	
	  List<String> timeSeriesKey = c.element();
	  BoundedWindow window = c.window();
	  
    for (String s : timeSeriesKey) {
      TSProto ts =
          TSProto.newBuilder().setKey(s).setIsLive(false)
              .setTime(window.maxTimestamp().getMillis()).build();

      c.outputWithTimestamp(KV.of(s, ts), window.maxTimestamp());
      
      LOG.debug(String.format("Generated value for missing tick details are key : %s time: %s" , ts.getKey(), new Instant(ts.getTime())));

    }

  }
}
