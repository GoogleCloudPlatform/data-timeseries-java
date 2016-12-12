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

package com.google.cloud.solutions.samples.timeseries.pipelines.fx;

import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.Validation;

public interface FXTimeSeriesPipelineOptions extends PipelineOptions {

  @Description("Number of Shards to create for the correlation calculations")
  @Validation.Required
  int getShards();

  void setShards(int value);

  @Description("Resolution of the candles in secs")
  @Default.Integer(120)
  int getCandleResolution();

  void setCandleResolution(int value);

  @Description("When outputing correlations should the underlying values be included. "
      + "Warning this will generate a lot of extra work and should only be used for testing / demos")
  @Validation.Required
  boolean getIncludeUnderlying();

  void setIncludeUnderlying(boolean value);

  @Description("The lowest ABS(correlation value) to emit. If there are 1000 FX instrements there will be (N^2-N)/2 "
      + "correlations very slide of the window. If you are consuming this in a UI downstream you need to be aware"
      + "of not swamping the downstream systems with values that have little importance. 0.5 is a good starting point."
      + "important to note that 0.5 is not the median there is normally a heavey skew towards 0 ")
  @Validation.Required
  double getMinCorrelationValue();

  void setMinCorrelationValue(double value);

  @Description("Should NAN values be propogated forward")
  @Validation.Required
  boolean getPropogateNAN();

  void setPropogateNAN(boolean value);


  @Description("Correlation sliding window size")
  @Default.Integer(600)
  int getCorrelationWindowSize();

  void setCorrelationWindowSize(int value);

  @Description("Sliding Window Period")
  @Default.Integer(300)
  int getCorrelationWindowPeriod();

  void setCorrelationWindowPeriod(int value);

  @Description("Enable Logging output to see results being output from the example")
  @Default.Boolean(false)
  boolean getEnableSampleLogging();

  void setEnableSampleLogging(boolean value);


}
