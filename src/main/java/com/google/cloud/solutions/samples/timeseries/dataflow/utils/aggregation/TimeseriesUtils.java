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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.solutions.samples.timeseries.dataflow.application.proto.TimeSeriesProtos.TSAggValueProto;
import com.google.cloud.solutions.samples.timeseries.dataflow.application.proto.TimeSeriesProtos.TSAggValueProto.Builder;
import com.google.cloud.solutions.samples.timeseries.dataflow.application.proto.TimeSeriesProtos.TSProto;


/**
 * Set of utilities for working with TimeSeries protos
 */
public class TimeseriesUtils {
  private static final Logger LOG = LoggerFactory.getLogger(TimeseriesUtils.class);

  public static TSAggValueProto addAggValue(TSAggValueProto aggValueProto,
      Iterable<TSAggValueProto> tsAccumilator) {

    Builder builder = TSAggValueProto.newBuilder(aggValueProto);

    for (TSAggValueProto ts : tsAccumilator) {

      addTSValue(builder, ts.getOpenState());
      addTSValue(builder, ts.getCloseState());
      addTSValue(builder, ts.getMaxAskValue());
      addTSValue(builder, ts.getMinAskValue());
      addTSValue(builder, ts.getMaxBidValue());
      addTSValue(builder, ts.getMinBidValue());

      aggValueProto = builder.build();

    }

    return builder.build();

  }

  public static TSAggValueProto addTSValue(TSAggValueProto aggValueProto, TSProto tsProto) {

    return addTSValue(TSAggValueProto.newBuilder(aggValueProto), tsProto).build();

  }

  /**
   * Addes a new proto to the provided builder updating min/max/open/close based on time and ASK /
   * BID values There are multiple comparisons that need to be undertaken. If this Accum has no
   * state then set all variables to new Input - Is this tsValue's value larger than the Max value
   * we hold - Is this tsValue's value smaller than the Min value we hold - Is this tsValue's time
   * stamp larger than our close timestamp - Is this tsValue's time stamp smaller than our open
   * timestamp
   * 
   * @param builder The builder that we want information to be added to
   * @param tsProto The TimeSeries Proto that is to be added to this Aggregator
   * @return
   */
  public static Builder addTSValue(Builder builder, TSProto tsProto) {



    // If this object has never been set then assign tSValue to all placeholders
    checkAndSetMinAskValue(builder, tsProto);
    checkAndSetMaxAskValue(builder, tsProto);

    checkAndSetMinBidValue(builder, tsProto);
    checkAndSetMaxBidValue(builder, tsProto);

    checkAndSetCloseTsValue(builder, tsProto);

    return builder;
  }

  /**
   * Special case value, an open is always set from the previous close At this point we also check
   * to see if our current close is a real value or not If not then we load the values for the open
   * into the close to propagate the value forward
   * 
   * @param builder
   * @param tsProto
   * @return
   */
  public static Builder addTSOpenValue(TSAggValueProto aggValueProto, TSProto tsProto) {


    TSProto.Builder propogatedValue =
        TSProto.newBuilder(tsProto).setTime(aggValueProto.getCloseState().getTime());

    TSAggValueProto.Builder builder = TSAggValueProto.newBuilder(aggValueProto);

    if (!aggValueProto.getCloseState().getIsLive()) {
      builder.setCloseState(propogatedValue);
    }

    if (!aggValueProto.getMaxAskValue().getIsLive()) {
      builder.setMaxAskValue(propogatedValue);
    }

    if (!aggValueProto.getMinAskValue().getIsLive()) {
      builder.setMinAskValue(propogatedValue);
    }
    if (!aggValueProto.getMaxBidValue().getIsLive()) {
      builder.setMaxBidValue(propogatedValue);
    }

    if (!aggValueProto.getMinBidValue().getIsLive()) {
      builder.setMinBidValue(propogatedValue);
    }

    builder.setOpenState(tsProto);

    return builder;
  }

  private static void checkAndSetMinAskValue(Builder builder, TSProto tsProto) {

    // Check if the current Value is live or one that has been generated in the Dataflow if not just
    // swap

    if (!builder.hasMinAskValue() || !builder.getMinAskValue().getIsLive()) {
      builder.setMinAskValue(tsProto);

    } else if (builder.getMinAskValue().getAskPrice() > tsProto.getAskPrice()) {
      builder.setMinAskValue(tsProto);

    }

  }

  private static void checkAndSetMaxAskValue(Builder builder, TSProto tsProto) {

    // Check if the current Value is live or one that has been generated in the Dataflow if not just
    // swap
    if (!builder.hasMaxAskValue() || !builder.getMaxAskValue().getIsLive()) {
      builder.setMaxAskValue(tsProto);

    } else if (builder.getMaxAskValue().getAskPrice() < tsProto.getAskPrice()) {
      builder.setMaxAskValue(tsProto);
    }

  }

  // Check if Min value is bigger than the new value

  private static void checkAndSetMinBidValue(Builder builder, TSProto tsProto) {

    // Check if the current Value is live or one that has been generated in the Dataflow if not just
    // swap
    if (!builder.hasMinBidValue() || !builder.getMinBidValue().getIsLive()) {
      builder.setMinBidValue(tsProto);

    } else if (builder.getMinBidValue().getAskPrice() > tsProto.getAskPrice()) {
      builder.setMinBidValue(tsProto);
    }

  }

  private static void checkAndSetMaxBidValue(Builder builder, TSProto tsProto) {

    // Check if the current Value is live or one that has been generated in the Dataflow if not just
    // swap
    if (!builder.hasMaxAskValue() || !builder.getMaxBidValue().getIsLive()) {
      builder.setMaxBidValue(tsProto);

    } else if (builder.getMaxBidValue().getAskPrice() < tsProto.getAskPrice()) {
      builder.setMaxBidValue(tsProto);
    }

  }


  /**
   * The close is the last value that is seen in the system, live values will always take precedence
   * over generated values
   * 
   * @param builder
   * @param tsProto
   */
  private static void checkAndSetCloseTsValue(Builder builder, TSProto tsProto) {

    // If there is no value just copy
    if (!builder.hasCloseState()) {

      builder.setCloseState(tsProto);

    } else {

      if (builder.getCloseState().getTime() < tsProto.getTime()) {
        
        // If this is not a generated value then just copy
        if (tsProto.getIsLive()) {
          
          builder.setCloseState(tsProto);
        
        } else {
          // As this is a generated value we need to copy the values and increase the time
          builder.setCloseState(TSProto.newBuilder(builder.getCloseState()).setIsLive(false).setTime(tsProto.getTime()));
        }

      }
    }

  }

  public static class TSTuple implements Comparable<TSTuple> {

    public TSAggValueProto tsAggValueProto;

    public TSTuple(TSAggValueProto tsAggValueProto) {
      this.tsAggValueProto = tsAggValueProto;
    }

    // Comparison of TSAggValueProto is done via the close timestamp
    @Override
    public int compareTo(TSTuple o) {
      return Long.compare(this.tsAggValueProto.getCloseTime(), o.tsAggValueProto.getCloseTime());
    }

  }

  @SuppressWarnings("serial")
  public static class TimeSeriesValue implements Comparable<TimeSeriesValue>, Serializable {

    long timestamp;
    public double value;

    public TimeSeriesValue(long timestamp, double value) {
      this.timestamp = timestamp;
      this.value = value;
    }

    @Override
    public int compareTo(TimeSeriesValue o) {
      if (this.timestamp < o.timestamp) {
        return -1;
      } else if (this.timestamp > o.timestamp) {
        return 1;
      }
      return 0;

    }

  }
}
