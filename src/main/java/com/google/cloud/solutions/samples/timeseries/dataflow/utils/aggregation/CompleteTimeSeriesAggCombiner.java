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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.dataflow.sdk.coders.AtomicCoder;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.CoderException;
import com.google.cloud.dataflow.sdk.coders.DefaultCoder;
import com.google.cloud.dataflow.sdk.coders.ListCoder;
import com.google.cloud.dataflow.sdk.coders.protobuf.ProtoCoder;
import com.google.cloud.dataflow.sdk.transforms.Combine.KeyedCombineFn;
import com.google.cloud.solutions.samples.timeseries.dataflow.application.proto.TimeSeriesProtos.TSAggValueProto;
import com.google.cloud.solutions.samples.timeseries.dataflow.application.proto.TimeSeriesProtos.TSProto;
import com.google.cloud.solutions.samples.timeseries.dataflow.utils.aggregation.TimeseriesUtils.TSTuple;
import com.google.protobuf.TextFormat;


@SuppressWarnings("serial")
/**
 * This combiner is designed to emulate keyed state by retaining the closing value of the last aggregation and propagating that value to the new aggregation for the window
 *
 */
public class CompleteTimeSeriesAggCombiner
    extends
    KeyedCombineFn<String, TSAggValueProto, CompleteTimeSeriesAggCombiner.Accum, List<TSAggValueProto>>
    implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(CompleteTimeSeriesAggCombiner.class);

  @DefaultCoder(CompleteCandleAccumCoder.class)
  public static class Accum {
    public TSProto lastCandle;

    // In the aggregation step we may get many Accum and this is not associative so to catch the
    // issue

    public List<TSAggValueProto> candles = new ArrayList<TSAggValueProto>();

  }

  @Override
  public Accum createAccumulator(String key) {
    return new Accum();
  }

  @Override
  public Accum addInput(String key, Accum accum, TSAggValueProto input) {

    compact(accum);
    
    // Add all new candles to the accum, this is not associative, every candle needs to be saved
    accum.candles.add(input);

    LOG.debug("Adding Accum " + TextFormat.shortDebugString(input));

    return accum;
  }

  @Override
  public Accum mergeAccumulators(String key, Iterable<Accum> accums) {

    for (Accum a : accums) {
      compact(a);
    }


    Accum aggregatedAccum = new Accum();

    // We can get these in any order so need to sort
    for (Accum accum : accums) {

      // If LastCandle is present add it to the accum
      if (accum.lastCandle != null) {
        aggregatedAccum.lastCandle = accum.lastCandle;
      }

      aggregatedAccum.candles.addAll(accum.candles);

    }

    return aggregatedAccum;
  }


  // We may have multiple accum's, so each one needs its close value set to the previous one in the
  // list before being emitted forward

  @Override
  public List<TSAggValueProto> extractOutput(String key, Accum accum) {


    List<TSTuple> candles = new ArrayList<TSTuple>();

    List<TSAggValueProto> completeCandles = new ArrayList<TSAggValueProto>();

    for (TSAggValueProto c : accum.candles) {
      candles.add(new TSTuple(c));

    }

    // Put the candles in ts order
    Collections.sort(candles);

    Iterator<TSTuple> it = candles.iterator();

    TSTuple currentValue = null;

    while (it.hasNext()) {
      currentValue = it.next();

      // Update the current candle with the last close if available

      TSAggValueProto aggValueProto = currentValue.tsAggValueProto;

      if (accum.lastCandle != null) {
        aggValueProto =
            TimeseriesUtils.addTSOpenValue(currentValue.tsAggValueProto, accum.lastCandle).build();

        // The close state can mutate as a result of the change in open position
        accum.lastCandle = aggValueProto.getCloseState();
      } else {
        // If there is no candle then this is the first window pane we are dealing with and we need
        // to add something so we will add the close
        // TODO In this situation we should be pulling from an outside source from Dataflow to get
        // the correct last value
        aggValueProto =
            TimeseriesUtils.addTSOpenValue(currentValue.tsAggValueProto,
                currentValue.tsAggValueProto.getCloseState()).build();

        accum.lastCandle = currentValue.tsAggValueProto.getCloseState();
      }



      completeCandles.add(aggValueProto);
    }

    // Clear down the accum list
    accum.candles.clear();

    return completeCandles;
  }

  public static Accum compact(Accum accum) {

    List<TSAggValueProto> delList = new ArrayList<TSAggValueProto>();

    // We need to do compaction of the accum here and not in extractOutput as it may not be the same
    // object as that stored locally
    if (accum.lastCandle != null) {
      for (TSAggValueProto ts : accum.candles) {
        if (ts.getCloseState().getTime() <= accum.lastCandle.getTime()) {
          delList.add(ts);
        }
      }

      delList.removeAll(delList);
    }

    return accum;

  }

  static public class CompleteCandleAccumCoder extends AtomicCoder<Accum> {

    private CompleteCandleAccumCoder() {};

    private static final CompleteCandleAccumCoder INSTANCE = new CompleteCandleAccumCoder();

    public static Coder<Accum> of(Class<Accum> c) {
      return INSTANCE;
    }

    private static final Coder<TSProto> TSPROTO_CODER = ProtoCoder.of(TSProto.class);

    private static final Coder<List<TSAggValueProto>> LIST_CODER = ListCoder.of(ProtoCoder
        .of(TSAggValueProto.class));



    @Override
    public void encode(Accum value, OutputStream outStream,
        com.google.cloud.dataflow.sdk.coders.Coder.Context context) throws CoderException,
        IOException {

      TSPROTO_CODER.encode(value.lastCandle, outStream, context.nested());
      LIST_CODER.encode(value.candles, outStream, context.nested());

    }

    @Override
    public Accum decode(InputStream inStream,
        com.google.cloud.dataflow.sdk.coders.Coder.Context context) throws CoderException,
        IOException {
      Accum accum = new Accum();
      accum.lastCandle = TSPROTO_CODER.decode(inStream, context.nested());
      accum.candles = LIST_CODER.decode(inStream, context.nested());
      return accum;
    }


  }
}
