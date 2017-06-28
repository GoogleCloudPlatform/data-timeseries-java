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

import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.dataflow.sdk.transforms.Combine;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.Flatten;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.windowing.AfterPane;
import com.google.cloud.dataflow.sdk.transforms.windowing.FixedWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.GlobalWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.OutputTimeFns;
import com.google.cloud.dataflow.sdk.transforms.windowing.Repeatedly;
import com.google.cloud.dataflow.sdk.transforms.windowing.Window;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionList;
import com.google.cloud.solutions.samples.timeseries.dataflow.application.pipelines.fx.FXTimeSeriesPipelineOptions;
import com.google.cloud.solutions.samples.timeseries.dataflow.application.proto.TimeSeriesProtos.TSAggValueProto;
import com.google.cloud.solutions.samples.timeseries.dataflow.application.proto.TimeSeriesProtos.TSProto;
import com.google.cloud.solutions.samples.timeseries.dataflow.application.proto.TimeSeriesProtos.WorkPacketConfig;
import com.google.cloud.solutions.samples.timeseries.dataflow.application.workpackets.FlattenKVIterableDoFn;
import com.google.protobuf.TextFormat;

@SuppressWarnings("serial")
public class CreateAggregatesTransform
    extends PTransform<PCollection<KV<String, TSProto>>, PCollection<KV<String, TSAggValueProto>>> {

  private static final Logger LOG = LoggerFactory.getLogger(CreateAggregatesTransform.class);

  FXTimeSeriesPipelineOptions options;


  WorkPacketConfig packetConfig;

  public CreateAggregatesTransform(FXTimeSeriesPipelineOptions options,
      WorkPacketConfig packetConfig) {

    this.options = options;
    this.packetConfig = packetConfig;

  }

  // Assign aggregation window

  @Override
  public PCollection<KV<String, TSAggValueProto>> apply(PCollection<KV<String, TSProto>> input) {



    PCollection<KV<String, TSProto>> windowedData =
        input.apply("CandleResolutionWindow", Window.<KV<String, TSProto>>into(
            FixedWindows.of(Duration.standardSeconds(options.getCandleResolution()))));

    // Determine streams that are missing in this Window and generate values for them

    PCollection<KV<String, TSProto>> generatedValues = windowedData
        .apply("DetectMissingTimeSeriesValues",
            Combine.globally(new DetectMissingTimeSeriesValuesCombiner(packetConfig))
                .withoutDefaults())
        .apply(ParDo.of(new CreateMissingTimeSeriesValuesDoFn()))
        .setName("CreateMissingTimeSeriesValues");

    // Flatten the live streams and the generated streams together

    PCollection<KV<String, TSProto>> completeWindowData =
        PCollectionList.of(windowedData).and(generatedValues).apply("MergeGeneratedLiveValues",
            Flatten.<KV<String, TSProto>>pCollections());

    // Create partial aggregates, at this stage we will not bring forward the previous windows close
    // value
    PCollection<KV<String, TSAggValueProto>> parital = completeWindowData
        .apply("CreatePartialAggregates", Combine.perKey(new PartialTimeSeriesAggCombiner()));

    // When these aggregates go through the Global Window they will lose their time value
    // We will embed the window close into the data so we can access it later on

    PCollection<KV<String, TSAggValueProto>> paritalWithWindowBoundary =
        parital.apply(ParDo.of(new EmbedWindowTimeIntoAggregateDoFn()));

    // Create a Global window which can retain the last value held in memory We must use
    // outputAtEarliestInputTimestamp as later on we re-attach the timestamp from within the data
    // point, for us not to hit 'skew' issues we need to ensure the output timestamp value is always
    // the smallest value
    PCollection<KV<String, TSAggValueProto>> completeAggregationStage1 =
        paritalWithWindowBoundary.apply("completeAggregationStage1",
            Window.<KV<String, TSAggValueProto>>into(new GlobalWindows())
                .triggering(Repeatedly.forever(AfterPane.elementCountAtLeast(1)))
                .withOutputTimeFn(OutputTimeFns.outputAtEarliestInputTimestamp())
                .accumulatingFiredPanes());

    PCollection<KV<String, TSAggValueProto>> completeAggregationStage2 = completeAggregationStage1
        .apply("CreateCompleteCandles", Combine.perKey(new CompleteTimeSeriesAggCombiner()))
        .apply("FlattenIterables", ParDo.of(new FlattenKVIterableDoFn()));



    // Reset timestamps after global window
    PCollection<KV<String, TSAggValueProto>> completeAggregationStage3 =
        completeAggregationStage2.apply("ResetTimestampsAfterGlobalWindow",
            ParDo.of(new DoFn<KV<String, TSAggValueProto>, KV<String, TSAggValueProto>>() {

              @Override
              public void processElement(
                  DoFn<KV<String, TSAggValueProto>, KV<String, TSAggValueProto>>.ProcessContext c)
                  throws Exception {
                //
                // TODO When the local Dataflow runners shuts down there will be some values
                // produced for the end of the the GlobalWindow. We can remove these values by
                // filtering out anything from year 3000+ for now. Better solution will be to check
                // the WINDOW PANE
                //
            	  Instant time = c.timestamp();
            	  
                if (time.isBefore(new Instant(32530703764000L))) {

                  // The timestamp produced from the Combiner after the GlobalWindow loses fidelity,
                  // we can add this back by looking at the value in the data

                  if (time
                      .isAfter(new Instant(c.element().getValue().getCloseState().getTime()))) {

                    LOG.error(
                        "There was a timestamp before earlier than the window and skew must be 0 :: "
                            + TextFormat.shortDebugString(c.element().getValue()));

                  } else {
                    c.outputWithTimestamp(c.element(),
                        new Instant(c.element().getValue().getCloseTime()));

                  }
                }

              }

            }));

    return completeAggregationStage3;
  }

}
