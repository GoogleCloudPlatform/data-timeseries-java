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

package com.google.cloud.solutions.samples.timeseries.dataflow.application.pipelines.fx;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.DirectPipelineRunner;
import com.google.cloud.dataflow.sdk.runners.inprocess.InProcessPipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.Combine;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.Flatten;
import com.google.cloud.dataflow.sdk.transforms.GroupByKey;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.View;
import com.google.cloud.dataflow.sdk.transforms.windowing.AfterPane;
import com.google.cloud.dataflow.sdk.transforms.windowing.DefaultTrigger;
import com.google.cloud.dataflow.sdk.transforms.windowing.FixedWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.GlobalWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.OutputTimeFns;
import com.google.cloud.dataflow.sdk.transforms.windowing.Repeatedly;
import com.google.cloud.dataflow.sdk.transforms.windowing.SlidingWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.Window;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionList;
import com.google.cloud.dataflow.sdk.values.PCollectionTuple;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import com.google.cloud.dataflow.sdk.values.TupleTagList;
import com.google.cloud.solutions.samples.timeseries.dataflow.application.coders.TimeSeriesCoders;
import com.google.cloud.solutions.samples.timeseries.dataflow.application.computation.ComputeCorrelationsDoFn;
import com.google.cloud.solutions.samples.timeseries.dataflow.application.computation.ComputeCorrelationsDoFn.CorrolationParDoConfig;
import com.google.cloud.solutions.samples.timeseries.dataflow.application.proto.TimeSeriesProtos.Correlation;
import com.google.cloud.solutions.samples.timeseries.dataflow.application.proto.TimeSeriesProtos.TSAggValueProto;
import com.google.cloud.solutions.samples.timeseries.dataflow.application.proto.TimeSeriesProtos.TSProto;
import com.google.cloud.solutions.samples.timeseries.dataflow.application.proto.TimeSeriesProtos.WorkDataPoint;
import com.google.cloud.solutions.samples.timeseries.dataflow.application.proto.TimeSeriesProtos.WorkPacket;
import com.google.cloud.solutions.samples.timeseries.dataflow.application.proto.TimeSeriesProtos.WorkPacketConfig;
import com.google.cloud.solutions.samples.timeseries.dataflow.application.proto.TimeSeriesProtos.WorkPackets;
import com.google.cloud.solutions.samples.timeseries.dataflow.application.workpackets.CreatePartitionWorkPacketsDoFn;
import com.google.cloud.solutions.samples.timeseries.dataflow.application.workpackets.CreateWorkPacketsDoFn;
import com.google.cloud.solutions.samples.timeseries.dataflow.application.workpackets.DistributeWorkDataDoFn;
import com.google.cloud.solutions.samples.timeseries.dataflow.application.workpackets.FlattenKVIterableDoFn;
import com.google.cloud.solutions.samples.timeseries.dataflow.utils.aggregation.CompleteTimeSeriesAggCombiner;
import com.google.cloud.solutions.samples.timeseries.dataflow.utils.aggregation.CreateMissingTimeSeriesValuesDoFn;
import com.google.cloud.solutions.samples.timeseries.dataflow.utils.aggregation.DetectMissingTimeSeriesValuesCombiner;
import com.google.cloud.solutions.samples.timeseries.dataflow.utils.aggregation.EmbedWindowTimeIntoAggregateDoFn;
import com.google.cloud.solutions.samples.timeseries.dataflow.utils.aggregation.PartialTimeSeriesAggCombiner;
import com.google.cloud.solutions.samples.timeseries.dataflow.utils.aggregation.TimeseriesUtils.TSTuple;
import com.google.protobuf.TextFormat;

public class FXTimeSeriesPipelineDemo {

  private static final Logger LOG = LoggerFactory.getLogger(FXTimeSeriesPipelineDemo.class);



  @SuppressWarnings("serial")
  public static void main(String[] args) throws Exception {


    // ----------------------- STEP 1: Setup -----------------------

    FXTimeSeriesPipelineOptions options =
        PipelineOptionsFactory.fromArgs(args).as(FXTimeSeriesPipelineOptions.class);

    // Enable logging output so that the user running the sample can see some results. This is very
    // verbose and will result in very poor performance with high loads.

    options.setEnableSampleLogging(true);

    options.setRunner(InProcessPipelineRunner.class);

    if (options.getRunner().equals(DirectPipelineRunner.class)) {
      throw new IllegalArgumentException("Please use InProcessPipelineRunner when running locally");
    }

    Pipeline pipeline = Pipeline.create(options);

    // Register all of the coders used within the Pipeline
    TimeSeriesCoders.registerCoders(pipeline);

    // Configure Correlation Output Criteria
    CorrolationParDoConfig config = new CorrolationParDoConfig();
    config.setIncludeUnderlying(true);
    config.setMinCorrValue(0);
    config.setPropogateNan(true);

    // Create WorkPacket SideInput

    WorkPacketConfig packetConfig = GenerateSampleData.generateWorkPacketConfig(20);


    // ----------------------- STEP 2 : Read data -----------------------

    // In this sample we read from a generator to avoid external dependencies This step can easily
    // be replaced with Pub/Sub or BQ sources

    @SuppressWarnings("serial")
    PCollection<KV<String, TSProto>> tsData =
        pipeline.apply("GenerateTestData_Test", Create.of(GenerateSampleData.getTestData())).apply(
            "AssignTimeStampsToInputs",
            ParDo.of(new DoFn<KV<String, TSProto>, KV<String, TSProto>>() {

              // Assign the data points timestamp to the element timestamp in the PCollection
              @Override
              public void processElement(ProcessContext c) throws Exception {
                c.outputWithTimestamp(c.element(),
                    new DateTime(c.element().getValue().getTime()).toInstant());

              }

            }));

    // ----------------------- STEP 3 : Create aggregates -----------------------

    // Assign aggregation window

    PCollection<KV<String, TSProto>> windowedData =
        tsData.apply("CandleResolutionWindow", Window.<KV<String, TSProto>>into(
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
                if (c.timestamp().isBefore(new Instant(32530703764000L))) {

                  // The timestamp produced from the Combiner after the GlobalWindow loses fidelity,
                  // we can add this back by looking at the value in the data

                  if (c.timestamp()
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

    // The following step is for illustration only, remove if you want to run under load. In this
    // step we will collect Keys together and then LOG out all of the aggregates that have been
    // created
    if (options.getEnableSampleLogging()) {

      completeAggregationStage3.apply("GroupByBeforeLoggingAggregates", GroupByKey.create())
          .apply(ParDo.of(new DoFn<KV<String, Iterable<TSAggValueProto>>, String>() {

            @Override
            public void processElement(
                DoFn<KV<String, Iterable<TSAggValueProto>>, String>.ProcessContext c)
                throws Exception {

              StringBuilder sb = new StringBuilder();

              sb.append(String.format("List of aggregated values for Key : %s  \n",
                  c.element().getKey()));

              List<TSTuple> sortedList = new ArrayList<TSTuple>();

              for (TSAggValueProto ts : c.element().getValue()) {
                sortedList.add(new TSTuple(ts));
              }

              Collections.sort(sortedList);

              for (TSTuple tuple : sortedList) {
                sb.append(TextFormat.shortDebugString(tuple.tsAggValueProto) + "\n");
              }
              LOG.info(sb.toString());

            }

          }));
    }


    // ----------------------- STEP 4 : Distribute work packets -----------------------

    final TupleTag<Map<String, WorkPacket>> main = new TupleTag<Map<String, WorkPacket>>() {};
    final TupleTag<Integer> counter = new TupleTag<Integer>() {};

    PCollection<KV<String, TSAggValueProto>> correlationWindow =
        completeAggregationStage3.apply("SlidingWindowForCorrelations",
            Window
                .<KV<String, TSAggValueProto>>into(
                    SlidingWindows.of(Duration.standardSeconds(options.getCorrelationWindowSize()))
                        .every(Duration.standardSeconds(options.getCorrelationWindowPeriod())))
                .triggering(DefaultTrigger.of()));

    PCollection<KV<String, WorkDataPoint>> workDataPoints =
        correlationWindow.apply(ParDo.of(new DistributeWorkDataDoFn(packetConfig, counter)));

    PCollectionTuple workpackets = workDataPoints.apply(GroupByKey.create())
        .apply(ParDo.of(new CreateWorkPacketsDoFn(packetConfig, counter)).withOutputTags(main,
            TupleTagList.of(counter)));


    PCollectionView<Map<String, WorkPacket>> groupedWork =
        workpackets.get(main).apply(View.asSingleton());

    PCollection<WorkPackets> workPackets =
        workpackets.get(counter).apply(ParDo.of(new DoFn<Integer, KV<Integer, Integer>>() {

          @Override
          public void processElement(DoFn<Integer, KV<Integer, Integer>>.ProcessContext c)
              throws Exception {

            for (int i = 0; i < 20; i++) {
              c.output(KV.of(i, i));
            }

          }

        })).apply(GroupByKey.create())

            .apply(ParDo.of(new CreatePartitionWorkPacketsDoFn(packetConfig, groupedWork))
                .withSideInputs(groupedWork));

    // ----------------------- STEP 5 : Correlate data points -----------------------

    PCollection<Correlation> correlations =

        workPackets.apply(ParDo.of(new ComputeCorrelationsDoFn(config)));

    if (options.getEnableSampleLogging()) {

      // The following step is for illustration only, remove if you want to run under load. In this
      // step we will LOG out all of the correlations that have been created

      PCollection<KV<Long, Iterable<Correlation>>> corrGBKTime =
          correlations.apply(ParDo.of(new DoFn<Correlation, KV<Long, Correlation>>() {

            @Override
            public void processElement(DoFn<Correlation, KV<Long, Correlation>>.ProcessContext c)
                throws Exception {
              c.output(KV.of(c.element().getTime(), c.element()));

            }

          })).apply(GroupByKey.create());

      corrGBKTime.apply(ParDo.of(new DoFn<KV<Long, Iterable<Correlation>>, String>() {

        @Override
        public void processElement(DoFn<KV<Long, Iterable<Correlation>>, String>.ProcessContext c)
            throws Exception {

          StringBuilder sb = new StringBuilder();
          sb.append(String.format("Corr for Timeslice : %s \n", c.element().getKey()));

          for (Correlation corr : c.element().getValue()) {

            sb.append(String.format(" Correlation Object Output : %s \n",
                TextFormat.shortDebugString(corr)));
          }

          LOG.info(sb.toString());
        }
      }));
    }
    pipeline.run();

  }
}
