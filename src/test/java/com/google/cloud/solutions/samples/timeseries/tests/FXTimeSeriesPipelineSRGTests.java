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

package com.google.cloud.solutions.samples.timeseries.tests;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.client.protobuf.ProtocolBuffers;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.PipelineResult;
import com.google.cloud.dataflow.sdk.coders.AvroCoder;
import com.google.cloud.dataflow.sdk.coders.DefaultCoder;
import com.google.cloud.dataflow.sdk.coders.protobuf.ProtoCoder;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.inprocess.InProcessPipelineRunner;
import com.google.cloud.dataflow.sdk.runners.inprocess.InProcessPipelineRunner.InProcessPipelineResult;
import com.google.cloud.dataflow.sdk.testing.DataflowAssert;
import com.google.cloud.dataflow.sdk.testing.TestStream;
import com.google.cloud.dataflow.sdk.transforms.Combine;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.Flatten;
import com.google.cloud.dataflow.sdk.transforms.GroupByKey;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.Sum;
import com.google.cloud.dataflow.sdk.transforms.View;
import com.google.cloud.dataflow.sdk.transforms.windowing.AfterPane;
import com.google.cloud.dataflow.sdk.transforms.windowing.FixedWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.GlobalWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.OutputTimeFns;
import com.google.cloud.dataflow.sdk.transforms.windowing.Repeatedly;
import com.google.cloud.dataflow.sdk.transforms.windowing.Window;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionList;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.cloud.solutions.samples.timeseries.dataflow.application.coders.TimeSeriesCoders;
import com.google.cloud.solutions.samples.timeseries.dataflow.application.pipelines.fx.FXTimeSeriesPipelineOptions;
import com.google.cloud.solutions.samples.timeseries.dataflow.application.pipelines.fx.GenerateSampleData;
import com.google.cloud.solutions.samples.timeseries.dataflow.application.proto.TimeSeriesProtos.SimpleAggTester;
import com.google.cloud.solutions.samples.timeseries.dataflow.application.proto.TimeSeriesProtos.TSAggValueProto;
import com.google.cloud.solutions.samples.timeseries.dataflow.application.proto.TimeSeriesProtos.TSProto;
import com.google.cloud.solutions.samples.timeseries.dataflow.application.proto.TimeSeriesProtos.WorkPacketConfig;
import com.google.cloud.solutions.samples.timeseries.dataflow.application.workpackets.FlattenKVIterableDoFn;
import com.google.cloud.solutions.samples.timeseries.dataflow.utils.aggregation.CompleteTimeSeriesAggCombiner;
import com.google.cloud.solutions.samples.timeseries.dataflow.utils.aggregation.CreateMissingTimeSeriesValuesDoFn;
import com.google.cloud.solutions.samples.timeseries.dataflow.utils.aggregation.DetectMissingTimeSeriesValuesCombiner;
import com.google.cloud.solutions.samples.timeseries.dataflow.utils.aggregation.EmbedWindowTimeIntoAggregateDoFn;
import com.google.cloud.solutions.samples.timeseries.dataflow.utils.aggregation.PartialTimeSeriesAggCombiner;
import com.google.protobuf.TextFormat;
import com.sun.javafx.binding.StringFormatter;

@SuppressWarnings("serial")
public class FXTimeSeriesPipelineSRGTests implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(FXTimeSeriesPipelineSRGTests.class);

  private static final int BAR_SIZE_SEC = 120;
  private static final int WINDOW_SIZE_SECS = 600;
  private static final int SLIDING_WINDOW_PERIOD_SECS = 300;


  public static Pipeline setup() {

    FXTimeSeriesPipelineOptions options =
        PipelineOptionsFactory.as(FXTimeSeriesPipelineOptions.class);


    // Setup the windowing variables for the test run
    options.setCandleResolution(BAR_SIZE_SEC);
    options.setCorrelationWindowSize(WINDOW_SIZE_SECS);
    options.setCorrelationWindowPeriod(SLIDING_WINDOW_PERIOD_SECS);

    // Set Runner to use
    options.setRunner(InProcessPipelineRunner.class);

    Pipeline pipeline = Pipeline.create(options);

    // Register all of the coders used within the Pipeline
    TimeSeriesCoders.registerCoders(pipeline);

    // Create WorkPacket SideInput

    return pipeline;
  }

  public PCollection<KV<String, TSProto>> setupDataInput(Pipeline pipeline,
      List<KV<String, TSProto>> data) {


    // Assert that we have 44 Elements in the PCollection
    PCollection<KV<String, TSProto>> tsData =
        pipeline.apply("ReadData", Create.of(data))
            .apply(ParDo.of(new DoFn<KV<String, TSProto>, KV<String, TSProto>>() {

              @Override
              public void processElement(ProcessContext c) throws Exception {
                c.outputWithTimestamp(c.element(),
                    new DateTime(c.element().getValue().getTime()).toInstant());

              }

            })).setName("Assign TimeStamps");
    return tsData;

  }

  public PCollection<KV<String, TSProto>> generateCompleteWindowData(Pipeline pipeline,
      List<KV<String, TSProto>> data, WorkPacketConfig packetConfig) {

    LOG.info("Check to see that time streams with missing 'ticks' have been corrected");

    PCollection<KV<String, TSProto>> tsData = setupDataInput(pipeline, data);


    PCollection<KV<String, TSProto>> windowedData =
        tsData.apply("CandleResolutionWindow", Window.<KV<String, TSProto>>into(FixedWindows
            .of(Duration.standardSeconds(((FXTimeSeriesPipelineOptions) pipeline.getOptions())
                .getCandleResolution()))));

    // Determine streams that are missing in this Window and generate values for them

    PCollection<KV<String, TSProto>> generatedValues =
        windowedData
            .apply(
                "DetectMissingTimeSeriesValues",
                Combine.globally(new DetectMissingTimeSeriesValuesCombiner(packetConfig))
                    .withoutDefaults()).apply(ParDo.of(new CreateMissingTimeSeriesValuesDoFn()))
            .setName("CreateMissingTimeSeriesValues");

    // Flatten the live streams and the generated streams together

    PCollection<KV<String, TSProto>> completeWindowData =
        PCollectionList.of(windowedData).and(generatedValues)
            .apply("MergeGeneratedLiveValues", Flatten.<KV<String, TSProto>>pCollections());


    return completeWindowData;
  }


  public PCollection<KV<String, TSAggValueProto>> createCompleteAggregates(Pipeline pipeline,
      List<KV<String, TSProto>> data, WorkPacketConfig packetConfig) {

    PCollection<KV<String, TSProto>> completeWindowData =
        generateCompleteWindowData(pipeline, data, packetConfig);

    PCollection<KV<String, TSAggValueProto>> parital =
        completeWindowData.apply("CreatePartialAggregates",
            Combine.perKey(new PartialTimeSeriesAggCombiner()));

    PCollection<KV<String, TSAggValueProto>> paritalWithWindowBoundary =
        parital.apply(ParDo.of(new EmbedWindowTimeIntoAggregateDoFn()));

    PCollection<KV<String, TSAggValueProto>> completeAggregationStage1 =
        paritalWithWindowBoundary.apply(
            "completeAggregationStage1",
            Window.<KV<String, TSAggValueProto>>into(new GlobalWindows())
                .triggering(Repeatedly.forever(AfterPane.elementCountAtLeast(1)))
                .withOutputTimeFn(OutputTimeFns.outputAtEarliestInputTimestamp())
                .accumulatingFiredPanes());

    PCollection<KV<String, TSAggValueProto>> completeAggregationStage2 =
        completeAggregationStage1.apply("CreateCompleteCandles",
            Combine.perKey(new CompleteTimeSeriesAggCombiner())).apply("FlattenIterables",
            ParDo.of(new FlattenKVIterableDoFn()));

    PCollection<KV<String, TSAggValueProto>> completeAggregationStage3 =
        completeAggregationStage2.apply("ResetTimestampsAfterGlobalWindow",
            ParDo.of(new DoFn<KV<String, TSAggValueProto>, KV<String, TSAggValueProto>>() {

              @Override
              public void processElement(
                  DoFn<KV<String, TSAggValueProto>, KV<String, TSAggValueProto>>.ProcessContext c)
                  throws Exception {
                if (c.timestamp().isBefore(new Instant(32530703764000L))) {

                  if (c.timestamp().isAfter(
                      new Instant(c.element().getValue().getCloseState().getTime()))) {

                    LOG.error("BUG There was a timestamp before current :: "
                        + TextFormat.shortDebugString(c.element().getValue()));

                  } else {
                    c.outputWithTimestamp(c.element(), new Instant(c.element().getValue()
                        .getCloseTime()));

                  }
                }

              }

            }));

    return completeAggregationStage3;

  }

  public static Map<String, TSProto> generateMapData(List<KV<String, TSProto>> dataList) {

    Map<String, TSProto> map = new HashMap<String, TSProto>();

    for (KV<String, TSProto> ts : dataList) {
      map.put(createTsKey(ts.getKey(), ts.getValue().getTime()), ts.getValue());
    }

    return map;

  }

  public static String createTsKey(String key, long ts) {

    return String.join(":", key, String.valueOf(ts));
  }

  public static String extractKey(String key) {

    return key.split(":")[0];
  }



  @org.junit.Test
  public void testDataInput() {

    Pipeline pipeline = setup();

    PCollection<KV<String, TSProto>> tsData =
        setupDataInput(pipeline, GenerateSampleData.getTestData());

    LOG.info("Check that we have 42 elements in the Input PCollection");

    DataflowAssert.that(
        tsData.apply("TestInputElementCount", ParDo.of(new DoFn<KV<String, TSProto>, Integer>() {

          @Override
          public void processElement(DoFn<KV<String, TSProto>, Integer>.ProcessContext c)
              throws Exception {

            c.output(1);
          }

        })).apply(Sum.integersGlobally())).containsInAnyOrder(42);

    pipeline.run();

  }

  @org.junit.Test
  public void testCompleteWindowData() {

    Pipeline pipeline = setup();

    List<KV<String, TSProto>> pipelineData = GenerateSampleData.getTestData();
    List<KV<String, TSProto>> testData = new ArrayList<KV<String, TSProto>>(pipelineData);
    WorkPacketConfig packetConfig = GenerateSampleData.generateWorkPacketConfig(2);

    PCollection<KV<String, TSProto>> completeWindowData =
        generateCompleteWindowData(pipeline, pipelineData, packetConfig);

    testData.add(KV.of(GenerateSampleData.TS3, TSProto.newBuilder().setKey(GenerateSampleData.TS3)
            .setIsLive(false).setTime(1451577839999L).build()));
    testData.add(KV.of(GenerateSampleData.TS4, TSProto.newBuilder().setKey(GenerateSampleData.TS4)
            .setIsLive(false).setTime(1451577839999L).build()));
    
    DataflowAssert.that(completeWindowData).containsInAnyOrder(testData);
    pipeline.run();
  }

  @org.junit.Test
  public void testCompleteCandleDataOneStream() {

    Pipeline pipeline = setup();

    List<KV<String, TSProto>> pipelineData = GenerateSampleData.getTestData();
    WorkPacketConfig packetConfig =
        GenerateSampleData.generateWorkPacketConfig(2, new String[] {GenerateSampleData.TS1});

    Map<String, TSProto> map = generateMapData(pipelineData);

    // Run test with TS-1 data only

    List<KV<String, TSProto>> ts1Only = new ArrayList<>();

    for (String ts : map.keySet()) {
      if (extractKey(ts).equals(GenerateSampleData.TS1)) {
        ts1Only.add(KV.of(extractKey(ts), map.get(ts)));
      }
    }

    List<KV<String, TSProto>> testData = new ArrayList<KV<String, TSProto>>(ts1Only);

    PCollection<KV<String, TSAggValueProto>> completeAggs =
        createCompleteAggregates(pipeline, ts1Only, packetConfig);

    PCollection<SimpleAggTester> simpleAgg =
        completeAggs.apply(ParDo.of(new DoFn<KV<String, TSAggValueProto>, SimpleAggTester>() {

          @Override
          public void processElement(
              DoFn<KV<String, TSAggValueProto>, SimpleAggTester>.ProcessContext c) throws Exception {

            c.output(SimpleAggTester.newBuilder().setKey(c.element().getKey())
                .setCloseTime(c.element().getValue().getCloseTime())
                .setOpenStateTime(c.element().getValue().getOpenState().getTime())
                .setCloseStateTime(c.element().getValue().getCloseState().getTime())
                .setMinAskPrice(c.element().getValue().getMinAskValue().getAskPrice())
                .setMaxAskPrice(c.element().getValue().getMaxAskValue().getAskPrice())
                .setMinBidPrice(c.element().getValue().getMinBidValue().getBidPrice())
                .setMaxBidPrice(c.element().getValue().getMaxBidValue().getBidPrice()).build());

          }

        }));

    List<SimpleAggTester> expectedList = new ArrayList<>();

    String key = GenerateSampleData.TS1;

    expectedList.add(SimpleAggTester.newBuilder().setKey(key).setCloseTime(1451577719999L)
            .setOpenStateTime(1451577660000L).setCloseStateTime(1451577660000L).setMinAskPrice(1)
            .setMaxAskPrice(2).setMinBidPrice(1).setMaxBidPrice(2).build());

    expectedList.add(SimpleAggTester.newBuilder().setKey(key).setCloseTime(1451577839999L)
            .setOpenStateTime(1451577660000L).setCloseStateTime(1451577780000L).setMinAskPrice(3)
            .setMaxAskPrice(4).setMinBidPrice(3).setMaxBidPrice(4).build());

    expectedList.add(SimpleAggTester.newBuilder().setKey(key).setCloseTime(1451577959999L)
            .setOpenStateTime(1451577780000L).setCloseStateTime(1451577900000L).setMinAskPrice(5)
            .setMaxAskPrice(5).setMinBidPrice(5).setMaxBidPrice(5).build());

    expectedList.add(SimpleAggTester.newBuilder().setKey(key).setCloseTime(1451578079999L)
            .setOpenStateTime(1451577900000L).setCloseStateTime(1451578020000L).setMinAskPrice(3)
            .setMaxAskPrice(4).setMinBidPrice(3).setMaxBidPrice(4).build());

    expectedList.add(SimpleAggTester.newBuilder().setKey(key).setCloseTime(1451578199999L)
            .setOpenStateTime(1451578020000L).setCloseStateTime(1451578140000L).setMinAskPrice(1)
            .setMaxAskPrice(2).setMinBidPrice(1).setMaxBidPrice(2).build());

    
      
    DataflowAssert.that(simpleAgg).containsInAnyOrder(expectedList);

    pipeline.run();
  }


}