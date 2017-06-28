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
import java.util.Arrays;
import java.util.List;

import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.solutions.samples.timeseries.dataflow.application.proto.TimeSeriesProtos.TSAggValueProto;
import com.google.cloud.solutions.samples.timeseries.dataflow.application.proto.TimeSeriesProtos.TSProto;
import com.google.cloud.solutions.samples.timeseries.dataflow.application.proto.TimeSeriesProtos.WorkPacketConfig;


public class GenerateSampleData {

  private static final Logger LOG = LoggerFactory.getLogger(GenerateSampleData.class);

  public static String TS1 = "TS-1";
  public static String TS2 = "TS-2";
  public static String TS3 = "TS-3";
  public static String TS4 = "TS-4";
  public static String TS5 = "TS-5";

  public static WorkPacketConfig generateWorkPacketConfig(int partitionLength) {

    return generateWorkPacketConfig(partitionLength, GenerateSampleData.getWorkingSet());
  }

  public static WorkPacketConfig generateWorkPacketConfig(int partitionLength, String[] fullList) {

    WorkPacketConfig.Builder builder =
        WorkPacketConfig.newBuilder().addAllKeys(Arrays.asList(fullList));

    return builder.setPartitionLength(partitionLength).build();
  }

  public static void printTSAggValueProto(TSAggValueProto proto) {

    LOG.info(String.format(
        "Key value : %s, OpenTimeStamp : %s, CloseTimeStamp : %s  , minAsk : %s , maxAsk : %s)",
        proto.getKey(), proto.getOpenState().getTime(), proto.getCloseState().getTime(), proto
            .getMinAskValue().getAskPrice(), proto.getMaxAskValue().getAskPrice()));
  }

  public static String[] getWorkingSet() {

    String[] list = {TS1, TS2, TS3, TS4, TS5};

    return list;
  }

  /**
   * Generate 5 timeseries values across a 10 min window TS1, TS2, TS3, TS4, TS5. TS1 / TS2 Will
   * contain a single value per 1 min and will increase by a value of 1 till min 5 and then decrease
   * by a value of 1 until 10 mins TS3 Will have missing values at 2, 3, 7 and 8 mins and will
   * decrease by a value of 1 till min 5 and then increase by a value of 1 until 10 mins TS4 Will
   * have missing values at 2, 3, 7 and 8 mins and will decrease by a value of 1 till min 5 and then
   * increase by a value of 1 until 10 mins TS5 Will have random values assigned to it throughout
   * the process this is the control time series
   */
  public static List<KV<String, TSProto>> getTestData() {

    List<KV<String, TSProto>> ts = new ArrayList<KV<String, TSProto>>();

    String dateTime = "01/01/2016 00:00:00";
    DateTimeFormatter dtf = DateTimeFormat.forPattern("dd/MM/yyyy HH:mm:ss");
    Instant time = Instant.parse(dateTime, dtf);

    // KEY 001 & KEY 002
    String[] list = {TS1, TS2};

    for (String s : list) {
      GenerateSampleData.generateSequentialList(ts, time, s, 1d, 1d);
    }

    // KEY003
    List<KV<String, TSProto>> key003List = new ArrayList<KV<String, TSProto>>();

    GenerateSampleData.generateSequentialList(key003List, time, TS3, 10d, -1d);

    // Remove values
    key003List.remove(2); // Remove time value 02
    key003List.remove(2); // Remove time value 03
    key003List.remove(5); // Remove time value 07
    key003List.remove(5); // Remove time value 08

    ts.addAll(key003List);

    // KEY004
    List<KV<String, TSProto>> key004List = new ArrayList<KV<String, TSProto>>();

    GenerateSampleData.generateSequentialList(key004List, time, TS4, 10d, -1d);

    // Remove values
    key004List.remove(2); // Remove time value 02
    key004List.remove(2); // Remove time value 03
    key004List.remove(5); // Remove time value 07
    key004List.remove(5); // Remove time value 08


    ts.addAll(key004List);

    // KEY005
    Instant tsTime = new Instant(time);
    for (int i = 0; i < 10; i++) {

      ts.add(KV.of(TS5, TSProto.newBuilder().setAskPrice(Math.random()).setBidPrice(Math.random())
          .setKey(TS5).setIsLive(true).setTime(tsTime.getMillis()).build()));
      tsTime = tsTime.plus(Duration.standardMinutes(1));

    }

    return ts;

  }

  public static void generateSequentialList(List<KV<String, TSProto>> ts, Instant time, String key,
      double value, double change) {

    Instant tsTime = new Instant(time);
    for (int i = 0; i < 5; i++) {

      ts.add(KV.of(key, TSProto.newBuilder().setAskPrice(value).setBidPrice(value).setKey(key)
          .setIsLive(true).setTime(tsTime.getMillis()).build()));
      tsTime = tsTime.plus(Duration.standardMinutes(1));
      value += change;
    }

    value -= change;

    for (int i = 5; i < 10; i++) {
      ts.add(KV.of(key, TSProto.newBuilder().setAskPrice(value).setBidPrice(value).setKey(key)
          .setIsLive(true).setTime(tsTime.getMillis()).build()));
      tsTime = tsTime.plus(Duration.standardMinutes(1));
      value -= change;

    }

  }

  public static void main(String[] args) throws Exception {

    for (KV<String, TSProto> ts : GenerateSampleData.getTestData()) {
      System.out.println(String.join(":", ts.getKey(), String.valueOf(ts.getValue().getAskPrice()),
          String.valueOf(ts.getValue().getBidPrice()),
          new Instant(ts.getValue().getTime()).toString()));

    }
  }
}
