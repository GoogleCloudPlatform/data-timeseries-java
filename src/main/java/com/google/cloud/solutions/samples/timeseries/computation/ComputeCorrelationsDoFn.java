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

package com.google.cloud.solutions.samples.timeseries.computation;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.math3.stat.correlation.PearsonsCorrelation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.solutions.samples.timeseries.proto.TimeSeriesProtos.Correlation;
import com.google.cloud.solutions.samples.timeseries.proto.TimeSeriesProtos.WorkDataPoint;
import com.google.cloud.solutions.samples.timeseries.proto.TimeSeriesProtos.WorkPacket;
import com.google.cloud.solutions.samples.timeseries.proto.TimeSeriesProtos.WorkPackets;
import com.google.cloud.solutions.samples.timeseries.workpackets.WorkPacketUtils;
import com.google.protobuf.TextFormat;

@SuppressWarnings("serial")
public class ComputeCorrelationsDoFn extends DoFn<WorkPackets, Correlation> implements
    Serializable, com.google.cloud.dataflow.sdk.transforms.DoFn.RequiresWindowAccess

{

  private static final Logger LOG = LoggerFactory.getLogger(ComputeCorrelationsDoFn.class);

  /**
   * 
   * Configuration parameters for the Correlation analaysis Minimum threashold for forward default
   * is ABS(0.5) NAN are by default not propogated forward The underlying array is not Propogated
   * forward by default
   *
   */
  @SuppressWarnings("serial")
  public static class CorrolationParDoConfig implements Serializable {

    double minCorrValue = 0.5;
    boolean propogateNan = false;
    boolean includeUnderlying = false;

    public double getMinCorrValue() {
      return minCorrValue;
    }

    /**
     * 
     * @param minCorrValue
     */
    public void setMinCorrValue(double minCorrValue) {
      this.minCorrValue = minCorrValue;
    }

    public boolean isPropogateNan() {
      return propogateNan;
    }

    /**
     * 
     * @param propogateNan default False
     */
    public void setPropogateNan(boolean propogateNan) {
      this.propogateNan = propogateNan;
    }

    public boolean isIncludeUnderlying() {
      return includeUnderlying;
    }

    /**
     * 
     * @param includeUnderlying defines if the values used to compute the correlation should be sent
     *        forward carful about turning this on as it will generate a lot of data downstream
     */
    public void setIncludeUnderlying(boolean includeUnderlying) {
      this.includeUnderlying = includeUnderlying;
    }

  }

  private CorrolationParDoConfig config = null;

  public ComputeCorrelationsDoFn(CorrolationParDoConfig config) {
    this.config = config;
  }

  /**
	 * 
	 */
  @Override
  public void processElement(DoFn<WorkPackets, Correlation>.ProcessContext c) throws Exception {

    PearsonsCorrelation pearsonsCorrelation = new PearsonsCorrelation();

    // Cartisein Join the lists and then compute Pearsons Done compute self joins or transitive
    List<String> transList = new ArrayList<String>();

    for (WorkPacket outer : c.element().getWorkPacketsList()) {

      for (WorkPacket inner : c.element().getWorkPacketsList()) {

        String key = WorkPacketUtils.createKey(outer.getKey(), inner.getKey());
        String reverseKey = WorkPacketUtils.createKey(inner.getKey(), outer.getKey());



        boolean correlate = true;

        // Don't correlate against self
        if (outer.getKey() == inner.getKey()) {
          correlate = false;
        }

        // Don't correlate if there is only 1 value in the dataset
        if (outer.getDataPointsList().size() < 2) {
          correlate = false;
        }

        // a-b is the same as b-a so dont correlate
        if (transList.contains(key)) {
          correlate = false;
        }

        // Ensure we dont correlate transitive
        transList.add(reverseKey);

        if (outer.getDataPointsList().size() != inner.getDataPointsList().size()) {

          StringBuffer sb = new StringBuffer();

          sb.append(String.format("Key size mismatch between %s & %s", outer.getKey(),
              inner.getKey()));

          sb.append("\n Outer WorkPoint: ");

          for (WorkDataPoint wdp : outer.getDataPointsList()) {
            sb.append(TextFormat.shortDebugString(wdp));
          }

          sb.append("\n Inner WorkPoint: ");

          for (WorkDataPoint wdp : inner.getDataPointsList()) {
            sb.append(TextFormat.shortDebugString(wdp));
          }

          LOG.info(sb.toString());
        }
        if (correlate) {
          double correlationResult =
              pearsonsCorrelation.correlation(ComputationUtils.sortAndExtractDoubles(outer),
                  ComputationUtils.sortAndExtractDoubles(inner));

          boolean isNan = Double.isNaN(correlationResult);

          // Check if we need to propogate
          boolean propogate = true;

          if (isNan && !config.propogateNan) {
            propogate = false;
          }

          if (Math.abs(correlationResult) < config.minCorrValue) {
            propogate = false;
          }


          if (propogate) {

            Correlation.Builder correlation =
                Correlation.newBuilder().setTime(c.window().maxTimestamp().getMillis())
                    .setKey(WorkPacketUtils.createKey(outer.getKey(), inner.getKey()))
                    .setXCount(outer.getDataPointsList().size())
                    .setYCount(inner.getDataPointsList().size()).setWindow(c.window().toString());

            if (isNan) {

              // If we want to propogate NaN then set value to 1 and move forward
              correlationResult = 1d;
              correlation.setIsNaN(true);
            }

            correlation.setValue(correlationResult);

            if (config.includeUnderlying) {
              correlation.addAllXValues(outer.getDataPointsList());
              correlation.addAllYValues(inner.getDataPointsList());
            }

            c.output(correlation.build());
          }

        }
      }
    }
  }
}
