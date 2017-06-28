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

package com.google.cloud.solutions.samples.timeseries.dataflow.application.computation;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import org.apache.commons.math3.stat.correlation.PearsonsCorrelation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.cloud.solutions.samples.timeseries.dataflow.application.proto.TimeSeriesProtos.Correlation;
import com.google.cloud.solutions.samples.timeseries.dataflow.application.proto.TimeSeriesProtos.WorkDataPoint;
import com.google.cloud.solutions.samples.timeseries.dataflow.application.proto.TimeSeriesProtos.WorkPacket;
import com.google.cloud.solutions.samples.timeseries.dataflow.application.proto.TimeSeriesProtos.WorkPacketKey;
import com.google.cloud.solutions.samples.timeseries.dataflow.application.workpackets.WorkPacketUtils;
import com.google.protobuf.TextFormat;

@SuppressWarnings("serial")
public class PartitionedComputeCorrelationsDoFn extends DoFn<Integer, Correlation>
    implements Serializable, DoFn.RequiresWindowAccess

{

  private static final Logger LOG =
      LoggerFactory.getLogger(PartitionedComputeCorrelationsDoFn.class);

  PCollectionView<Map<String, WorkPacket>> sideInput;
  PCollectionView<Map<Integer, List<WorkPacketKey>>> keyMatrix;

  /**
   * @param config Configuration for the correlations
   * @param sideInput Sideinput to be used for the transform 
   * @param matrix Key Matrix
   */
  public PartitionedComputeCorrelationsDoFn(CorrelationParDoConfig config,
      PCollectionView<Map<String, WorkPacket>> sideInput,
      PCollectionView<Map<Integer, List<WorkPacketKey>>> matrix) {
    
    this.sideInput = sideInput;
    this.keyMatrix = matrix;
    this.config = config;
  }

  /**
   * 
   * Configuration parameters for the Correlation analysis Minimum threshold for forward default
   * is ABS(0.5) NAN are by default not propagated forward The underlying array is not Propagated
   * forward by default
   *
   */
  @SuppressWarnings("serial")
  public static class CorrelationParDoConfig implements Serializable {

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
     *        forward careful about turning this on as it will generate a lot of data downstream
     */
    public void setIncludeUnderlying(boolean includeUnderlying) {
      this.includeUnderlying = includeUnderlying;
    }

  }

  private CorrelationParDoConfig config = null;



  /**
   * Using the data in the sideinput compute the correlations
   */
  @Override
  public void processElement(DoFn<Integer, Correlation>.ProcessContext c) throws Exception {

    PearsonsCorrelation pearsonsCorrelation = new PearsonsCorrelation();


    if (c.sideInput(keyMatrix).containsKey(c.element())) {
 
      for (WorkPacketKey keys : c.sideInput(keyMatrix).get(c.element())) {

    	  // TODO : Rename key 1, Key 2 to more descriptive names 
        WorkPacket outer = c.sideInput(sideInput).get(keys.getKey1());
        WorkPacket inner = c.sideInput(sideInput).get(keys.getKey2());;

        boolean correlate = true;

        
        
         //  TODO: Create a shouldCorrelate function to clean up this flag 
         
         
        // Don't correlate if there is only 1 value in the dataset
        if (outer.getDataPointsList().size() < 2) {
          correlate = false;
        }

        if (outer.getDataPointsList().size() != inner.getDataPointsList().size()) {

          StringBuffer sb = new StringBuffer();

          sb.append(
              String.format("Key size mismatch between %s & %s", outer.getKey(), inner.getKey()));

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
