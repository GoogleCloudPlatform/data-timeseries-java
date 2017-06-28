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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import com.google.cloud.solutions.samples.timeseries.dataflow.application.proto.TimeSeriesProtos.WorkDataPoint;
import com.google.cloud.solutions.samples.timeseries.dataflow.application.proto.TimeSeriesProtos.WorkPacket;




/**
 * Utilities class used for Computations
 */
public class ComputationUtils {

  /**
   * Sort the contents of a WorkPacket by Time and return array of double[] which is needed by the
   * Math functions used downstream
   * 
   * @param workPacket
   * @return Sorted Array of values from the WorkPacket
   */
  public static double[] sortAndExtractDoubles(WorkPacket workPacket) {

    List<WorkDataPoint> wdpList = new ArrayList<WorkDataPoint>(workPacket.getDataPointsList());

    Collections.sort(wdpList, new TimeSeriesComparator());

    double[] sortedValues = new double[wdpList.size()];

    for (int i = 0; i < wdpList.size(); i++) {
      sortedValues[i] = wdpList.get(i).getValue();
    }

    return sortedValues;

  }

  private static class TimeSeriesComparator implements Comparator<WorkDataPoint> {
    @Override
    public int compare(WorkDataPoint o1, WorkDataPoint o2) {
      return Double.compare(o1.getTime(), o2.getTime());
    }
  }

}
