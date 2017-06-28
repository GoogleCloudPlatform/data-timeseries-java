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

package com.google.cloud.solutions.samples.timeseries.dataflow.application.workpackets;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.solutions.samples.timeseries.dataflow.application.proto.TimeSeriesProtos.WorkPacketConfig;
import com.google.cloud.solutions.samples.timeseries.dataflow.application.proto.TimeSeriesProtos.WorkPacketKey;
import com.google.protobuf.ProtocolStringList;


/**
 * Iterate through the keys and build the key list and partition space
 */
@SuppressWarnings("serial")
public class CreatePartitionKeysDoFn extends DoFn<WorkPacketConfig, Map<Integer, List<WorkPacketKey>>>
		implements DoFn.RequiresWindowAccess, Serializable {

	private static final Logger LOG = LoggerFactory.getLogger(CreatePartitionKeysDoFn.class);

	@Override
	public void processElement(DoFn<WorkPacketConfig, Map<Integer, List<WorkPacketKey>>>.ProcessContext c)
			throws Exception {

		long keyCount = 0;

		// Now we build the cartesian join and emit each pair forward, we ignore
		// self joins and transitive joins {a,b,c} become {a-b,a-c,b-c}

		Set<String> transList = new HashSet<String>();

		Map<Integer, List<WorkPacketKey>> keysForPartition = new HashMap<>();

		ProtocolStringList keyList = c.element().getKeysList();

		for (String outer : keyList) {

			for (String inner : keyList) {

				String key = WorkPacketUtils.createKey(outer, inner);

				if (!outer.equals(inner) && !transList.contains(key)) {

					int partition = WorkPacketUtils.getMyPartitions(c.element(), key);

					++keyCount;
					if (!keysForPartition.containsKey(partition)) {
						keysForPartition.put(partition, new ArrayList<WorkPacketKey>());
					}

					keysForPartition.get(partition)
							.add(WorkPacketKey.newBuilder().setKey1(inner).setKey2(outer).build());

					transList.add(key);
				}
			}
		}

		// TODO: Check for empty list
		c.output(keysForPartition);

		LOG.info(String.format("Number of Keys was %s Number of partitions are %s", keyCount,
				c.element().getPartitionLength()));
	}

}
