/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.rest.messages.job;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link DdpsChannelInfo}. */
class DdpsChannelInfoTest {

    @Test
    void testGroupsMetricsByDataSet() {
        Map<String, String> metrics = new HashMap<>();
        metrics.put("ddps.aa.consumerParallelism", "2");
        metrics.put("ddps.aa.targets", "0,1");
        metrics.put("ddps.aa.records", "10,20");
        metrics.put("ddps.aa.bytes", "100,200");
        metrics.put("ddps.aa.queuedBuffers", "0,3");
        metrics.put("ddps.bb.targets", "1");
        metrics.put("ddps.bb.records", "5");
        metrics.put("numRecordsOut", "35");

        Map<String, DdpsChannelInfo> channels = DdpsChannelInfo.fromMetrics(metrics);

        assertThat(channels).containsOnlyKeys("aa", "bb");
        DdpsChannelInfo aa = channels.get("aa");
        assertThat(aa.getConsumerParallelism()).isEqualTo(2);
        assertThat(aa.getTargets()).containsExactly(0, 1);
        assertThat(aa.getRecords()).containsExactly(10L, 20L);
        assertThat(aa.getBytes()).containsExactly(100L, 200L);
        assertThat(aa.getQueuedBuffers()).containsExactly(0, 3);
        DdpsChannelInfo bb = channels.get("bb");
        assertThat(bb.getRecords()).containsExactly(5L);
        assertThat(bb.getBytes()).isNull();
    }

    @Test
    void testNoDdpsMetrics() {
        Map<String, String> metrics = new HashMap<>();
        metrics.put("numRecordsOut", "35");
        assertThat(DdpsChannelInfo.fromMetrics(metrics)).isNull();
    }

    @Test
    void testMalformedValueIsDropped() {
        Map<String, String> metrics = new HashMap<>();
        metrics.put("ddps.aa.records", "10,x");
        metrics.put("ddps.aa.targets", "0,1");
        DdpsChannelInfo aa = DdpsChannelInfo.fromMetrics(metrics).get("aa");
        assertThat(aa.getRecords()).isNull();
        assertThat(aa.getTargets()).containsExactly(0, 1);
    }
}
