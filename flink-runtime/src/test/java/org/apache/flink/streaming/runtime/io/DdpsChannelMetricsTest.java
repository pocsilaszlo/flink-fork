/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.io;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the channel to consumer mapping of {@link DdpsChannelMetrics}. */
class DdpsChannelMetricsTest {

    @Test
    void testAllToAllIsIdentity() {
        // HASH / REBALANCE 2 -> 4: one channel per consumer
        assertThat(DdpsChannelMetrics.computeTargets(1, 2, 4, 4)).containsExactly(0, 1, 2, 3);
    }

    @Test
    void testForwardSameParallelism() {
        // FORWARD 4 -> 4: producer i feeds consumer i through one channel
        for (int i = 0; i < 4; i++) {
            assertThat(DdpsChannelMetrics.computeTargets(i, 4, 4, 1)).containsExactly(i);
        }
    }

    @Test
    void testRescaleDown() {
        // RESCALE 4 -> 2: producers 0,1 feed consumer 0, producers 2,3 feed consumer 1
        assertThat(DdpsChannelMetrics.computeTargets(0, 4, 2, 1)).containsExactly(0);
        assertThat(DdpsChannelMetrics.computeTargets(1, 4, 2, 1)).containsExactly(0);
        assertThat(DdpsChannelMetrics.computeTargets(2, 4, 2, 1)).containsExactly(1);
        assertThat(DdpsChannelMetrics.computeTargets(3, 4, 2, 1)).containsExactly(1);
    }

    @Test
    void testRescaleUp() {
        // RESCALE 2 -> 4: producer 0 feeds consumers 0,1, producer 1 feeds consumers 2,3
        assertThat(DdpsChannelMetrics.computeTargets(0, 2, 4, 2)).containsExactly(0, 1);
        assertThat(DdpsChannelMetrics.computeTargets(1, 2, 4, 2)).containsExactly(2, 3);
    }

    @Test
    void testUnevenRescaleUp() {
        // RESCALE 2 -> 3: producer 0 feeds consumers 0,1, producer 1 feeds consumer 2
        assertThat(DdpsChannelMetrics.computeTargets(0, 2, 3, 2)).containsExactly(0, 1);
        assertThat(DdpsChannelMetrics.computeTargets(1, 2, 3, 1)).containsExactly(2);
    }

    @Test
    void testUnexpectedChannelCountFallsBackToIdentity() {
        assertThat(DdpsChannelMetrics.computeTargets(0, 2, 4, 3)).containsExactly(0, 1, 2);
    }
}
