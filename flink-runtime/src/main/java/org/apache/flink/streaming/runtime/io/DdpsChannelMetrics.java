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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.io.network.api.writer.RecordWriter;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.partition.BufferWritingResultPartition;
import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;

import java.util.function.IntToLongFunction;

/**
 * DDPS: per-channel output metrics of one task output, used by the DDPS observability project to
 * draw measured subtask-to-subtask edges.
 *
 * <p>For every non-chained output the task registers, under its IO metric group:
 *
 * <pre>
 *   ddps.&lt;dataSetId&gt;.consumerParallelism   parallelism of the consuming vertex
 *   ddps.&lt;dataSetId&gt;.targets               consumer subtask index of each channel, e.g. "0,1,2,3"
 *   ddps.&lt;dataSetId&gt;.records               user records sent on each channel
 *   ddps.&lt;dataSetId&gt;.bytes                 serialized bytes sent on each channel
 *   ddps.&lt;dataSetId&gt;.queuedBuffers         buffers currently queued on each channel
 * </pre>
 *
 * <p>The per-channel values are comma separated, one value per channel (subpartition), in the order
 * of {@code targets}. One string gauge per quantity keeps the number of metrics independent of the
 * parallelism.
 */
public final class DdpsChannelMetrics {

    public static final String GROUP = "ddps";
    public static final String CONSUMER_PARALLELISM = "consumerParallelism";
    public static final String TARGETS = "targets";
    public static final String RECORDS = "records";
    public static final String BYTES = "bytes";
    public static final String QUEUED_BUFFERS = "queuedBuffers";

    private DdpsChannelMetrics() {}

    /**
     * Registers the metrics of one output.
     *
     * @param ioMetricGroup the task's IO metric group
     * @param dataSetId the data set this output produces
     * @param recordWriter the writer of this output, counts the records per channel
     * @param partitionWriter the partition of this output, holds the bytes and queues per channel
     * @param producerIndex index of this subtask
     * @param producerParallelism parallelism of this vertex
     * @param consumerParallelism parallelism of the consuming vertex
     */
    public static void register(
            MetricGroup ioMetricGroup,
            IntermediateDataSetID dataSetId,
            RecordWriter<?> recordWriter,
            ResultPartitionWriter partitionWriter,
            int producerIndex,
            int producerParallelism,
            int consumerParallelism) {

        final int channels = partitionWriter.getNumberOfSubpartitions();
        final String targets =
                join(
                        computeTargets(
                                producerIndex, producerParallelism, consumerParallelism, channels));

        MetricGroup group = ioMetricGroup.addGroup(GROUP).addGroup(dataSetId.toString());
        group.gauge(CONSUMER_PARALLELISM, (Gauge<Integer>) () -> consumerParallelism);
        group.gauge(TARGETS, (Gauge<String>) () -> targets);
        group.gauge(
                RECORDS, (Gauge<String>) () -> join(channels, recordWriter::getDdpsRecordsEmitted));

        if (partitionWriter instanceof BufferWritingResultPartition) {
            BufferWritingResultPartition partition = (BufferWritingResultPartition) partitionWriter;
            group.gauge(
                    BYTES, (Gauge<String>) () -> join(channels, partition::getDdpsWrittenBytes));
        }
        if (partitionWriter instanceof ResultPartition) {
            ResultPartition partition = (ResultPartition) partitionWriter;
            group.gauge(
                    QUEUED_BUFFERS,
                    (Gauge<String>) () -> join(channels, partition::getNumberOfQueuedBuffers));
        }
    }

    /**
     * Consumer subtask index of every channel (subpartition) of one producer subtask.
     *
     * <p>All-to-all edges (HASH, REBALANCE, SHUFFLE, BROADCAST, GLOBAL, custom) have one channel
     * per consumer, and channel {@code k} goes to consumer {@code k}. Pointwise edges (FORWARD,
     * RESCALE) have fewer channels and follow {@code
     * VertexInputInfoComputationUtils#computeVertexInputInfoForPointwise}: with at least as many
     * producers as consumers, a group of producers feeds one consumer through a single channel;
     * with fewer, each producer feeds a contiguous range of consumers.
     *
     * <p>The edge type is derived from the channel count, not from the partitioner, because {@code
     * StreamTask} replaces a FORWARD partitioner with REBALANCE on single-channel outputs. If the
     * result does not match the number of channels, the identity mapping is used.
     */
    @VisibleForTesting
    static int[] computeTargets(
            int producerIndex, int producerParallelism, int consumerParallelism, int channels) {

        int[] identity = new int[channels];
        for (int k = 0; k < channels; k++) {
            identity[k] = k;
        }
        if (channels == consumerParallelism || producerParallelism <= 0) {
            return identity;
        }

        int s = producerParallelism;
        int t = consumerParallelism;
        if (s >= t) {
            // consumer c reads producers [c*s/t, (c+1)*s/t)
            if (channels != 1) {
                return identity;
            }
            for (int c = 0; c < t; c++) {
                if (c * s / t <= producerIndex && producerIndex < (c + 1) * s / t) {
                    return new int[] {c};
                }
            }
            return identity;
        }

        // producer i feeds consumers [ceil(i*t/s), ceil((i+1)*t/s))
        int start = (producerIndex * t + s - 1) / s;
        int end = ((producerIndex + 1) * t + s - 1) / s;
        if (end - start != channels) {
            return identity;
        }
        int[] targets = new int[channels];
        for (int k = 0; k < channels; k++) {
            targets[k] = start + k;
        }
        return targets;
    }

    private static String join(int channels, IntToLongFunction value) {
        StringBuilder sb = new StringBuilder();
        for (int k = 0; k < channels; k++) {
            if (k > 0) {
                sb.append(',');
            }
            sb.append(value.applyAsLong(k));
        }
        return sb.toString();
    }

    private static String join(int[] values) {
        return join(values.length, k -> values[k]);
    }
}
