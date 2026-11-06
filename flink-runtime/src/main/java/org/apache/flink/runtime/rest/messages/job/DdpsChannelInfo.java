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

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude.Include;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.function.Function;

/**
 * DDPS: output of one subtask towards one data set, built from the {@code ddps.<dataSetId>.*}
 * metrics registered by {@code DdpsChannelMetrics}.
 *
 * <p>{@code targets} has one entry per consumer subtask this subtask sends to, keyed by the
 * consumer's subtask index, so every entry is one edge of the job graph. This mirrors the
 * per-source objects of the DDPS Spark fork ({@code sourceMetrics}).
 */
public class DdpsChannelInfo {

    private static final String PREFIX = "ddps.";

    private static final String METRIC_CONSUMER_PARALLELISM = "consumerParallelism";
    private static final String METRIC_TARGETS = "targets";
    private static final String METRIC_RECORDS = "records";
    private static final String METRIC_BYTES = "bytes";
    private static final String METRIC_QUEUED_BUFFERS = "queuedBuffers";

    public static final String FIELD_NAME_CONSUMER_VERTEX_ID = "consumerVertexId";
    public static final String FIELD_NAME_CONSUMER_PARALLELISM = "consumerParallelism";
    public static final String FIELD_NAME_TARGETS = "targets";

    @JsonProperty(FIELD_NAME_CONSUMER_VERTEX_ID)
    @JsonInclude(Include.NON_NULL)
    @Nullable
    private final String consumerVertexId;

    @JsonProperty(FIELD_NAME_CONSUMER_PARALLELISM)
    @JsonInclude(Include.NON_NULL)
    @Nullable
    private final Integer consumerParallelism;

    @JsonProperty(FIELD_NAME_TARGETS)
    private final Map<String, Target> targets;

    @JsonCreator
    public DdpsChannelInfo(
            @JsonProperty(FIELD_NAME_CONSUMER_VERTEX_ID) @Nullable String consumerVertexId,
            @JsonProperty(FIELD_NAME_CONSUMER_PARALLELISM) @Nullable Integer consumerParallelism,
            @JsonProperty(FIELD_NAME_TARGETS) @Nullable Map<String, Target> targets) {
        this.consumerVertexId = consumerVertexId;
        this.consumerParallelism = consumerParallelism;
        this.targets = targets == null ? new LinkedHashMap<>() : targets;
    }

    /** DDPS: traffic of one edge, from this subtask to one consumer subtask. */
    public static class Target {

        public static final String FIELD_NAME_RECORDS = "records";
        public static final String FIELD_NAME_BYTES = "bytes";
        public static final String FIELD_NAME_QUEUED_BUFFERS = "queuedBuffers";

        @JsonProperty(FIELD_NAME_RECORDS)
        @JsonInclude(Include.NON_NULL)
        @Nullable
        private final Long records;

        @JsonProperty(FIELD_NAME_BYTES)
        @JsonInclude(Include.NON_NULL)
        @Nullable
        private final Long bytes;

        @JsonProperty(FIELD_NAME_QUEUED_BUFFERS)
        @JsonInclude(Include.NON_NULL)
        @Nullable
        private final Integer queuedBuffers;

        @JsonCreator
        public Target(
                @JsonProperty(FIELD_NAME_RECORDS) @Nullable Long records,
                @JsonProperty(FIELD_NAME_BYTES) @Nullable Long bytes,
                @JsonProperty(FIELD_NAME_QUEUED_BUFFERS) @Nullable Integer queuedBuffers) {
            this.records = records;
            this.bytes = bytes;
            this.queuedBuffers = queuedBuffers;
        }

        @Nullable
        public Long getRecords() {
            return records;
        }

        @Nullable
        public Long getBytes() {
            return bytes;
        }

        @Nullable
        public Integer getQueuedBuffers() {
            return queuedBuffers;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Target that = (Target) o;
            return Objects.equals(records, that.records)
                    && Objects.equals(bytes, that.bytes)
                    && Objects.equals(queuedBuffers, that.queuedBuffers);
        }

        @Override
        public int hashCode() {
            return Objects.hash(records, bytes, queuedBuffers);
        }
    }

    /**
     * Groups the {@code ddps.<dataSetId>.<name>} entries of a subtask's metrics by data set and
     * turns the per-channel lists into one {@link Target} per consumer subtask.
     *
     * @param metrics the subtask's metrics
     * @param consumerByDataSet data set id to consuming vertex id, or {@code null} if unknown
     * @return data set id to channel info, or {@code null} if the subtask has no such metrics
     */
    @Nullable
    public static Map<String, DdpsChannelInfo> fromMetrics(
            Map<String, String> metrics, @Nullable Map<String, String> consumerByDataSet) {
        Map<String, Map<String, String>> byDataSet = new TreeMap<>();
        for (Map.Entry<String, String> e : metrics.entrySet()) {
            String key = e.getKey();
            if (!key.startsWith(PREFIX)) {
                continue;
            }
            int dot = key.indexOf('.', PREFIX.length());
            if (dot < 0) {
                continue;
            }
            byDataSet
                    .computeIfAbsent(key.substring(PREFIX.length(), dot), k -> new TreeMap<>())
                    .put(key.substring(dot + 1), e.getValue());
        }
        if (byDataSet.isEmpty()) {
            return null;
        }

        Map<String, DdpsChannelInfo> result = new TreeMap<>();
        for (Map.Entry<String, Map<String, String>> e : byDataSet.entrySet()) {
            Map<String, String> m = e.getValue();
            result.put(
                    e.getKey(),
                    new DdpsChannelInfo(
                            consumerByDataSet == null ? null : consumerByDataSet.get(e.getKey()),
                            parseInt(m.get(METRIC_CONSUMER_PARALLELISM)),
                            toTargets(
                                    parseList(m.get(METRIC_TARGETS), Integer::valueOf),
                                    parseList(m.get(METRIC_RECORDS), Long::valueOf),
                                    parseList(m.get(METRIC_BYTES), Long::valueOf),
                                    parseList(m.get(METRIC_QUEUED_BUFFERS), Integer::valueOf))));
        }
        return result;
    }

    /** Builds one entry per channel; without {@code targets} the channel index is the key. */
    private static Map<String, Target> toTargets(
            @Nullable List<Integer> targets,
            @Nullable List<Long> records,
            @Nullable List<Long> bytes,
            @Nullable List<Integer> queuedBuffers) {
        int channels =
                Math.max(
                        Math.max(size(targets), size(records)),
                        Math.max(size(bytes), size(queuedBuffers)));
        Map<String, Target> out = new LinkedHashMap<>();
        for (int k = 0; k < channels; k++) {
            Integer target = at(targets, k);
            String key = target == null ? Integer.toString(k) : target.toString();
            Target prev = out.get(key);
            Target cur = new Target(at(records, k), at(bytes, k), at(queuedBuffers, k));
            out.put(key, prev == null ? cur : merge(prev, cur));
        }
        return out;
    }

    /** Two channels to the same consumer (not expected in Flink 2.3) are added up. */
    private static Target merge(Target a, Target b) {
        return new Target(
                sum(a.records, b.records),
                sum(a.bytes, b.bytes),
                a.queuedBuffers == null || b.queuedBuffers == null
                        ? null
                        : a.queuedBuffers + b.queuedBuffers);
    }

    @Nullable
    private static Long sum(@Nullable Long a, @Nullable Long b) {
        return a == null || b == null ? null : a + b;
    }

    private static int size(@Nullable List<?> list) {
        return list == null ? 0 : list.size();
    }

    @Nullable
    private static <T> T at(@Nullable List<T> list, int index) {
        return list == null || index >= list.size() ? null : list.get(index);
    }

    @Nullable
    private static Integer parseInt(@Nullable String value) {
        if (value == null) {
            return null;
        }
        try {
            return Integer.valueOf(value.trim());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    @Nullable
    private static <T> List<T> parseList(@Nullable String value, Function<String, T> parser) {
        if (value == null) {
            return null;
        }
        List<T> out = new ArrayList<>();
        if (value.isEmpty()) {
            return out;
        }
        try {
            for (String part : value.split(",")) {
                out.add(parser.apply(part.trim()));
            }
        } catch (NumberFormatException e) {
            return null;
        }
        return out;
    }

    @Nullable
    public String getConsumerVertexId() {
        return consumerVertexId;
    }

    @Nullable
    public Integer getConsumerParallelism() {
        return consumerParallelism;
    }

    public Map<String, Target> getTargets() {
        return targets;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DdpsChannelInfo that = (DdpsChannelInfo) o;
        return Objects.equals(consumerVertexId, that.consumerVertexId)
                && Objects.equals(consumerParallelism, that.consumerParallelism)
                && Objects.equals(targets, that.targets);
    }

    @Override
    public int hashCode() {
        return Objects.hash(consumerVertexId, consumerParallelism, targets);
    }
}
