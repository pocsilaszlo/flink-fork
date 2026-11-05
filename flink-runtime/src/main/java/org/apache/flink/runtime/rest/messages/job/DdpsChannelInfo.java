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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.function.Function;

/**
 * DDPS: per-channel output of one subtask towards one data set, built from the {@code
 * ddps.<dataSetId>.*} metrics registered by {@code DdpsChannelMetrics}. All lists have one entry
 * per channel, in the same order; {@code targets} names the consumer subtask of each channel.
 */
public class DdpsChannelInfo {

    private static final String PREFIX = "ddps.";

    public static final String FIELD_NAME_CONSUMER_PARALLELISM = "consumerParallelism";
    public static final String FIELD_NAME_TARGETS = "targets";
    public static final String FIELD_NAME_RECORDS = "records";
    public static final String FIELD_NAME_BYTES = "bytes";
    public static final String FIELD_NAME_QUEUED_BUFFERS = "queuedBuffers";

    @JsonProperty(FIELD_NAME_CONSUMER_PARALLELISM)
    @JsonInclude(Include.NON_NULL)
    @Nullable
    private final Integer consumerParallelism;

    @JsonProperty(FIELD_NAME_TARGETS)
    @JsonInclude(Include.NON_NULL)
    @Nullable
    private final List<Integer> targets;

    @JsonProperty(FIELD_NAME_RECORDS)
    @JsonInclude(Include.NON_NULL)
    @Nullable
    private final List<Long> records;

    @JsonProperty(FIELD_NAME_BYTES)
    @JsonInclude(Include.NON_NULL)
    @Nullable
    private final List<Long> bytes;

    @JsonProperty(FIELD_NAME_QUEUED_BUFFERS)
    @JsonInclude(Include.NON_NULL)
    @Nullable
    private final List<Integer> queuedBuffers;

    @JsonCreator
    public DdpsChannelInfo(
            @JsonProperty(FIELD_NAME_CONSUMER_PARALLELISM) @Nullable Integer consumerParallelism,
            @JsonProperty(FIELD_NAME_TARGETS) @Nullable List<Integer> targets,
            @JsonProperty(FIELD_NAME_RECORDS) @Nullable List<Long> records,
            @JsonProperty(FIELD_NAME_BYTES) @Nullable List<Long> bytes,
            @JsonProperty(FIELD_NAME_QUEUED_BUFFERS) @Nullable List<Integer> queuedBuffers) {
        this.consumerParallelism = consumerParallelism;
        this.targets = targets;
        this.records = records;
        this.bytes = bytes;
        this.queuedBuffers = queuedBuffers;
    }

    /**
     * Groups the {@code ddps.<dataSetId>.<name>} entries of a subtask's metrics by data set.
     *
     * @return data set id to channel info, or {@code null} if the subtask has no such metrics
     */
    @Nullable
    public static Map<String, DdpsChannelInfo> fromMetrics(Map<String, String> metrics) {
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
                            parseInt(m.get(FIELD_NAME_CONSUMER_PARALLELISM)),
                            parseList(m.get(FIELD_NAME_TARGETS), Integer::valueOf),
                            parseList(m.get(FIELD_NAME_RECORDS), Long::valueOf),
                            parseList(m.get(FIELD_NAME_BYTES), Long::valueOf),
                            parseList(m.get(FIELD_NAME_QUEUED_BUFFERS), Integer::valueOf)));
        }
        return result;
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
    public Integer getConsumerParallelism() {
        return consumerParallelism;
    }

    @Nullable
    public List<Integer> getTargets() {
        return targets;
    }

    @Nullable
    public List<Long> getRecords() {
        return records;
    }

    @Nullable
    public List<Long> getBytes() {
        return bytes;
    }

    @Nullable
    public List<Integer> getQueuedBuffers() {
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
        DdpsChannelInfo that = (DdpsChannelInfo) o;
        return Objects.equals(consumerParallelism, that.consumerParallelism)
                && Objects.equals(targets, that.targets)
                && Objects.equals(records, that.records)
                && Objects.equals(bytes, that.bytes)
                && Objects.equals(queuedBuffers, that.queuedBuffers);
    }

    @Override
    public int hashCode() {
        return Objects.hash(consumerParallelism, targets, records, bytes, queuedBuffers);
    }
}
