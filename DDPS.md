# Apache Flink fork for DDPS observability

This fork of [Apache Flink](https://github.com/apache/flink) adds **per-channel output metrics**
for the [DDPS observability](https://github.com/zzvara/ddps-observability) project. For every
network channel it reports how many records and bytes a producer subtask sent to each consumer
subtask and how many buffers are queued on it, so every subtask-to-subtask edge of a job graph is
*measured* instead of estimated. It is the Flink counterpart of the Spark fork
([nagyb3/spark-fork](https://github.com/nagyb3/spark-fork)).

| | |
|---|---|
| Base | tag `release-2.3.0` |
| Branch | `ddps/channel-metrics` |
| Scope | streaming jobs with pipelined partitions |

## What it adds

### Metrics (per task output)

Registered under each task's IO metric group, one group per output data set:

| Metric | Example | Meaning |
|---|---|---|
| `ddps.<dataSetId>.consumerParallelism` | `4` | parallelism of the consuming vertex |
| `ddps.<dataSetId>.targets` | `0,1,2,3` | consumer subtask of each channel |
| `ddps.<dataSetId>.records` | `52539,52540,52540,52540` | user records sent on each channel (watermarks excluded) |
| `ddps.<dataSetId>.bytes` | `1471124,…` | serialized bytes sent on each channel (records and stream elements) |
| `ddps.<dataSetId>.queuedBuffers` | `1,1,4,1` | buffers currently queued on each channel |

All per-channel values are comma separated, in the order of `targets`. They are available through
the standard metrics endpoint, e.g.
`GET /jobs/{jid}/vertices/{vid}/subtasks/{i}/metrics?get=ddps.<dataSetId>.records`.

### REST fields

* `GET /jobs/{jid}/plan`: every input has a `dataset` field, the id used in the metric names above.
* `GET /jobs/{jid}/vertices/{vid}` (and the subtask detail endpoints): every running subtask has a
  `channels` object, keyed by data set id:

```json
"channels": {
  "700e0079c8225e5f4cd5c9ebb8733a79": {
    "consumerParallelism": 4,
    "targets": [0, 1, 2, 3],
    "records": [20644, 12575, 44831, 27000],
    "bytes": [578036, 352104, 1255272, 756004],
    "queuedBuffers": [1, 1, 1, 1]
  }
}
```

One request per vertex therefore returns the nodes and all measured outgoing edges. The values
refresh every `metrics.fetcher.update-interval`; this fork sets it to 500 ms in the default
`config.yaml`, together with 4 task slots per TaskManager.

### Changed files

| File | Change |
|---|---|
| `RecordWriter`, `ChannelSelectorRecordWriter`, `BroadcastRecordWriter`, `AdaptiveLoadBasedRecordWriter` | count user records per channel |
| `BufferWritingResultPartition` | getter for the bytes written per channel |
| `streaming/runtime/io/DdpsChannelMetrics` (new) | registers the metrics, maps channels to consumer subtasks |
| `streaming/runtime/tasks/StreamTask` | calls the registration for every output |
| `JobPlanInfo`, `JsonPlanGenerator` | `dataset` field in the plan |
| `rest/messages/job/DdpsChannelInfo` (new), `SubtaskExecutionAttemptDetailsInfo` | `channels` field in the vertex and subtask responses |
| `flink-dist/src/main/resources/config.yaml` | 4 slots, 500 ms metric fetcher interval |
| `DdpsChannelMetricsTest`, `DdpsChannelInfoTest` (new) | unit tests |

Most additions are marked with a `DDPS` comment; the record counters live in the `ddpsRecordsPerSubpartition` field of `RecordWriter`.

## Build

Requires Java 17 or 21 (the build targets Java 17) and network access; Maven is downloaded by
`./mvnw`.

```bash
# First build (result in build-target/)
./mvnw clean install -DskipTests -Djdk21 -Pjava17-target -Dfast -T 1C

# After a change in flink-runtime
./mvnw install -DskipTests -Djdk21 -Pjava17-target -Dfast -pl flink-runtime
./mvnw clean install -DskipTests -Djdk21 -Pjava17-target -Dfast -pl flink-dist
```

Always rebuild `flink-dist` with `clean`: otherwise the shade plugin merges the previous
distribution jar first and the old versions of the changed classes win.

`install` also puts the forked jars into `~/.m2` under version `2.3.0`. To go back to stock Flink,
delete `~/.m2/repository/org/apache/flink/*/2.3.0`.

### Tests

```bash
./mvnw test -Djdk21 -Pjava17-target -pl flink-runtime \
  -Dtest='DdpsChannelMetricsTest,DdpsChannelInfoTest' -Dsurefire.failIfNoSpecifiedTests=false

# The REST API snapshot changes with the new fields; regenerate it once
./mvnw test -Djdk21 -Pjava17-target -pl flink-runtime-web \
  -Dtest=RuntimeRestAPIStabilityTest -Dgenerate-rest-snapshot
```

## Run

```bash
./build-target/bin/start-cluster.sh                  # Web UI and REST on http://localhost:8081
./build-target/bin/flink run -d <job>.jar
./build-target/bin/stop-cluster.sh
```

Build job jars with Flink in `provided` scope so the cluster's (forked) classes are used. In the
DDPS repository, `flink/check_channels.sh` prints the measured channels of a running job.

## Moving to a new Flink version

```bash
git fetch upstream --tags
git switch -c ddps/channel-metrics-<version> release-<version>
git cherry-pick <first DDPS commit>^..ddps/channel-metrics
```

Conflicts are most likely in `StreamTask.createRecordWriter` and the REST message classes.

## Limitations

* Chained operators have no channel, so there is no edge between them.
* Finished subtasks have no `channels`: the gauges are removed with the task.
* Batch execution with hybrid or blocking shuffle uses other partition classes; their bytes and
  queues are not reported (records still are).
* The `targets` mapping follows Flink 2.3's pointwise rule
  (`VertexInputInfoComputationUtils#computeVertexInputInfoForPointwise`); recheck it after a
  version change.
