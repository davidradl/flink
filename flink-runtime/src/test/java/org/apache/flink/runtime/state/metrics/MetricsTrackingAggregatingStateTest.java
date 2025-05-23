/*

* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.

*/

package org.apache.flink.runtime.state.metrics;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.VoidNamespace;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.concurrent.ThreadLocalRandom;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link MetricsTrackingAggregatingState}. */
class MetricsTrackingAggregatingStateTest extends MetricsTrackingStateTestBase<Integer> {
    @Override
    @SuppressWarnings("unchecked")
    AggregatingStateDescriptor<Long, Long, Long> getStateDescriptor() {
        return new AggregatingStateDescriptor<>(
                "aggregate",
                new AggregateFunction<Long, Long, Long>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Long createAccumulator() {
                        return 0L;
                    }

                    @Override
                    public Long add(Long value, Long accumulator) {
                        return value + accumulator;
                    }

                    @Override
                    public Long getResult(Long accumulator) {
                        return accumulator;
                    }

                    @Override
                    public Long merge(Long a, Long b) {
                        return a + b;
                    }
                },
                Long.class);
    }

    @Override
    TypeSerializer<Integer> getKeySerializer() {
        return IntSerializer.INSTANCE;
    }

    @Override
    void setCurrentKey(AbstractKeyedStateBackend<Integer> keyedBackend) {
        keyedBackend.setCurrentKey(1);
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    void testLatencyTrackingAggregatingState() throws Exception {
        AbstractKeyedStateBackend<Integer> keyedBackend = createKeyedBackend(getKeySerializer());
        try {
            MetricsTrackingAggregatingState<Integer, VoidNamespace, Long, Long, Long>
                    latencyTrackingState =
                            (MetricsTrackingAggregatingState)
                                    createMetricsTrackingState(keyedBackend, getStateDescriptor());
            latencyTrackingState.setCurrentNamespace(VoidNamespace.INSTANCE);
            MetricsTrackingAggregatingState.AggregatingStateMetrics latencyTrackingStateMetric =
                    latencyTrackingState.getLatencyTrackingStateMetric();

            assertThat(latencyTrackingStateMetric.getAddCount()).isZero();
            assertThat(latencyTrackingStateMetric.getGetCount()).isZero();
            assertThat(latencyTrackingStateMetric.getMergeNamespaceCount()).isZero();

            setCurrentKey(keyedBackend);
            ThreadLocalRandom random = ThreadLocalRandom.current();
            for (int index = 1; index <= SAMPLE_INTERVAL; index++) {
                int expectedResult = index == SAMPLE_INTERVAL ? 0 : index;
                latencyTrackingState.add(random.nextLong());
                assertThat(latencyTrackingStateMetric.getAddCount()).isEqualTo(expectedResult);

                latencyTrackingState.get();
                assertThat(latencyTrackingStateMetric.getGetCount()).isEqualTo(expectedResult);

                latencyTrackingState.mergeNamespaces(
                        VoidNamespace.INSTANCE, Collections.emptyList());
                assertThat(latencyTrackingStateMetric.getMergeNamespaceCount())
                        .isEqualTo(expectedResult);
            }
        } finally {
            if (keyedBackend != null) {
                keyedBackend.close();
                keyedBackend.dispose();
            }
        }
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    void testSizeTrackingAggregatingState() throws Exception {
        AbstractKeyedStateBackend<Integer> keyedBackend = createKeyedBackend(getKeySerializer());
        try {
            MetricsTrackingAggregatingState<Integer, VoidNamespace, Long, Long, Long>
                    sizeTrackingState =
                            (MetricsTrackingAggregatingState)
                                    createMetricsTrackingState(keyedBackend, getStateDescriptor());
            sizeTrackingState.setCurrentNamespace(VoidNamespace.INSTANCE);
            MetricsTrackingAggregatingState.AggregatingStateMetrics sizeTrackingStateMetric =
                    sizeTrackingState.getSizeTrackingStateMetric();

            assertThat(sizeTrackingStateMetric.getAddCount()).isZero();
            assertThat(sizeTrackingStateMetric.getGetCount()).isZero();
            assertThat(sizeTrackingStateMetric.getMergeNamespaceCount()).isZero();

            setCurrentKey(keyedBackend);
            ThreadLocalRandom random = ThreadLocalRandom.current();
            for (int index = 1; index <= SAMPLE_INTERVAL; index++) {
                int expectedResult = index == SAMPLE_INTERVAL ? 0 : index;
                sizeTrackingState.add(random.nextLong());
                assertThat(sizeTrackingStateMetric.getAddCount()).isEqualTo(expectedResult);

                sizeTrackingState.get();
                assertThat(sizeTrackingStateMetric.getGetCount()).isEqualTo(expectedResult);
            }
        } finally {
            if (keyedBackend != null) {
                keyedBackend.close();
                keyedBackend.dispose();
            }
        }
    }
}
