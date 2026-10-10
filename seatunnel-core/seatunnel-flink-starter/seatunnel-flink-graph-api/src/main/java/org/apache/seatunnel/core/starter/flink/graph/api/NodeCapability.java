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

package org.apache.seatunnel.core.starter.flink.graph.api;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;

/** Version-neutral capability descriptor used before a connector is initialized. */
public final class NodeCapability {

    public enum ValueKind {
        DATA,
        CONTROL,
        COMPLETION
    }

    public enum ExecutionScope {
        FLINK_DATA,
        CLIENT_CONTROL
    }

    private final Set<ValueKind> acceptedInputKinds;
    private final Set<ValueKind> outputKinds;
    private final int minimumInputCount;
    private final int maximumInputCount;
    private final boolean supportsBatch;
    private final boolean producesCompletion;
    private final ExecutionScope executionScope;

    private NodeCapability(Builder builder) {
        this.acceptedInputKinds = immutableSet(builder.acceptedInputKinds);
        this.outputKinds = immutableSet(builder.outputKinds);
        if (builder.minimumInputCount < 0
                || builder.maximumInputCount < builder.minimumInputCount) {
            throw new IllegalArgumentException("invalid input count range");
        }
        this.minimumInputCount = builder.minimumInputCount;
        this.maximumInputCount = builder.maximumInputCount;
        this.supportsBatch = builder.supportsBatch;
        this.producesCompletion = builder.producesCompletion;
        this.executionScope = builder.executionScope;
    }

    public Set<ValueKind> acceptedInputKinds() {
        return acceptedInputKinds;
    }

    public Set<ValueKind> outputKinds() {
        return outputKinds;
    }

    public int minimumInputCount() {
        return minimumInputCount;
    }

    public int maximumInputCount() {
        return maximumInputCount;
    }

    public boolean supportsBatch() {
        return supportsBatch;
    }

    public boolean producesCompletion() {
        return producesCompletion;
    }

    public ExecutionScope executionScope() {
        return executionScope;
    }

    public static Builder builder() {
        return new Builder();
    }

    private static Set<ValueKind> immutableSet(Set<ValueKind> values) {
        EnumSet<ValueKind> copy =
                values.isEmpty() ? EnumSet.noneOf(ValueKind.class) : EnumSet.copyOf(values);
        return Collections.unmodifiableSet(copy);
    }

    public static final class Builder {
        private final Set<ValueKind> acceptedInputKinds = EnumSet.noneOf(ValueKind.class);
        private final Set<ValueKind> outputKinds = EnumSet.noneOf(ValueKind.class);
        private int minimumInputCount;
        private int maximumInputCount = Integer.MAX_VALUE;
        private boolean supportsBatch;
        private boolean producesCompletion;
        private ExecutionScope executionScope = ExecutionScope.FLINK_DATA;

        public Builder acceptsInput(ValueKind kind) {
            acceptedInputKinds.add(kind);
            return this;
        }

        public Builder produces(ValueKind kind) {
            outputKinds.add(kind);
            return this;
        }

        public Builder inputCount(int minimum, int maximum) {
            minimumInputCount = minimum;
            maximumInputCount = maximum;
            return this;
        }

        public Builder supportsBatch(boolean value) {
            supportsBatch = value;
            return this;
        }

        public Builder producesCompletion(boolean value) {
            producesCompletion = value;
            return this;
        }

        public Builder executionScope(ExecutionScope value) {
            if (value == null) {
                throw new IllegalArgumentException("execution scope must not be null");
            }
            executionScope = value;
            return this;
        }

        public NodeCapability build() {
            return new NodeCapability(this);
        }
    }
}
