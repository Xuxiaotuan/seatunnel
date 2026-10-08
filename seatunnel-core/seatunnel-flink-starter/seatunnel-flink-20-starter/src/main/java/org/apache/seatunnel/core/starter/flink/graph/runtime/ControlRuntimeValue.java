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

package org.apache.seatunnel.core.starter.flink.graph.runtime;

import org.apache.seatunnel.core.starter.flink.graph.api.NodeCapability;

/** A completed client-side action result. */
public final class ControlRuntimeValue implements GraphRuntimeValue {

    private final String actionId;

    public ControlRuntimeValue(String actionId) {
        if (actionId == null || actionId.trim().isEmpty()) {
            throw new IllegalArgumentException("actionId must not be empty");
        }
        this.actionId = actionId;
    }

    @Override
    public NodeCapability.ValueKind kind() {
        return NodeCapability.ValueKind.CONTROL;
    }

    public String actionId() {
        return actionId;
    }
}
