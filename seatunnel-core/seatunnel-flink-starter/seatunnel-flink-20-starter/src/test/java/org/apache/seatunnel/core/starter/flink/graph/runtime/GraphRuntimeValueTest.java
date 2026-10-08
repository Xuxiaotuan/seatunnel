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

import org.apache.seatunnel.core.starter.flink.execution.DataStreamTableInfo;
import org.apache.seatunnel.core.starter.flink.graph.api.NodeCapability;
import org.apache.seatunnel.core.starter.flink.graph.validate.GraphValidationException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;

class GraphRuntimeValueTest {

    @Test
    void dataValueCarriesTableMetadataAndCannotBeCreatedFromControlValue() {
        DataStreamTableInfo tableInfo =
                new DataStreamTableInfo(null, Collections.emptyList(), "orders");
        DataRuntimeValue data = new DataRuntimeValue(tableInfo, true, true);

        Assertions.assertEquals(NodeCapability.ValueKind.DATA, data.kind());
        Assertions.assertSame(tableInfo, data.tableInfo());
        Assertions.assertTrue(data.bounded());
        Assertions.assertTrue(data.insertOnly());
        Assertions.assertThrows(
                GraphValidationException.class,
                () -> DataRuntimeValue.require(new ControlRuntimeValue("prepare"), "source"));
    }

    @Test
    void completionValueIsDistinctFromClientSideControlValue() {
        ControlRuntimeValue control = new ControlRuntimeValue("prepare");
        CompletionRuntimeValue completion = new CompletionRuntimeValue("sink");

        Assertions.assertEquals(NodeCapability.ValueKind.CONTROL, control.kind());
        Assertions.assertEquals(NodeCapability.ValueKind.COMPLETION, completion.kind());
        Assertions.assertEquals("prepare", control.actionId());
        Assertions.assertEquals("sink", completion.nodeId());
    }
}
