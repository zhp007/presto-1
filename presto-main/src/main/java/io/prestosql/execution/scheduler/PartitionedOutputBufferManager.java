/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.prestosql.execution.scheduler;

import com.google.common.collect.ImmutableMap;
import io.prestosql.execution.buffer.OutputBuffers;
import io.prestosql.execution.buffer.OutputBuffers.OutputBufferId;
import io.prestosql.sql.planner.PartitioningHandle;

import javax.annotation.concurrent.ThreadSafe;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.execution.buffer.OutputBuffers.createInitialEmptyOutputBuffers;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class PartitionedOutputBufferManager
        implements OutputBufferManager
{
    private final Map<OutputBufferId, Integer> outputBuffers;

    public PartitionedOutputBufferManager(PartitioningHandle partitioningHandle, int partitionCount, Consumer<OutputBuffers> outputBufferTarget)
    {
        checkArgument(partitionCount >= 1, "partitionCount must be at least 1");

        ImmutableMap.Builder<OutputBufferId, Integer> partitions = ImmutableMap.builder();
        for (int partition = 0; partition < partitionCount; partition++) {
            partitions.put(new OutputBufferId(partition), partition);
        }

        OutputBuffers outputBuffers = createInitialEmptyOutputBuffers(requireNonNull(partitioningHandle, "partitioningHandle is null"))
                // 每次调用withBuffers(), version + 1
                .withBuffers(partitions.build())
                .withNoMoreBufferIds();
        outputBufferTarget.accept(outputBuffers);

        this.outputBuffers = outputBuffers.getBuffers();
    }

    /**
     * 这个方法只是验证 newBuffers 提供的 OutputBufferId 在 partitionCount 的范围内，partitionCount在创建PartitionedOutputBufferManager
     * 的时候就指定了，这里的 outputBuffers 只是 id -> int (partition channel)的映射
     */
    @Override
    public void addOutputBuffers(List<OutputBufferId> newBuffers, boolean noMoreBuffers)
    {
        // All buffers are created in the constructor, so just validate that this isn't
        // a request to add a new buffer
        for (OutputBufferId newBuffer : newBuffers) {
            Integer existingBufferId = outputBuffers.get(newBuffer);
            if (existingBufferId == null) {
                throw new IllegalStateException("Unexpected new output buffer " + newBuffer);
            }
            if (newBuffer.getId() != existingBufferId) {
                throw new IllegalStateException("newOutputBuffers has changed the assignment for task " + newBuffer);
            }
        }
    }
}
