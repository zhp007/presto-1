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

import io.prestosql.execution.RemoteTask;
import io.prestosql.metadata.InternalNode;
import io.prestosql.metadata.Split;

import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

public class DynamicSplitPlacementPolicy
        implements SplitPlacementPolicy
{
    private final NodeSelector nodeSelector;
    private final Supplier<? extends List<RemoteTask>> remoteTasks;

    /*
    * 只在SqlQueryScheduler创建source stage的时候用到
    *
    * 这里的supplier实际上是SqlStageExecution.getAllTasks(), 使用supplier.get()确保拿到最新的task列表
    * */
    public DynamicSplitPlacementPolicy(NodeSelector nodeSelector, Supplier<? extends List<RemoteTask>> remoteTasks)
    {
        this.nodeSelector = requireNonNull(nodeSelector, "nodeSelector is null");
        this.remoteTasks = requireNonNull(remoteTasks, "remoteTasks is null");
    }

    @Override
    public SplitPlacementResult computeAssignments(Set<Split> splits)
    {
        return nodeSelector.computeAssignments(splits, remoteTasks.get());
    }

    @Override
    public void lockDownNodes()
    {
        nodeSelector.lockDownNodes();
    }

    @Override
    public List<InternalNode> allNodes()
    {
        return nodeSelector.allNodes();
    }
}
