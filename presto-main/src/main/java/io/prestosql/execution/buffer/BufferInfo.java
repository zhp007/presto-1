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
package io.prestosql.execution.buffer;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.prestosql.execution.buffer.OutputBuffers.OutputBufferId;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * BufferInfo: OutputBufferId 对应的 ClientBuffer 包含多少 buffered_page, sent_page
 * PageBufferInfo: 每个BufferInfo有一个PageBufferInfo，属于哪个partition (OutputBufferId -> id)，有多少buffered_page,
 * added_page，有多少行
 *
 * 每个OutputBuffer (interface)，都包含一个OutputBuffers
 * 一对多：OutputBuffers -> map(OutputBufferId -> partition)
 * 一一对应：OutputBufferId (partition) -> ClientBuffer -> 包含BufferInfo, PageBufferInfo
 */
public class BufferInfo
{
    private final OutputBufferId bufferId;
    private final boolean finished;
    private final int bufferedPages;

    private final long pagesSent;
    private final PageBufferInfo pageBufferInfo;

    @JsonCreator
    public BufferInfo(
            @JsonProperty("bufferId") OutputBufferId bufferId,
            @JsonProperty("finished") boolean finished,
            @JsonProperty("bufferedPages") int bufferedPages,
            @JsonProperty("pagesSent") long pagesSent,
            @JsonProperty("pageBufferInfo") PageBufferInfo pageBufferInfo)
    {
        checkArgument(bufferedPages >= 0, "bufferedPages must be >= 0");
        checkArgument(pagesSent >= 0, "pagesSent must be >= 0");

        this.bufferId = requireNonNull(bufferId, "bufferId is null");
        this.pagesSent = pagesSent;
        this.pageBufferInfo = requireNonNull(pageBufferInfo, "pageBufferInfo is null");
        this.finished = finished;
        this.bufferedPages = bufferedPages;
    }

    @JsonProperty
    public OutputBufferId getBufferId()
    {
        return bufferId;
    }

    @JsonProperty
    public boolean isFinished()
    {
        return finished;
    }

    @JsonProperty
    public int getBufferedPages()
    {
        return bufferedPages;
    }

    @JsonProperty
    public long getPagesSent()
    {
        return pagesSent;
    }

    @JsonProperty
    public PageBufferInfo getPageBufferInfo()
    {
        return pageBufferInfo;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BufferInfo that = (BufferInfo) o;
        return Objects.equals(finished, that.finished) &&
                Objects.equals(bufferedPages, that.bufferedPages) &&
                Objects.equals(pagesSent, that.pagesSent) &&
                Objects.equals(bufferId, that.bufferId) &&
                Objects.equals(pageBufferInfo, that.pageBufferInfo);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(bufferId, finished, bufferedPages, pagesSent, pageBufferInfo);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("bufferId", bufferId)
                .add("finished", finished)
                .add("bufferedPages", bufferedPages)
                .add("pagesSent", pagesSent)
                .add("pageBufferInfo", pageBufferInfo)
                .toString();
    }
}
