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
package io.prestosql.spi.block;

import io.airlift.slice.Slice;

/**
 * BlockBuilder的数据保存在SliceOutput，SliceOutput本质是Slice的包装，而Slice则是byte[]
 *
 * 每个Type的write操作有两种：writeSlice(), writeXXX()，XXX表示不同的数据类型，对应的BlockBuilder的操作为：
 * Type.writeSlice() -> BlockBuilder.writeBytes(), Type.writeXXX() -> BlockBuilder.writeXXX()
 * 但是两者writeXXX()支持的数据类型不同，BlockBuilder由于是对Slice直接操作，操作的单位为byte，所以它只支持写定长且长度为byte的
 * 整数倍的类型，比如byte, short(2), int(4), long(8), bytes(n)，Type则支持所有类型的操作，最终则转化为BlockBuilder的对基本byte
 * 类型的操作，比如Type.writeDouble()，通过Double.doubleToLongBits()把double转化成用long表示，在BlockBuilder里调用writeLong()，
 * 不属于byte基本操作的类型包括：boolean, double, object
 */
public interface BlockBuilder
        extends Block
{
    /**
     * Write a byte to the current entry;
     */
    default BlockBuilder writeByte(int value)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    /**
     * Write a short to the current entry;
     */
    default BlockBuilder writeShort(int value)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    /**
     * Write a int to the current entry;
     */
    default BlockBuilder writeInt(int value)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    /**
     * Write a long to the current entry;
     */
    default BlockBuilder writeLong(long value)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    /**
     * Write a byte sequences to the current entry;
     */
    default BlockBuilder writeBytes(Slice source, int sourceIndex, int length)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    /**
     * Return a writer to the current entry. The caller can operate on the returned caller to incrementally build the object. This is generally more efficient than
     * building the object elsewhere and call writeObject afterwards because a large chunk of memory could potentially be unnecessarily copied in this process.
     */
    default BlockBuilder beginBlockEntry()
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    /**
     * Create a new block from the current materialized block by keeping the same elements
     * only with respect to {@code visiblePositions}.
     */
    default Block getPositions(int[] visiblePositions, int offset, int length)
    {
        return build().getPositions(visiblePositions, offset, length);
    }

    /**
     * Write a byte to the current entry;
     */
    BlockBuilder closeEntry();

    /**
     * Appends a null value to the block.
     */
    BlockBuilder appendNull();

    /**
     * Append a struct to the block and close the entry.
     */
    default BlockBuilder appendStructure(Block value)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    /**
     * Do not use this interface outside block package.
     * Instead, use Block.writePositionTo(BlockBuilder, position)
     */
    default BlockBuilder appendStructureInternal(Block block, int position)
    {
        throw new UnsupportedOperationException(getClass().getName());
    }

    /**
     * Builds the block. This method can be called multiple times.
     */
    Block build();

    /**
     * Creates a new block builder of the same type based on the current usage statistics of this block builder.
     */
    BlockBuilder newBlockBuilderLike(BlockBuilderStatus blockBuilderStatus);
}
