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
package io.prestosql.operator;

import io.prestosql.spi.Page;

import java.util.List;

/**
 * This class is responsible for iterating over build rows, which have
 * same values in hash columns as given probe row (but according to
 * filterFunction can have non matching values on some other column).
 */

/**
 * 在PagesHash中，每一行会经过下面的步骤算出的hash值并存储行号到hash表中：
 * 1. 从realPosition解码得到实际的位置，用其计算出hash值
 * 2. 根据hash值，求出这一行在PagesHash的hash表key[]中的位置，pos = get_pos(hash值)，key[pos] = 行号
 *
 * 如果有多行计算出的hash值相同，并且这些行对应的列的值也相同，怎么处理？
 * 用PositionLinks保存满足上面2个相同条件的所有行号
 *
 * 过程如下：
 * hash(row1) = hash1
 * get_pos(hash1) = pos1
 * key[pos1] = row1
 *
 * 如果现在有行row2：
 * hash(row2) = hash2
 * get_pos(hash2) = pos2
 * 满足 pos1 == pos2，则表明它们指向hash表中的同一个位置，发生冲突
 * 这时用PositionLinks作为解决冲突的链表，在里面把row2作为下标的位置存放row1，同时在hash表中pos1的位置存储row2，
 * PositionLinks.link(row2) = row1
 * key[pos1] = row2
 *
 * hash表: pos1 -> row2，PositionLinks: row2 -> row1，表示下标为row2的位置存放行号row1
 * 之后用hash表寻址到pos1时，可以找到对应列完全相同的行row2, row1
 *
 * 类似的，如果有行row3满足相同的情况：
 * pos1 == pos2 == pos3
 * 经过处理后，hash表: pos1 -> row3，PositionLinks: row3 -> row2, row2 -> row1
 *
 * hash表的值和PositionLinks的下标和值用的都是realPosition，所以PositionLinks中的下标肯定不会冲突，
 * 从而保证这个数据结构能保存所有值相同的行号
 *
 * 通过PositionLinks，在HashMap中找到值对应的key后，可以快速找到所有其他有相同值的行，从realPosition中解码出实际的page index和
 * position，进行行连接
 */
public interface PositionLinks
{
    long getSizeInBytes();

    /**
     * Initialize iteration over position links. Returns first potentially eligible
     * join position starting from (and including) position argument.
     * <p>
     * When there are no more position -1 is returned
     */
    int start(int position, int probePosition, Page allProbeChannelsPage);

    /**
     * Iterate over position links. When there are no more position -1 is returned.
     */
    int next(int position, int probePosition, Page allProbeChannelsPage);

    interface FactoryBuilder
    {
        /**
         * @return value that should be used in future references to created position links
         */
        int link(int left, int right);

        Factory build();

        /**
         * @return number of linked elements
         */
        int size();

        default boolean isEmpty()
        {
            return size() == 0;
        }
    }

    interface Factory
    {
        /**
         * Separate JoinFilterFunctions have to be created and supplied for each thread using PositionLinks
         * since JoinFilterFunction is not thread safe...
         */
        PositionLinks create(List<JoinFilterFunction> searchFunctions);

        /**
         * @return a checksum for this {@link PositionLinks}, useful when entity is restored from spilled data
         */
        long checksum();
    }
}
