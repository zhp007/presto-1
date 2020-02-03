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

import io.airlift.units.DataSize;
import io.prestosql.spi.Page;
import io.prestosql.spi.PageBuilder;
import it.unimi.dsi.fastutil.HashCommon;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import org.openjdk.jol.info.ClassLayout;

import java.util.Arrays;

import static io.airlift.slice.SizeOf.sizeOf;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.prestosql.operator.SyntheticAddress.decodePosition;
import static io.prestosql.operator.SyntheticAddress.decodeSliceIndex;
import static io.prestosql.util.HashCollisionsEstimator.estimateNumberOfHashCollisions;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

// This implementation assumes arrays used in the hash are always a power of 2
public final class PagesHash
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(PagesHash.class).instanceSize();
    private static final DataSize CACHE_SIZE = new DataSize(128, KILOBYTE);
    // addresses就是PagesIndex里valueAddresses，每个值表示当前page集合里的某一行数据属于哪个page，以及在这个page中的行号
    private final LongArrayList addresses;
    private final PagesHashStrategy pagesHashStrategy;

    private final int channelCount;
    private final int mask;
    // key[]表示hash表，保存 取模后的hash值 -> realPosition(绝对行号) 的映射
    private final int[] key;
    private final long size;

    // Native array of hashes for faster collisions resolution compared
    // to accessing values in blocks. We use bytes to reduce memory foot print
    // and there is no performance gain from storing full hashes
    private final byte[] positionToHashes;
    private final long hashCollisions;
    private final double expectedHashCollisions;

    public PagesHash(
            LongArrayList addresses,
            PagesHashStrategy pagesHashStrategy,
            PositionLinks.FactoryBuilder positionLinks)
    {
        this.addresses = requireNonNull(addresses, "addresses is null");
        this.pagesHashStrategy = requireNonNull(pagesHashStrategy, "pagesHashStrategy is null");
        this.channelCount = pagesHashStrategy.getChannelCount();

        // reserve memory for the arrays
        int hashSize = HashCommon.arraySize(addresses.size(), 0.75f);

        mask = hashSize - 1;
        key = new int[hashSize];
        Arrays.fill(key, -1);

        positionToHashes = new byte[addresses.size()];

        // We will process addresses in batches, to save memory on array of hashes.
        int positionsInStep = Math.min(addresses.size() + 1, (int) CACHE_SIZE.toBytes() / Integer.SIZE);
        long[] positionToFullHashes = new long[positionsInStep];
        long hashCollisionsLocal = 0;

        for (int step = 0; step * positionsInStep <= addresses.size(); step++) {
            int stepBeginPosition = step * positionsInStep;
            int stepEndPosition = Math.min((step + 1) * positionsInStep, addresses.size());
            int stepSize = stepEndPosition - stepBeginPosition;

            // First extract all hashes from blocks to native array.
            // Somehow having this as a separate loop is much faster compared
            // to extracting hashes on the fly in the loop below.
            for (int position = 0; position < stepSize; position++) {
                int realPosition = position + stepBeginPosition;
                // 用这一行在page集合中绝对行号解码出实际位置（原本属于哪个page，以及在这个page中的行号），用其计算出hash值
                // hash值是基于这个位置存储的实际的数据值计算出来的
                long hash = readHashPosition(realPosition);
                // 创建这一行在当前这个address batch中的相对行号到hash值的映射
                positionToFullHashes[position] = hash;
                // 创建这一行在page集合中的绝对行号到hash值的映射
                // 这两个数组存储相同的hash值，只是1个下标用相对行号（position），1个下标用绝对行号（realPosition）
                positionToHashes[realPosition] = (byte) hash;
            }

            // index pages
            for (int position = 0; position < stepSize; position++) {
                int realPosition = position + stepBeginPosition;
                if (isPositionNull(realPosition)) {
                    continue;
                }

                long hash = positionToFullHashes[position];
                // 根据每行的hash值，求出这行在hash表中的位置，即hash值对hashSize取模后的值
                int pos = getHashPosition(hash, mask);

                // look for an empty slot or a slot containing this key
                // 如果hash表中对应的这个位置非空
                while (key[pos] != -1) {
                    int currentKey = key[pos];
                    // 检查发生冲突的位置保存的地址对应的值是否和当前这一行的值相同，相同要满足2个条件：
                    // 1. hash值相同
                    // 2. 实际存储的值相同，用Type.equalTo()检查
                    // 如果相同，则使用PositionLinks来处理，类似于HashMap中用链表来存储key相同的值
                    if (((byte) hash) == positionToHashes[currentKey] && positionEqualsPositionIgnoreNulls(currentKey, realPosition)) {
                        // found a slot for this key
                        // link the new key position to the current key position
                        realPosition = positionLinks.link(realPosition, currentKey);

                        // key[pos] updated outside of this loop
                        break;
                    }
                    // increment position and mask to handler wrap around
                    pos = (pos + 1) & mask;
                    hashCollisionsLocal++;
                }

                key[pos] = realPosition;
            }
        }

        size = sizeOf(addresses.elements()) + pagesHashStrategy.getSizeInBytes() +
                sizeOf(key) + sizeOf(positionToHashes);
        hashCollisions = hashCollisionsLocal;
        expectedHashCollisions = estimateNumberOfHashCollisions(addresses.size(), hashSize);
    }

    public final int getChannelCount()
    {
        return channelCount;
    }

    public int getPositionCount()
    {
        return addresses.size();
    }

    public long getInMemorySizeInBytes()
    {
        return INSTANCE_SIZE + size;
    }

    public long getHashCollisions()
    {
        return hashCollisions;
    }

    public double getExpectedHashCollisions()
    {
        return expectedHashCollisions;
    }

    public int getAddressIndex(int position, Page hashChannelsPage)
    {
        return getAddressIndex(position, hashChannelsPage, pagesHashStrategy.hashRow(position, hashChannelsPage));
    }

    public int getAddressIndex(int rightPosition, Page hashChannelsPage, long rawHash)
    {
        int pos = getHashPosition(rawHash, mask);

        while (key[pos] != -1) {
            if (positionEqualsCurrentRowIgnoreNulls(key[pos], (byte) rawHash, rightPosition, hashChannelsPage)) {
                return key[pos];
            }
            // increment position and mask to handler wrap around
            pos = (pos + 1) & mask;
        }
        return -1;
    }

    public void appendTo(long position, PageBuilder pageBuilder, int outputChannelOffset)
    {
        long pageAddress = addresses.getLong(toIntExact(position));
        int blockIndex = decodeSliceIndex(pageAddress);
        int blockPosition = decodePosition(pageAddress);

        pagesHashStrategy.appendTo(blockIndex, blockPosition, pageBuilder, outputChannelOffset);
    }

    private boolean isPositionNull(int position)
    {
        long pageAddress = addresses.getLong(position);
        int blockIndex = decodeSliceIndex(pageAddress);
        int blockPosition = decodePosition(pageAddress);

        return pagesHashStrategy.isPositionNull(blockIndex, blockPosition);
    }

    private long readHashPosition(int position)
    {
        long pageAddress = addresses.getLong(position);
        int blockIndex = decodeSliceIndex(pageAddress);
        int blockPosition = decodePosition(pageAddress);

        return pagesHashStrategy.hashPosition(blockIndex, blockPosition);
    }

    private boolean positionEqualsCurrentRowIgnoreNulls(int leftPosition, byte rawHash, int rightPosition, Page rightPage)
    {
        if (positionToHashes[leftPosition] != rawHash) {
            return false;
        }

        long pageAddress = addresses.getLong(leftPosition);
        int blockIndex = decodeSliceIndex(pageAddress);
        int blockPosition = decodePosition(pageAddress);

        return pagesHashStrategy.positionEqualsRowIgnoreNulls(blockIndex, blockPosition, rightPosition, rightPage);
    }

    private boolean positionEqualsPositionIgnoreNulls(int leftPosition, int rightPosition)
    {
        long leftPageAddress = addresses.getLong(leftPosition);
        int leftBlockIndex = decodeSliceIndex(leftPageAddress);
        int leftBlockPosition = decodePosition(leftPageAddress);

        long rightPageAddress = addresses.getLong(rightPosition);
        int rightBlockIndex = decodeSliceIndex(rightPageAddress);
        int rightBlockPosition = decodePosition(rightPageAddress);

        return pagesHashStrategy.positionEqualsPositionIgnoreNulls(leftBlockIndex, leftBlockPosition, rightBlockIndex, rightBlockPosition);
    }

    private static int getHashPosition(long rawHash, long mask)
    {
        // Avalanches the bits of a long integer by applying the finalisation step of MurmurHash3.
        //
        // This function implements the finalisation step of Austin Appleby's <a href="http://sites.google.com/site/murmurhash/">MurmurHash3</a>.
        // Its purpose is to avalanche the bits of the argument to within 0.25% bias. It is used, among other things, to scramble quickly (but deeply) the hash
        // values returned by {@link Object#hashCode()}.
        //

        rawHash ^= rawHash >>> 33;
        rawHash *= 0xff51afd7ed558ccdL;
        rawHash ^= rawHash >>> 33;
        rawHash *= 0xc4ceb9fe1a85ec53L;
        rawHash ^= rawHash >>> 33;

        return (int) (rawHash & mask);
    }
}
