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

import com.google.common.collect.AbstractIterator;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockEncodingSerde;

import java.util.Iterator;

import static io.prestosql.block.BlockSerdeUtil.readBlock;
import static io.prestosql.block.BlockSerdeUtil.writeBlock;
import static java.lang.Math.toIntExact;
import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;

public class PagesSerdeUtil
{
    private PagesSerdeUtil()
    {
    }

    /**
     * Page在SliceOutput里面的结构为：
     * [num_of_blocks, block_1[position_count, [offset_for_each_position], total_length, total_data_slice]]
     *
     * Page里面的每个Block怎么写到SliceOutput?
     * blockEncodingSerde.writeBlock(output, block) <- InternalBlockEncodingSerde
     *   // block包含encoding_name，由metadata.getBlockEncoding(encoding_name)得到BlockEncoding
     *   BlockEncoding.writeBlock(this, sliceOutput, block)
     *
     * SliceOutput包含实际保存数据的Slice以及它的大小，这里的Slice只包含原始数据raw_page
     *
     * 下面的writeSerializedPage()方法也有SliceOutput，它Slice的则包含各种序列化以后的性质，结构为：
     * [position_count, markers, uncompressed_bytes, actual_bytes, data_slice]
     *
     * Page、SerializedPage分别对应什么读写方法？
     * 写：
     * Page -> writeRawPage()
     * SerializedPage -> writeSerializedPage()
     * 写的输出都是SliceOutput
     *
     * 读：类似写
     * Page -> readRawPage()
     * SerializedPage -> readSerializedPage()
     * 读的输入都是SliceInput
     * 总结：根据不同类型的page构造对应结构的SliceOutput，以及用对应的方法解读SliceInput
     */
    static void writeRawPage(Page page, SliceOutput output, BlockEncodingSerde serde)
    {
        output.writeInt(page.getChannelCount());
        for (int channel = 0; channel < page.getChannelCount(); channel++) {
            writeBlock(serde, output, page.getBlock(channel));
        }
    }

    static Page readRawPage(int positionCount, SliceInput input, BlockEncodingSerde blockEncodingSerde)
    {
        int numberOfBlocks = input.readInt();
        Block[] blocks = new Block[numberOfBlocks];
        for (int i = 0; i < blocks.length; i++) {
            blocks[i] = readBlock(blockEncodingSerde, input);
        }

        return new Page(positionCount, blocks);
    }

    public static void writeSerializedPage(SliceOutput output, SerializedPage page)
    {
        output.writeInt(page.getPositionCount());
        output.writeByte(page.getPageCodecMarkers());
        output.writeInt(page.getUncompressedSizeInBytes());
        output.writeInt(page.getSizeInBytes());
        output.writeBytes(page.getSlice());
    }

    /**
     * SliceInput的结构：
     * [position_count, markers, uncompressed_size, actual_size, data_slice]
     * markers用1个byte里面的每个bit位都分别表示压缩加密等性质
     * 实际数据都存在data_slice里，大小为actual_size
     */
    private static SerializedPage readSerializedPage(SliceInput sliceInput)
    {
        int positionCount = sliceInput.readInt();
        PageCodecMarker.MarkerSet markers = PageCodecMarker.MarkerSet.fromByteValue(sliceInput.readByte());
        int uncompressedSizeInBytes = sliceInput.readInt();
        int sizeInBytes = sliceInput.readInt();
        Slice slice = sliceInput.readSlice(toIntExact((sizeInBytes)));
        return new SerializedPage(slice, markers, positionCount, uncompressedSizeInBytes);
    }

    public static long writeSerializedPages(SliceOutput sliceOutput, Iterable<SerializedPage> pages)
    {
        Iterator<SerializedPage> pageIterator = pages.iterator();
        long size = 0;
        while (pageIterator.hasNext()) {
            SerializedPage page = pageIterator.next();
            writeSerializedPage(sliceOutput, page);
            size += page.getSizeInBytes();
        }
        return size;
    }

    public static long writePages(PagesSerde serde, SliceOutput sliceOutput, Page... pages)
    {
        return writePages(serde, sliceOutput, asList(pages).iterator());
    }

    public static long writePages(PagesSerde serde, SliceOutput sliceOutput, Iterator<Page> pages)
    {
        long size = 0;
        while (pages.hasNext()) {
            Page page = pages.next();
            writeSerializedPage(sliceOutput, serde.serialize(page));
            size += page.getSizeInBytes();
        }
        return size;
    }

    public static Iterator<Page> readPages(PagesSerde serde, SliceInput sliceInput)
    {
        return new PageReader(serde, sliceInput);
    }

    private static class PageReader
            extends AbstractIterator<Page>
    {
        private final PagesSerde serde;
        private final SliceInput input;

        PageReader(PagesSerde serde, SliceInput input)
        {
            this.serde = requireNonNull(serde, "serde is null");
            this.input = requireNonNull(input, "input is null");
        }

        @Override
        protected Page computeNext()
        {
            if (!input.isReadable()) {
                return endOfData();
            }

            // SliceInput里面的游标position会随着读取page移动，读取完当前page时，游标会移动到下一个page的起始位置
            return serde.deserialize(readSerializedPage(input));
        }
    }

    public static Iterator<SerializedPage> readSerializedPages(SliceInput sliceInput)
    {
        return new SerializedPageReader(sliceInput);
    }

    private static class SerializedPageReader
            extends AbstractIterator<SerializedPage>
    {
        private final SliceInput input;

        SerializedPageReader(SliceInput input)
        {
            this.input = requireNonNull(input, "input is null");
        }

        @Override
        protected SerializedPage computeNext()
        {
            if (!input.isReadable()) {
                return endOfData();
            }

            return readSerializedPage(input);
        }
    }
}
