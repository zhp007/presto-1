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
package io.prestosql.plugin.hive;

import io.prestosql.spi.connector.ConnectorPageSource;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.predicate.TupleDomain;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.joda.time.DateTimeZone;

import java.util.List;
import java.util.Optional;
import java.util.Properties;

/*
* TableScanOperator.getOutput()
*   PageSourceManager.createPageSource() （继承自PageSourceProvider）
*     HivePageSourceProvider.createPageSource() （继承自ConnectorPageSourceProvider）-> createHivePageSource()
* 1. 先在里面遍历pageSourceFactories，包括orc, parquet, rcfile，对每个factory，都尝试能创建出对应的HivePageSourceFactory，
* 如果能创建，则说明是相应的格式，则返回对应类型的ConnectorPageSource，通过序列化的lib的类名来判断是不是这种格式
* 2. 如果不满足上面第一类，则遍历cursorProviders，创建RecordCursor，其中的RecordReader来自hadoop lib，包括hadoop mapred
* 支持的各种文件格式
*
* 核心入口方法：HivePageSourceProvider.createPageSource()
*
* 有两种不同类型的数据源：ConnectorPageSource
*   HivePageSourceFactory - orc, parquet, rcfile
*   HiveRecordCursorProvider - generic（各种其他格式）, s3 select
*
* */
public interface HivePageSourceFactory
{
    Optional<? extends ConnectorPageSource> createPageSource(
            Configuration configuration,
            ConnectorSession session,
            Path path,
            long start,
            long length,
            long fileSize,
            Properties schema,
            List<HiveColumnHandle> columns,
            TupleDomain<HiveColumnHandle> effectivePredicate,
            DateTimeZone hiveStorageTimeZone);
}
