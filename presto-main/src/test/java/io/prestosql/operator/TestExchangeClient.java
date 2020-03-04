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

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.http.client.testing.TestingHttpClient;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import io.airlift.units.Duration;
import io.prestosql.block.BlockAssertions;
import io.prestosql.execution.buffer.PagesSerde;
import io.prestosql.execution.buffer.SerializedPage;
import io.prestosql.memory.context.SimpleLocalMemoryContext;
import io.prestosql.spi.Page;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.google.common.collect.Maps.uniqueIndex;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static io.airlift.concurrent.MoreFutures.tryGetFutureValue;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.testing.Assertions.assertLessThan;
import static io.prestosql.execution.buffer.TestingPagesSerdeFactory.testingPagesSerde;
import static io.prestosql.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestExchangeClient
{
    private ScheduledExecutorService scheduler;
    private ExecutorService pageBufferClientCallbackExecutor;

    private static final PagesSerde PAGES_SERDE = testingPagesSerde();

    @BeforeClass
    public void setUp()
    {
        scheduler = newScheduledThreadPool(4, daemonThreadsNamed("test-%s"));
        pageBufferClientCallbackExecutor = Executors.newSingleThreadExecutor();
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        if (scheduler != null) {
            scheduler.shutdownNow();
            scheduler = null;
        }
        if (pageBufferClientCallbackExecutor != null) {
            pageBufferClientCallbackExecutor.shutdownNow();
            pageBufferClientCallbackExecutor = null;
        }
    }

    @Test
    public void testHappyPath()
    {
        DataSize maxResponseSize = new DataSize(10, Unit.MEGABYTE);
        MockExchangeRequestProcessor processor = new MockExchangeRequestProcessor(maxResponseSize);

        URI location = URI.create("http://localhost:8080");
        processor.addPage(location, createPage(1));
        processor.addPage(location, createPage(2));
        processor.addPage(location, createPage(3));
        processor.setComplete(location);

        // TestingHttpClient.execute(request) -> processor.handle(request)
        // 提交给TestingHttpClient的request会交到processor来处理
        @SuppressWarnings("resource")
        ExchangeClient exchangeClient = new ExchangeClient(
                new DataSize(32, Unit.MEGABYTE),
                maxResponseSize,
                1,
                new Duration(1, TimeUnit.MINUTES),
                true,
                new TestingHttpClient(processor, scheduler),
                scheduler,
                new SimpleLocalMemoryContext(newSimpleAggregatedMemoryContext(), "test"),
                pageBufferClientCallbackExecutor);

        // 对这个location创建一个HttpPageBufferClient
        exchangeClient.addLocation(location);
        exchangeClient.noMoreLocations();

        assertEquals(exchangeClient.isClosed(), false);
        assertPageEquals(getNextPage(exchangeClient), createPage(1));
        assertEquals(exchangeClient.isClosed(), false);
        assertPageEquals(getNextPage(exchangeClient), createPage(2));
        assertEquals(exchangeClient.isClosed(), false);
        assertPageEquals(getNextPage(exchangeClient), createPage(3));
        assertNull(getNextPage(exchangeClient));
        /**
         * 在ExchangeClient.addLocation()中创建HttpPageBufferClient时，传入ExchangeClientCallback
         * 在HttpPageBufferClient完成后，调用close() -> sendDelete()删除location对应的task，如果成功则会调用
         * clientCallback.clientFinished()
         *
         * 回调的过程：
         * Class.this表示外部类的实例，从而使内部类能引用外部类
         * HttpPageBufferClient.sendDelete() (这个client包含ExchangeClient的callback)
         *   // 调用外部类的callback，并把自己传进去
         *   clientCallback.clientFinished(HttpPageBufferClient.this)
         *     // callback是外部类的一个私有内部类，接受上面的传的client (this)后，调用该内部类的外部类的实例的方法，
         *     // 把这个object传给外部类的方法
         *     ExchangeClient.this.clientFinished(client);
         * 这里callback相当于两个类传递方法和私有成员的工具：类B调用类A的方法，类A的方法又要用到类B的私有成员
         */
        assertEquals(exchangeClient.isClosed(), true);

        ExchangeClientStatus status = exchangeClient.getStatus();
        assertEquals(status.getBufferedPages(), 0);
        assertEquals(status.getBufferedBytes(), 0);

        // client should have sent only 2 requests: one to get all pages and once to get the done signal
        assertStatus(status.getPageBufferClientStatuses().get(0), location, "closed", 3, 3, 3, "not scheduled");
    }

    @Test(timeOut = 10000)
    public void testAddLocation()
            throws Exception
    {
        DataSize maxResponseSize = new DataSize(10, Unit.MEGABYTE);
        MockExchangeRequestProcessor processor = new MockExchangeRequestProcessor(maxResponseSize);

        @SuppressWarnings("resource")
        ExchangeClient exchangeClient = new ExchangeClient(
                new DataSize(32, Unit.MEGABYTE),
                maxResponseSize,
                1,
                new Duration(1, TimeUnit.MINUTES),
                true,
                new TestingHttpClient(processor, newCachedThreadPool(daemonThreadsNamed("test-%s"))),
                scheduler,
                new SimpleLocalMemoryContext(newSimpleAggregatedMemoryContext(), "test"),
                pageBufferClientCallbackExecutor);

        URI location1 = URI.create("http://localhost:8081/foo");
        processor.addPage(location1, createPage(1));
        processor.addPage(location1, createPage(2));
        processor.addPage(location1, createPage(3));
        processor.setComplete(location1);
        exchangeClient.addLocation(location1);

        assertEquals(exchangeClient.isClosed(), false);
        assertPageEquals(getNextPage(exchangeClient), createPage(1));
        assertEquals(exchangeClient.isClosed(), false);
        assertPageEquals(getNextPage(exchangeClient), createPage(2));
        assertEquals(exchangeClient.isClosed(), false);
        assertPageEquals(getNextPage(exchangeClient), createPage(3));

        assertFalse(tryGetFutureValue(exchangeClient.isBlocked(), 10, MILLISECONDS).isPresent());
        assertEquals(exchangeClient.isClosed(), false);

        URI location2 = URI.create("http://localhost:8082/bar");
        processor.addPage(location2, createPage(4));
        processor.addPage(location2, createPage(5));
        processor.addPage(location2, createPage(6));
        processor.setComplete(location2);
        exchangeClient.addLocation(location2);

        assertEquals(exchangeClient.isClosed(), false);
        assertPageEquals(getNextPage(exchangeClient), createPage(4));
        assertEquals(exchangeClient.isClosed(), false);
        assertPageEquals(getNextPage(exchangeClient), createPage(5));
        assertEquals(exchangeClient.isClosed(), false);
        assertPageEquals(getNextPage(exchangeClient), createPage(6));

        assertFalse(tryGetFutureValue(exchangeClient.isBlocked(), 10, MILLISECONDS).isPresent());
        assertEquals(exchangeClient.isClosed(), false);

        /**
         * 调用exchangeClient.noMoreLocations() -> scheduleRequestIfNecessary() -> notifyBlockedCallers()
         * 通知exchangeClient没有更多的源头(location)去获取page，如果已经处理完当前的page，则可以close
         *
         * 当[1]处理的page为NO_MORE_PAGES, [2]调用close()，[3]处理完所有page后发送最后一次request，[4]加新的page，
         * [5]client获取page失败时，会notifyBlockedCallers() 通知所有等待的exchangeClient，从而exchangeClient.isBlocked() = false
         */
        exchangeClient.noMoreLocations();
        // The transition to closed may happen asynchronously, since it requires that all the HTTP clients
        // receive a final GONE response, so just spin until it's closed or the test times out.
        while (!exchangeClient.isClosed()) {
            Thread.sleep(1);
        }

        // uniqueIndex: 2个参数：value, func(value -> key)，把每个value用在函数上产生对应的key，得到map<key, value>
        ImmutableMap<URI, PageBufferClientStatus> statuses = uniqueIndex(exchangeClient.getStatus().getPageBufferClientStatuses(), PageBufferClientStatus::getUri);
        assertStatus(statuses.get(location1), location1, "closed", 3, 3, 3, "not scheduled");
        assertStatus(statuses.get(location2), location2, "closed", 3, 3, 3, "not scheduled");
    }

    @Test
    public void testBufferLimit()
    {
        /**
         * 指定maxResponseSize时，会用它创建HttpPageBufferClient
         * // 把这个参数作为http header，加到发送给TaskResource的request中
         * HttpPageBufferClient.sendGetResults()
         *   TaskResource.getResults()
         *     TaskManager.getTaskResults()
         *       SqlTask.getTaskResults()
         *         OutputBuffer.get(maxSize)
         *           // 不管是哪种OutputBuffer，最后的调用实际上都由ClientBuffer执行
         *           // 即使单个page的大小>maxSize，也至少会返回1个page作为结果，不会用maxSize对page进行分割
         *           // 总是返回整数个page
         *           ClientBuffer.getPages(maxSize) -> processRead(maxSize)
         */
        DataSize maxResponseSize = new DataSize(1, Unit.BYTE);
        MockExchangeRequestProcessor processor = new MockExchangeRequestProcessor(maxResponseSize);

        URI location = URI.create("http://localhost:8080");

        // add a pages
        processor.addPage(location, createPage(1));
        processor.addPage(location, createPage(2));
        processor.addPage(location, createPage(3));
        processor.setComplete(location);

        @SuppressWarnings("resource")
        ExchangeClient exchangeClient = new ExchangeClient(
                new DataSize(1, Unit.BYTE),
                maxResponseSize,
                1,
                new Duration(1, TimeUnit.MINUTES),
                true,
                new TestingHttpClient(processor, newCachedThreadPool(daemonThreadsNamed("test-%s"))),
                scheduler,
                new SimpleLocalMemoryContext(newSimpleAggregatedMemoryContext(), "test"),
                pageBufferClientCallbackExecutor);

        exchangeClient.addLocation(location);
        exchangeClient.noMoreLocations();
        assertEquals(exchangeClient.isClosed(), false);

        long start = System.nanoTime();

        // start fetching pages
        exchangeClient.scheduleRequestIfNecessary();
        // wait for a page to be fetched
        do {
            // there is no thread coordination here, so sleep is the best we can do
            assertLessThan(Duration.nanosSince(start), new Duration(5, TimeUnit.SECONDS));
            sleepUninterruptibly(100, MILLISECONDS);
        }
        while (exchangeClient.getStatus().getBufferedPages() == 0);

        // client should have sent a single request for a single page
        // 由于maxResponseSize的大小限制，exchangeClient的pageBuffer里面只能1次存1个page
        assertEquals(exchangeClient.getStatus().getBufferedPages(), 1);
        assertTrue(exchangeClient.getStatus().getBufferedBytes() > 0);
        assertStatus(exchangeClient.getStatus().getPageBufferClientStatuses().get(0), location, "queued", 1, 1, 1, "not scheduled");

        // remove the page and wait for the client to fetch another page
        assertPageEquals(exchangeClient.pollPage(), createPage(1));
        do {
            assertLessThan(Duration.nanosSince(start), new Duration(5, TimeUnit.SECONDS));
            sleepUninterruptibly(100, MILLISECONDS);
        }
        while (exchangeClient.getStatus().getBufferedPages() == 0);

        // client should have sent a single request for a single page
        assertStatus(exchangeClient.getStatus().getPageBufferClientStatuses().get(0), location, "queued", 2, 2, 2, "not scheduled");
        assertEquals(exchangeClient.getStatus().getBufferedPages(), 1);
        assertTrue(exchangeClient.getStatus().getBufferedBytes() > 0);

        // remove the page and wait for the client to fetch another page
        assertPageEquals(exchangeClient.pollPage(), createPage(2));
        do {
            assertLessThan(Duration.nanosSince(start), new Duration(5, TimeUnit.SECONDS));
            sleepUninterruptibly(100, MILLISECONDS);
        }
        while (exchangeClient.getStatus().getBufferedPages() == 0);

        // client should have sent a single request for a single page
        assertStatus(exchangeClient.getStatus().getPageBufferClientStatuses().get(0), location, "queued", 3, 3, 3, "not scheduled");
        assertEquals(exchangeClient.getStatus().getBufferedPages(), 1);
        assertTrue(exchangeClient.getStatus().getBufferedBytes() > 0);

        // remove last page
        assertPageEquals(getNextPage(exchangeClient), createPage(3));

        //  wait for client to decide there are no more pages
        assertNull(getNextPage(exchangeClient));
        assertEquals(exchangeClient.getStatus().getBufferedPages(), 0);
        assertTrue(exchangeClient.getStatus().getBufferedBytes() == 0);
        assertEquals(exchangeClient.isClosed(), true);
        assertStatus(exchangeClient.getStatus().getPageBufferClientStatuses().get(0), location, "closed", 3, 5, 5, "not scheduled");
    }

    @Test
    public void testClose()
            throws Exception
    {
        DataSize maxResponseSize = new DataSize(1, Unit.BYTE);
        MockExchangeRequestProcessor processor = new MockExchangeRequestProcessor(maxResponseSize);

        URI location = URI.create("http://localhost:8080");
        processor.addPage(location, createPage(1));
        processor.addPage(location, createPage(2));
        processor.addPage(location, createPage(3));

        @SuppressWarnings("resource")
        ExchangeClient exchangeClient = new ExchangeClient(
                new DataSize(1, Unit.BYTE),
                maxResponseSize,
                1,
                new Duration(1, TimeUnit.MINUTES),
                true,
                new TestingHttpClient(processor, newCachedThreadPool(daemonThreadsNamed("test-%s"))),
                scheduler,
                new SimpleLocalMemoryContext(newSimpleAggregatedMemoryContext(), "test"),
                pageBufferClientCallbackExecutor);
        exchangeClient.addLocation(location);
        exchangeClient.noMoreLocations();

        // fetch a page
        assertEquals(exchangeClient.isClosed(), false);
        assertPageEquals(getNextPage(exchangeClient), createPage(1));

        // close client while pages are still available
        exchangeClient.close();
        while (!exchangeClient.isFinished()) {
            MILLISECONDS.sleep(10);
        }
        assertEquals(exchangeClient.isClosed(), true);
        assertNull(exchangeClient.pollPage());
        assertEquals(exchangeClient.getStatus().getBufferedPages(), 0);
        assertEquals(exchangeClient.getStatus().getBufferedBytes(), 0);

        // client should have sent only 2 requests: one to get all pages and once to get the done signal
        PageBufferClientStatus clientStatus = exchangeClient.getStatus().getPageBufferClientStatuses().get(0);
        assertEquals(clientStatus.getUri(), location);
        assertEquals(clientStatus.getState(), "closed", "status");
        assertEquals(clientStatus.getHttpRequestState(), "not scheduled", "httpRequestState");
    }

    private static Page createPage(int size)
    {
        return new Page(BlockAssertions.createLongSequenceBlock(0, size));
    }

    private static SerializedPage getNextPage(ExchangeClient exchangeClient)
    {
        ListenableFuture<SerializedPage> futurePage = Futures.transform(exchangeClient.isBlocked(), ignored -> exchangeClient.pollPage(), directExecutor());
        return tryGetFutureValue(futurePage, 100, TimeUnit.SECONDS).orElse(null);
    }

    private static void assertPageEquals(SerializedPage actualPage, Page expectedPage)
    {
        assertNotNull(actualPage);
        assertEquals(actualPage.getPositionCount(), expectedPage.getPositionCount());
        assertEquals(PAGES_SERDE.deserialize(actualPage).getChannelCount(), expectedPage.getChannelCount());
    }

    private static void assertStatus(PageBufferClientStatus clientStatus,
            URI location,
            String status,
            int pagesReceived,
            int requestsScheduled,
            int requestsCompleted,
            String httpRequestState)
    {
        assertEquals(clientStatus.getUri(), location);
        assertEquals(clientStatus.getState(), status, "status");
        assertEquals(clientStatus.getPagesReceived(), pagesReceived, "pagesReceived");
        assertEquals(clientStatus.getRequestsScheduled(), requestsScheduled, "requestsScheduled");
        assertEquals(clientStatus.getRequestsCompleted(), requestsCompleted, "requestsCompleted");
        assertEquals(clientStatus.getHttpRequestState(), httpRequestState, "httpRequestState");
    }
}
