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
package io.prestosql.util;

import com.google.common.collect.Sets;
import io.airlift.log.Logger;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.lang.ref.PhantomReference;
import java.lang.ref.ReferenceQueue;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadExecutor;

@ThreadSafe
public class FinalizerService
{
    private static final Logger log = Logger.get(FinalizerService.class);

    private final Set<FinalizerReference> finalizers = Sets.newConcurrentHashSet();
    private final ReferenceQueue<Object> finalizerQueue = new ReferenceQueue<>();
    @GuardedBy("this")
    private ExecutorService executor;

    @GuardedBy("this")
    private Future<?> finalizerTask;

    @PostConstruct
    public synchronized void start()
    {
        if (finalizerTask != null) {
            return;
        }
        if (executor == null) {
            executor = newSingleThreadExecutor(daemonThreadsNamed("FinalizerService"));
        }
        if (executor.isShutdown()) {
            throw new IllegalStateException("Finalizer service has been destroyed");
        }
        finalizerTask = executor.submit(this::processFinalizerQueue);
    }

    @PreDestroy
    public synchronized void destroy()
    {
        if (finalizerTask != null) {
            finalizerTask.cancel(true);
            finalizerTask = null;
        }
        if (executor != null) {
            executor.shutdownNow();
            executor = null;
        }
    }

    /**
     * When referent is freed by the garbage collector, run cleanup.
     * <p>
     * Note: cleanup must not contain a reference to the referent object.
     */
    public void addFinalizer(Object referent, Runnable cleanup)
    {
        requireNonNull(referent, "referent is null");
        requireNonNull(cleanup, "cleanup is null");
        finalizers.add(new FinalizerReference(referent, finalizerQueue, cleanup));
    }

    private void processFinalizerQueue()
    {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                FinalizerReference finalizer = (FinalizerReference) finalizerQueue.remove();
                finalizers.remove(finalizer);
                finalizer.cleanup();
            }
            catch (InterruptedException e) {
                return;
            }
            catch (Throwable e) {
                log.error(e, "Finalizer cleanup failed");
            }
        }
    }

    private static class FinalizerReference
            extends PhantomReference<Object>
    {
        private final Runnable cleanup;
        private final AtomicBoolean executed = new AtomicBoolean();

        /*
        * 通过构造函数把reference注册到ReferenceQueue，满足某个条件时GC会把这个reference加到ReferenceQueue中准备进行回收
        *
        * 这里是extend PhantomReference引用被注册的object，当GC认为这个object是phantom reachable时，就会把它加到reference queue
        * 中等待回收。
        *
        * phantom reachable的定义：object在finalize后并且只被phantom reference引用，就会被加入到队列，但并不是马上会被回收。
        * 在finalize后直到unreachable被GC回收前，phantom reference一直在队列中，只是不能再被增加新的引用 (get返回null)，直到已有
        * 的引用全都清空后则被回收。
        *
        * phantom reference提供了比finalize()更灵活的object cleanup的手段：把object让phantom reference 引用，在object达到
        * phantom reachable的状态后被加入队列。同时在创建phantom reference时，启动一个线程池不停的查询队列，能队列非空时，能从队列中
        * 获取的reference都是phantom reference，获取后则运行reference对应的cleanup()方法，进行GC回收前的收尾清理工作
        * */
        public FinalizerReference(Object referent, ReferenceQueue<Object> queue, Runnable cleanup)
        {
            super(requireNonNull(referent, "referent is null"), requireNonNull(queue, "queue is null"));
            this.cleanup = requireNonNull(cleanup, "cleanup is null");
        }

        public void cleanup()
        {
            if (executed.compareAndSet(false, true)) {
                cleanup.run();
            }
        }
    }
}
