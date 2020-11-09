package com.amazon.opendistroforelasticsearch.search.async.context.permits;

import com.amazon.opendistroforelasticsearch.search.async.context.AsyncSearchActiveContextId;
import com.amazon.opendistroforelasticsearch.search.async.plugin.AsyncSearchPlugin;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.CheckedRunnable;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ScalingExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPoolStats;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.instanceOf;

public class AsyncSearchContextPermitsTests extends ESTestCase {

    private static ThreadPool threadPool;

    private AsyncSearchContextPermits permits;

    @BeforeClass
    public static void setupThreadPool() {
        int writeThreadPoolSize = randomIntBetween(1, 2);
        int writeThreadPoolQueueSize = randomIntBetween(1, 2);
        Settings settings = Settings.builder()
                .put("thread_pool." + AsyncSearchPlugin.OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME
                        + ".size", writeThreadPoolSize)
                .put("thread_pool." + AsyncSearchPlugin.OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME
                        + ".queue_size", writeThreadPoolQueueSize)
                .build();
        final int availableProcessors = EsExecutors.allocatedProcessors(settings);
        ScalingExecutorBuilder scalingExecutorBuilder =
                new ScalingExecutorBuilder(AsyncSearchPlugin.OPEN_DISTRO_ASYNC_SEARCH_GENERIC_THREAD_POOL_NAME, 1,
                        Math.min(2 * availableProcessors, Math.max(128, 512)), TimeValue.timeValueMinutes(30));
        threadPool = new TestThreadPool("IndexShardOperationPermitsTests", settings, scalingExecutorBuilder);
    }

    @AfterClass
    public static void shutdownThreadPool() {
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        threadPool = null;
    }

    @Before
    public void createAsyncSearchContextPermit() {
        permits = new AsyncSearchContextPermits(new AsyncSearchActiveContextId(UUID.randomUUID().toString(),
                randomNonNegativeLong()), threadPool);
    }

    @After
    public void checkNoInflightOperations() {
        assertThat(permits.semaphore.availablePermits(), equalTo(Integer.MAX_VALUE));
    }

    public void testAllOperationsInvoked() throws InterruptedException, TimeoutException {
        int numThreads = 10;

        class DummyException extends RuntimeException {
        }

        List<PlainActionFuture<Releasable>> futures = new ArrayList<>();
        List<Thread> operationThreads = new ArrayList<>();
        CountDownLatch latch = new CountDownLatch(numThreads / 4);
        for (int i = 0; i < numThreads; i++) {
            boolean failingListener = randomBoolean();
            PlainActionFuture<Releasable> future = new PlainActionFuture<Releasable>() {
                @Override
                public void onResponse(Releasable releasable) {
                    releasable.close();
                    if (failingListener) {
                        throw new DummyException();
                    } else {
                        super.onResponse(releasable);
                    }
                }
            };
            Thread thread = new Thread(() -> {
                latch.countDown();
                try {
                    permits.asyncAcquirePermit(future, TimeValue.timeValueSeconds(1), "testAllOperationsInvoked");
                } catch (DummyException dummyException) {
                    // ok, notify future
                    assertTrue(failingListener);
                    future.onFailure(dummyException);
                }
            });
            futures.add(future);
            operationThreads.add(thread);
        }

        CountDownLatch blockFinished = new CountDownLatch(1);
        threadPool.generic().execute(() -> {
            try {
                latch.await();
                blockFinished.countDown();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

        for (Thread thread : operationThreads) {
            thread.start();
        }

        for (PlainActionFuture<Releasable> future : futures) {
            try {
                assertNotNull(future.get(1, TimeUnit.MINUTES));
            } catch (ExecutionException e) {

                assertThat(e.getCause(), either(instanceOf(DummyException.class))
                        .or(instanceOf(EsRejectedExecutionException.class)));
            }
        }

        for (Thread thread : operationThreads) {
            thread.join();
        }

        blockFinished.await();
    }


    public void testAsyncBlockOperationsOperationBeforeBlocked() throws InterruptedException, BrokenBarrierException {
        final CyclicBarrier barrier = new CyclicBarrier(2);
        final CountDownLatch operationExecutingLatch = new CountDownLatch(1);
        final CountDownLatch firstOperationLatch = new CountDownLatch(1);
        final CountDownLatch firstOperationCompleteLatch = new CountDownLatch(1);
        final Thread firstOperationThread =
                new Thread(controlledAcquire(barrier, operationExecutingLatch, firstOperationLatch, firstOperationCompleteLatch));
        firstOperationThread.start();

        barrier.await();

        operationExecutingLatch.await();

        final CountDownLatch blockedLatch = new CountDownLatch(1);
        final AtomicBoolean onBlocked = new AtomicBoolean();
        permits.asyncAcquireAllPermits(wrap(() -> {
            onBlocked.set(true);
            blockedLatch.countDown();
        }), TimeValue.timeValueMinutes(1), "");
        assertFalse(onBlocked.get());

        final CountDownLatch secondOperationExecuting = new CountDownLatch(1);
        final CountDownLatch secondOperationComplete = new CountDownLatch(1);
        final AtomicBoolean secondOperation = new AtomicBoolean();
        final Thread secondOperationThread = new Thread(() -> {
            secondOperationExecuting.countDown();
            permits.asyncAcquirePermit(
                    new ActionListener<Releasable>() {
                        @Override
                        public void onResponse(Releasable releasable) {
                            secondOperation.set(true);
                            releasable.close();
                            secondOperationComplete.countDown();
                        }

                        @Override
                        public void onFailure(Exception e) {
                            throw new RuntimeException(e);
                        }
                    },

                    TimeValue.timeValueMinutes(1), "");
        });
        secondOperationThread.start();

        secondOperationExecuting.await();
        assertFalse(secondOperation.get());

        firstOperationLatch.countDown();
        firstOperationCompleteLatch.await();
        blockedLatch.await();
        assertTrue(onBlocked.get());

        secondOperationComplete.await();
        assertTrue(secondOperation.get());

        firstOperationThread.join();
        secondOperationThread.join();
    }

    public void testAsyncBlockOperationsRace() throws Exception {
        // we racily submit operations and a delay, and then ensure that all operations were actually completed
        final int operations = scaledRandomIntBetween(1, 64);
        final CyclicBarrier barrier = new CyclicBarrier(1 + 1 + operations);
        final CountDownLatch operationLatch = new CountDownLatch(1 + operations);
        final Set<Integer> values = Collections.newSetFromMap(new ConcurrentHashMap<>());
        final List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < operations; i++) {
            final int value = i;
            final Thread thread = new Thread(() -> {
                try {
                    barrier.await();
                } catch (final BrokenBarrierException | InterruptedException e) {
                    throw new RuntimeException(e);
                }
                permits.asyncAcquirePermit(
                        new ActionListener<Releasable>() {
                            @Override
                            public void onResponse(Releasable releasable) {
                                values.add(value);
                                releasable.close();
                                operationLatch.countDown();
                            }

                            @Override
                            public void onFailure(Exception e) {

                            }
                        },
                        TimeValue.timeValueMinutes(1), "");
            });
            thread.start();
            threads.add(thread);
        }

        final Thread blockingThread = new Thread(() -> {
            try {
                barrier.await();
            } catch (final BrokenBarrierException | InterruptedException e) {
                throw new RuntimeException(e);
            }
            permits.asyncAcquireAllPermits(wrap(() -> {
                values.add(operations);
                operationLatch.countDown();
            }), TimeValue.timeValueMinutes(30), "");
        });
        blockingThread.start();

        barrier.await();

        operationLatch.await();
        for (final Thread thread : threads) {
            thread.join();
        }
        blockingThread.join();

        // check that all operations completed
        for (int i = 0; i < operations; i++) {
            assertTrue(values.contains(i));
        }
        assertTrue(values.contains(operations));
        /*
         * The block operation is executed on another thread and the operations can have completed before this thread has returned all the
         * permits to the semaphore. We wait here until all generic threads are idle as an indication that all permits have been returned to
         * the semaphore.
         */
        assertBusy(() -> {
            for (final ThreadPoolStats.Stats stats : threadPool.stats()) {
                if (ThreadPool.Names.GENERIC.equals(stats.getName())) {
                    assertThat("Expected no active threads in GENERIC pool", stats.getActive(), equalTo(0));
                    return;
                }
            }
            fail("Failed to find stats for the GENERIC thread pool");
        });
    }


    public void testTimeout() throws BrokenBarrierException, InterruptedException {
        final CyclicBarrier barrier = new CyclicBarrier(2);
        final CountDownLatch operationExecutingLatch = new CountDownLatch(1);
        final CountDownLatch operationLatch = new CountDownLatch(1);
        final CountDownLatch operationCompleteLatch = new CountDownLatch(1);

        final Thread thread = new Thread(controlledAcquire(barrier, operationExecutingLatch,
                operationLatch, operationCompleteLatch));
        thread.start();

        barrier.await();

        operationExecutingLatch.await();

        final CountDownLatch onFailureLatch = new CountDownLatch(2);
        permits.asyncAcquireAllPermits(ActionListener.wrap(releasable -> {
                    try {
                        releasable.close();
                        fail("Permit acquisition attempt should have timed out");
                    } finally {
                        onFailureLatch.countDown();
                    }
                }, e -> {
                    try {
                        assertThat(e, hasToString(containsString("timed out")));
                    } finally {
                        onFailureLatch.countDown();
                    }
                }),
                TimeValue.timeValueMillis(1), "");

        {
            final AtomicReference<Exception> reference = new AtomicReference<>();
            permits.asyncAcquireAllPermits(new ActionListener<Releasable>() {
                @Override
                public void onResponse(Releasable releasable) {
                    try {
                        releasable.close();
                        fail("Permit acquisition attempt should have timed out.");
                    } finally {
                        onFailureLatch.countDown();
                    }
                }

                @Override
                public void onFailure(final Exception e) {
                    assertThat(e, hasToString(containsString("timed out")));
                    onFailureLatch.countDown();
                }
            }, TimeValue.timeValueMillis(1), "");

            onFailureLatch.await();
        }

        operationLatch.countDown();

        operationCompleteLatch.await();

        thread.join();
    }


    /**
     * Returns an operation that acquires a permit and synchronizes in the following manner:
     * <ul>
     * <li>waits on the {@code barrier} before acquiring a permit</li>
     * <li>counts down the {@code operationExecutingLatch} when it acquires the permit</li>
     * <li>waits on the {@code operationLatch} before releasing the permit</li>
     * <li>counts down the {@code operationCompleteLatch} after releasing the permit</li>
     * </ul>
     *
     * @param barrier                 the barrier to wait on
     * @param operationExecutingLatch the latch to countdown after acquiring the permit
     * @param operationLatch          the latch to wait on before releasing the permit
     * @param operationCompleteLatch  the latch to countdown after releasing the permit
     * @return a controllable runnable that acquires a permit
     */
    private Runnable controlledAcquire(
            final CyclicBarrier barrier,
            final CountDownLatch operationExecutingLatch,
            final CountDownLatch operationLatch,
            final CountDownLatch operationCompleteLatch) {
        return () -> {
            try {
                barrier.await();
            } catch (final BrokenBarrierException | InterruptedException e) {
                throw new RuntimeException(e);
            }
            permits.asyncAcquirePermit(
                    new ActionListener<Releasable>() {
                        @Override
                        public void onResponse(Releasable releasable) {
                            operationExecutingLatch.countDown();
                            try {
                                operationLatch.await();
                            } catch (final InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                            releasable.close();
                            operationCompleteLatch.countDown();
                        }

                        @Override
                        public void onFailure(Exception e) {
                            throw new RuntimeException(e);
                        }
                    },
                    TimeValue.timeValueMinutes(1), "");
        };
    }

    private static ActionListener<Releasable> wrap(final CheckedRunnable<Exception> onResponse) {
        return new ActionListener<Releasable>() {
            @Override
            public void onResponse(final Releasable releasable) {
                try (Releasable ignored = releasable) {
                    onResponse.run();
                } catch (final Exception e) {
                    onFailure(e);
                }
            }

            @Override
            public void onFailure(final Exception e) {
                throw new AssertionError(e);
            }
        };
    }


}

