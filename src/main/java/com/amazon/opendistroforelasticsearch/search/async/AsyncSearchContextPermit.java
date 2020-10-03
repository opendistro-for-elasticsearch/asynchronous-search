package com.amazon.opendistroforelasticsearch.search.async;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.RunOnce;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/***
 * The permit needed by any mutating operation on {@link AbstractAsyncSearchContext} while it is being moved over to the
 * persistence store
 */
public class AsyncSearchContextPermit {

    private final Semaphore mutex = new Semaphore(1, true);
    private final AsyncSearchContextId asyncSearchContextId;
    private final ThreadPool threadPool;
    private static final Logger logger = LogManager.getLogger(AsyncSearchContextPermit.class);

    public AsyncSearchContextPermit(AsyncSearchContextId asyncSearchContextId, ThreadPool threadPool) {
        this.asyncSearchContextId = asyncSearchContextId;
        this.threadPool = threadPool;
    }


    private Releasable acquireContextLock(TimeValue timeout, final String details) throws RuntimeException {
        try {
            if (mutex.tryAcquire(timeout.getMillis(), TimeUnit.MILLISECONDS)) {
                final RunOnce release = new RunOnce(() -> {
                    mutex.release(1);
                });
                return release::run;
            } else {
                throw new RuntimeException(
                        "obtaining context lock"+ asyncSearchContextId +"timed out after " + timeout.getMillis() + "ms, " +
                                "previous lock details: [" + details + "] trying to lock for [" + details + "]");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("thread interrupted while trying to obtain context lock", e);
        }
    }

    /***
     * Acquire the permit in an async fashion so as to not block the thread while acquiring. The {@link ActionListener} is invoked if
     * the mutex was successfully acquired within the timeout. The caller has a responsibility of executing the {@link Releasable}
     * on completion or failure of the operation run within the permit
     *
     * @param onAcquired the releasable that must be invoked
     * @param timeout the timeout within which the permit must be acquired or deemed failed
     * @param reason the reason for acquiring the permit
     */
    public void asyncAcquirePermit(final ActionListener<Releasable> onAcquired, final TimeValue timeout, String reason)  {
        threadPool.executor(ThreadPool.Names.GENERIC).execute(new AbstractRunnable() {

            @Override
            public void onFailure(final Exception e) {
               onAcquired.onFailure(e);
            }

            @Override
            protected void doRun()  {
                final Releasable releasable = acquireContextLock(timeout, reason);
                logger.info("Successfully acquired permit for {}", reason);
                onAcquired.onResponse(() -> releasable.close());
            }
        });
    }
}
