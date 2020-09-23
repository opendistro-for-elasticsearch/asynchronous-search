package com.amazon.opendistroforelasticsearch.search.async;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

public final class AsyncSearchContextLockManager {

    private static final Logger logger = LogManager.getLogger(AsyncSearchContextLockManager.class);

    private final Map<AsyncSearchContextId, AsyncSearchContextLock> contextLocks = new HashMap<>();

    public AsyncSearchContextLock contextLock(final AsyncSearchContextId contextId, final String details,
                               final long lockTimeoutMS) throws RuntimeException {
        logger.trace("acquiring node contextLock on [{}], timeout [{}], details [{}]", contextId, lockTimeoutMS, details);
        final AsyncSearchContextLock contextLock;
        final boolean acquired;
        synchronized (contextLocks) {
            if (contextLocks.containsKey(contextId)) {
                contextLock = contextLocks.get(contextId);
                contextLock.incWaitCount();
                acquired = false;
            } else {
                contextLock = new AsyncSearchContextLock(contextId, details);
                contextLocks.put(contextId, contextLock);
                acquired = true;
            }
        }
        if (acquired == false) {
            boolean success = false;
            try {
                contextLock.acquire(lockTimeoutMS, details);
                success = true;
            } finally {
                if (success == false) {
                    contextLock.decWaitCount();
                }
            }
        }
        logger.trace("successfully acquired contextLock for [{}]", contextId);
        return null;
       /* return new AsyncSearchContextLock(contextId) { // new instance prevents double closing
            @Override
            protected void closeInternal() {
                contextLock.release();
                logger.trace("released contextLock for [{}]", contextId);
            }
        };*/
    }

    @FunctionalInterface
    public interface ContextLocker {
        AsyncSearchContextLock lock(AsyncSearchContextId shardId, String lockDetails, long lockTimeoutMS) throws RuntimeException;
    }

    private final class AsyncSearchContextLock {
        /*
         * This class holds a mutex for exclusive access and timeout / wait semantics
         * and a reference count to cleanup the context lock instance form the internal data
         * structure if nobody is waiting for it. the wait count is guarded by the same lock
         * to ensure exclusive access
         */
        private final Semaphore mutex = new Semaphore(1);
        private int waitCount = 1; // guarded by shardLocks
        private String lockDetails;
        private final AsyncSearchContextId asyncSearchContextId;

        AsyncSearchContextLock(final AsyncSearchContextId asyncSearchContextId, final String details) {
            this.asyncSearchContextId = asyncSearchContextId;
            mutex.acquireUninterruptibly();
            lockDetails = details;
        }

        protected void release() {
            mutex.release();
            decWaitCount();
        }

        void incWaitCount() {
            synchronized (contextLocks) {
                assert waitCount > 0 : "waitCount is " + waitCount + " but should be > 0";
                waitCount++;
            }
        }

            private void decWaitCount() {
                synchronized (contextLocks) {
                    assert waitCount > 0 : "waitCount is " + waitCount + " but should be > 0";
                    --waitCount;
                    logger.trace("context lock wait count for {} is now [{}]", asyncSearchContextId, waitCount);
                    if (waitCount == 0) {
                        logger.trace("last context lock wait decremented, removing lock for {}", asyncSearchContextId);
                    }
                }
            }

        void acquire(long timeoutInMillis, final String details) throws RuntimeException {
            try {
                if (mutex.tryAcquire(timeoutInMillis, TimeUnit.MILLISECONDS)) {
                    lockDetails = details;
                } else {
                    throw new RuntimeException(
                            "obtaining context lock timed out after " + timeoutInMillis + "ms, previous lock details: [" + lockDetails +
                                    "] trying to lock for [" + details + "]");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("thread interrupted while trying to obtain context lock", e);
            }
        }
    }
}

