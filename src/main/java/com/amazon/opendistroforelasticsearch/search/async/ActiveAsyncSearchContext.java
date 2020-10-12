package com.amazon.opendistroforelasticsearch.search.async;

import com.amazon.opendistroforelasticsearch.search.async.listener.AsyncSearchProgressListener;
import com.amazon.opendistroforelasticsearch.search.async.reaper.AsyncSearchManagementService;
import com.amazon.opendistroforelasticsearch.search.async.response.AsyncSearchResponse;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchProgressActionListener;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchTask;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The context representing an ongoing search, keeps track of the underlying {@link SearchTask} and {@link SearchProgressActionListener}
 */
public class ActiveAsyncSearchContext extends AbstractAsyncSearchContext {

    /**
     * The state of the async search.
     */
    public enum Stage {
        /**
         * At the start of the search, before the {@link SearchTask starts to run}
         */
        INIT,
        /**
         * The search state actually has been started
         */
        RUNNING,
        /**
         * The search has completed successfully
         */
        COMPLETED,
        /**
         * The search has been cancelled either by the user, task API cancel or {@link AsyncSearchManagementService}
         */
        ABORTED,
        /**
         * The context has been persisted to system index
         */
        PERSISTED,
        /**
         * The context has failed to persist to system index
         */
        PERSIST_FAILED,
        /**
         * The search execution has failed
         */
        FAILED
    }

    private final AtomicBoolean isCompleted;
    private final AtomicReference<ElasticsearchException> error;
    private final AtomicReference<SearchResponse> searchResponse;
    private volatile SetOnce<SearchTask> searchTask = new SetOnce<>();
    private volatile long expirationTimeMillis;
    private volatile long startTimeMillis;
    private final Boolean keepOnCompletion;
    private volatile TimeValue keepAlive;
    private volatile Stage stage;
    private final AsyncSearchContextPermit asyncSearchContextPermit;
    private AsyncSearchProgressListener progressActionListener;
    private String nodeId;
    @Nullable
    private AsyncSearchId asyncSearchId;

    public ActiveAsyncSearchContext(AsyncSearchContextId asyncSearchContextId, String nodeId, TimeValue keepAlive, boolean keepOnCompletion,
                                    ThreadPool threadPool, AsyncSearchProgressListener progressActionListener) {
        super(asyncSearchContextId);
        this.keepOnCompletion = keepOnCompletion;
        this.isCompleted = new AtomicBoolean(false);
        this.error = new AtomicReference<>();
        this.searchResponse = new AtomicReference<>();
        this.keepAlive = keepAlive;
        this.nodeId = nodeId;
        this.asyncSearchContextPermit = new AsyncSearchContextPermit(asyncSearchContextId, threadPool);
        this.progressActionListener = progressActionListener;
        this.startTimeMillis = System.currentTimeMillis();
    }

    @Override
    public Optional<SearchProgressActionListener> getSearchProgressActionListener() {
        return Optional.of(progressActionListener);
    }

    @Override
    public AsyncSearchId getAsyncSearchId() {
        return asyncSearchId;
    }

    public void initializeTask(SearchTask searchTask) {
        this.searchTask.set(searchTask);
        this.searchTask.get().setProgressListener(progressActionListener);
        this.startTimeMillis = searchTask.getStartTime();
        this.asyncSearchId = new AsyncSearchId(nodeId, searchTask.getId(), getAsyncSearchContextId());
    }

    public TimeValue getKeepAlive() {
        return keepAlive;
    }


    public void acquireContextPermit(final ActionListener<Releasable> onPermitAcquired, TimeValue timeout, String reason) {
        asyncSearchContextPermit.asyncAcquirePermit(onPermitAcquired, timeout, reason);
    }

    public void acquireAllContextPermit(final ActionListener<Releasable> onPermitAcquired, TimeValue timeout, String reason) {
        asyncSearchContextPermit.asyncAcquireAllPermits(onPermitAcquired, timeout, reason);
    }

    public SearchTask getTask() {
        return searchTask.get();
    }

    public void setExpirationMillis(long expirationTimeMillis) {
        this.expirationTimeMillis = expirationTimeMillis;
    }

    public boolean needsPersistence() {
        return keepOnCompletion;
    }

    @Override
    public AsyncSearchResponse getAsyncSearchResponse() {
        return new AsyncSearchResponse(AsyncSearchId.buildAsyncId(getAsyncSearchId()), isPartial(), isRunning(), startTimeMillis,
                getExpirationTimeMillis(), isRunning() ? progressActionListener.partialResponse() : getFinalSearchResponse(), error.get());
    }

    public SearchResponse getFinalSearchResponse() {
        return searchResponse.get();
    }

    public boolean isRunning() {
        return isCompleted.get() == false;
    }

    public boolean isPartial() {
        return isCompleted.get() == false || isCancelled();
    }

    public boolean isCancelled() {
        return searchTask.get() != null && searchTask.get().isCancelled();
    }

    @Override
    public long getExpirationTimeMillis() {
        return expirationTimeMillis;
    }

    @Override
    public Source getSource() {
        return Source.IN_MEMORY;
    }

    public Stage getStage() {
        return stage;
    }


    public void processFailure(Exception e) {
        if (isCompleted.compareAndSet(false, true)) {
            error.set(new ElasticsearchException(e));
            setStage(Stage.FAILED);
            this.searchTask = null;
        }
    }


    public void processFinalResponse(SearchResponse response) {
        if (isCompleted.compareAndSet(false, true)) {
            this.searchResponse.compareAndSet(null, response);
            setStage(Stage.COMPLETED);
            this.searchTask = null;
        }
    }


    /**
     * Atomically validates and updates the state of the search
     *
     * @param stage stage to set
     */
    public synchronized void setStage(Stage stage) {
        switch (stage) {
            case INIT:
                validateAndSetStage(null, stage);
                break;
            case RUNNING:
                assert searchTask.get() != null : "search task cannot be null";
                validateAndSetStage(Stage.INIT, stage);
                break;
            case COMPLETED:
                assert searchTask.get() == null || searchTask.get().isCancelled() == false : "search task is cancelled";
            case ABORTED:
                assert searchTask.get() == null || searchTask.get().isCancelled() : "search task should be cancelled";
            case FAILED:
                validateAndSetStage(Stage.RUNNING, stage);
                break;
            case PERSISTED:
                validateAndSetStage(Stage.COMPLETED, stage);
                break;
            default:
                throw new IllegalArgumentException("unknown AsyncSearchContext.Stage [" + stage + "]");
        }
    }

    private void validateAndSetStage(Stage expected, Stage next) {
        if (stage != expected) {
            throw new IllegalStateException("can't move to stage [" + next + "]. current stage: ["
                    + stage + "] (expected [" + expected + "])");
        }
        stage = next;
    }

    @Override
    public String toString() {
        return "ActiveAsyncSearchContext{" +
                ", isCompleted=" + isCompleted +
                ", error=" + error +
                ", searchResponse=" + searchResponse +
                ", searchTask=" + searchTask +
                ", expirationTimeNanos=" + expirationTimeMillis +
                ", keepOnCompletion=" + keepOnCompletion +
                ", keepAlive=" + keepAlive +
                ", stage=" + stage +
                ", asyncSearchContextPermit=" + asyncSearchContextPermit +
                ", progressActionListener=" + progressActionListener +
                '}';
    }
}
