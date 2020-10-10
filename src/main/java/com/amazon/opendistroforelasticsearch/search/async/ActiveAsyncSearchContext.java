package com.amazon.opendistroforelasticsearch.search.async;

import com.amazon.opendistroforelasticsearch.search.async.listener.AsyncSearchProgressListener;
import com.amazon.opendistroforelasticsearch.search.async.reaper.AsyncSearchManagementService;
import com.amazon.opendistroforelasticsearch.search.async.response.AsyncSearchResponse;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchProgressActionListener;
import org.elasticsearch.action.search.SearchTask;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.HashSet;
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
         * The search execution has failed
         */
        FAILED
    }

    private final AtomicBoolean isRunning;
    private final AtomicBoolean isCompleted;
    private final AtomicBoolean isPartial;
    private final AtomicReference<ElasticsearchException> error;
    private final AtomicReference<SearchResponse> searchResponse;
    private SetOnce<SearchTask> searchTask = new SetOnce<>();
    private volatile long expirationTimeMillis;
    private final Boolean keepOnCompletion;
    private final AsyncSearchContextId asyncSearchContextId;
    private volatile TimeValue keepAlive;
    private volatile Stage stage;
    private final AsyncSearchContextPermit asyncSearchContextPermit;
    private AsyncSearchProgressListener progressActionListener;

    public ActiveAsyncSearchContext(AsyncSearchId asyncSearchId, TimeValue keepAlive, boolean keepOnCompletion,
                                    ThreadPool threadPool, AsyncSearchProgressListener progressActionListener) {
        super(asyncSearchId);
        this.asyncSearchContextId = asyncSearchId.getAsyncSearchContextId();
        this.keepOnCompletion = keepOnCompletion;
        this.isRunning = new AtomicBoolean(true);
        this.isPartial = new AtomicBoolean(true);
        this.isCompleted = new AtomicBoolean(false);
        this.error = new AtomicReference<>();
        this.searchResponse = new AtomicReference<>();
        this.keepAlive = keepAlive;
        this.asyncSearchContextPermit = new AsyncSearchContextPermit(asyncSearchContextId, threadPool);
        this.progressActionListener = progressActionListener;
    }

    @Override
    public Optional<SearchProgressActionListener> getSearchProgressActionListener() {
        return Optional.of(progressActionListener);
    }

    public void initializeTask(SearchTask searchTask) {
        this.searchTask.set(searchTask);
        this.searchTask.get().setProgressListener(progressActionListener);
    }

    public TimeValue getKeepAlive() {
        return keepAlive;
    }


    public void acquireContextPermit(final ActionListener<Releasable> onPermitAcquired, TimeValue timeout, String reason) {
        asyncSearchContextPermit.asyncAcquirePermit(onPermitAcquired, timeout, reason);
    }

    public void acquireAllContextPermit(final ActionListener<Releasable> onPermitAcquired, TimeValue timeout, String reason) {
        asyncSearchContextPermit.asyncAcquireALLPermits(onPermitAcquired, timeout, reason);
    }

    public SearchTask getTask() {
        return searchTask.get();
    }

    public void performPostPersistenceCleanup() {
        searchResponse.set(null);
    }

    public void setExpirationMillis(long expirationTimeMillis) {
        this.expirationTimeMillis = expirationTimeMillis;
    }

    public boolean needsPersistence() {
        return keepOnCompletion;
    }

    @Override
    public AsyncSearchResponse getAsyncSearchResponse() {
        return new AsyncSearchResponse(AsyncSearchId.buildAsyncId(getAsyncSearchId()), isPartial(), isRunning(), searchTask.get().getStartTime(),
                getExpirationTimeMillis(),
                isRunning() ? progressActionListener.partialResponse() : getFinalSearchResponse(), error.get());
    }

    public SearchResponse getFinalSearchResponse() {
        return searchResponse.get();
    }

    public boolean isRunning() {
        return isCancelled() == false && isRunning.get();
    }

    public boolean isCancelled() {
        return searchTask.get().isCancelled();
    }

    public boolean isPartial() {
        return isPartial.get();
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
        this.isCompleted.set(true);
        this.isPartial.set(false);
        this.isRunning.set(false);
        error.set(new ElasticsearchException(e));
        setStage(Stage.FAILED);
    }


    public void processFinalResponse(SearchResponse response) {
        setStage(Stage.COMPLETED);
        this.searchResponse.compareAndSet(null, response);
        this.isCompleted.set(true);
        this.isRunning.set(false);
        this.isPartial.set(false);
    }


    /**
     * Atomically validates and updates the state of the search
     * @param stage stage to set
     */
    public synchronized void setStage(Stage stage) {
        switch (stage) {
            case RUNNING:
                validateAndSetStage(Stage.INIT, stage);
                break;
            case COMPLETED:
            case ABORTED:
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
                "isRunning=" + isRunning +
                ", isCompleted=" + isCompleted +
                ", isPartial=" + isPartial +
                ", error=" + error +
                ", searchResponse=" + searchResponse +
                ", searchTask=" + searchTask +
                ", expirationTimeNanos=" + expirationTimeMillis +
                ", keepOnCompletion=" + keepOnCompletion +
                ", asyncSearchContextId=" + asyncSearchContextId +
                ", keepAlive=" + keepAlive +
                ", stage=" + stage +
                ", asyncSearchContextPermit=" + asyncSearchContextPermit +
                ", progressActionListener=" + progressActionListener +
                '}';
    }
}
