package com.amazon.opendistroforelasticsearch.search.async.memory;

import com.amazon.opendistroforelasticsearch.search.async.AbstractAsyncSearchContext;
import com.amazon.opendistroforelasticsearch.search.async.AsyncSearchContextId;
import com.amazon.opendistroforelasticsearch.search.async.AsyncSearchContextPermit;
import com.amazon.opendistroforelasticsearch.search.async.AsyncSearchId;
import com.amazon.opendistroforelasticsearch.search.async.listener.AsyncSearchResponseActionListener;
import com.amazon.opendistroforelasticsearch.search.async.response.AsyncSearchResponse;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchTask;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class ActiveAsyncSearchContext extends AbstractAsyncSearchContext {

    public enum Stage {
        INIT,
        RUNNING,
        COMPLETED,
        ABORTED,
        PERSISTED,
        FAILED
    }

    private final AtomicBoolean isRunning;
    private final AtomicBoolean isCompleted;
    private final AtomicBoolean isPartial;

    private final AtomicReference<ElasticsearchException> error;
    private final AtomicReference<SearchResponse> searchResponse;

    private SetOnce<SearchTask> searchTask = new SetOnce<>();
    private volatile long expirationTimeNanos;
    private final Boolean keepOnCompletion;
    private final AsyncSearchContextId asyncSearchContextId;
    private volatile TimeValue keepAlive;
    private volatile ActiveAsyncSearchContext.Stage stage;
    private final AsyncSearchContextPermit asyncSearchContextPermit;
    private AsyncSearchResponseActionListener progressActionListener;

    public ActiveAsyncSearchContext(AsyncSearchId asyncSearchId, TimeValue keepAlive, boolean keepOnCompletion,
                                    ThreadPool threadPool, AsyncSearchResponseActionListener progressActionListener) {
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

    public AsyncSearchResponseActionListener getProgressActionListener() {
        return progressActionListener;
    }

    public void prepareSearch(SearchTask searchTask) {
        this.searchTask.set(searchTask);
        this.setExpirationNanos(searchTask.getStartTime() + keepAlive.getNanos());
        this.stage = Stage.INIT;
    }

    public void acquireContextPermit(final ActionListener<Releasable> onPermitAcquired, TimeValue timeout, String reason) {
        asyncSearchContextPermit.asyncAcquirePermit(onPermitAcquired, timeout, reason);
    }

    public SearchTask getTask() {
        return searchTask.get();
    }

    public void performPostPersistenceCleanup() {
        searchResponse.set(null);
    }

    public void setExpirationNanos(long expirationTimeNanos) {
        this.expirationTimeNanos = expirationTimeNanos;
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
    public long getExpirationTimeNanos() {
        return expirationTimeNanos;
    }

    public long getExpirationTimeMillis() {
        return TimeUnit.NANOSECONDS.toMillis(this.expirationTimeNanos);
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
}
