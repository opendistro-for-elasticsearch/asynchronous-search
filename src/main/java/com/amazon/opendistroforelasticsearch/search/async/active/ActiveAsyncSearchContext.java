package com.amazon.opendistroforelasticsearch.search.async.active;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.search.SearchProgressActionListener;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchTask;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;

import com.amazon.opendistroforelasticsearch.search.async.AsyncSearchContext;
import com.amazon.opendistroforelasticsearch.search.async.AsyncSearchContextId;
import com.amazon.opendistroforelasticsearch.search.async.AsyncSearchId;
import com.amazon.opendistroforelasticsearch.search.async.listener.AsyncSearchContextListener;
import com.amazon.opendistroforelasticsearch.search.async.listener.AsyncSearchProgressListener;

/**
 * The context representing an ongoing search, keeps track of the underlying {@link SearchTask} and {@link SearchProgressActionListener}
 */
public class ActiveAsyncSearchContext extends AsyncSearchContext {

    private volatile SetOnce<SearchTask> searchTask;
    private volatile long expirationTimeMillis;
    private volatile long startTimeMillis;
    private final Boolean keepOnCompletion;
    private volatile TimeValue keepAlive;
    private final String nodeId;
    private volatile SetOnce<AsyncSearchId> asyncSearchId;
    private final AsyncSearchContextListener contextListener;
    private final ThreadPool threadPool;

    public ActiveAsyncSearchContext(AsyncSearchContextId asyncSearchContextId, String nodeId, TimeValue keepAlive, boolean keepOnCompletion,
                                    ThreadPool threadPool, AsyncSearchProgressListener searchProgressActionListener,
                                    AsyncSearchContextListener contextListener) {
        super(asyncSearchContextId);
        this.threadPool = threadPool;
        this.keepOnCompletion = keepOnCompletion;
        this.error = new AtomicReference<>();
        this.searchResponse = new AtomicReference<>();
        this.keepAlive = keepAlive;
        this.nodeId = nodeId;
        this.searchProgressActionListener = searchProgressActionListener;
        this.startTimeMillis = System.currentTimeMillis();
        this.searchTask = new SetOnce<>();
        this.asyncSearchId = new SetOnce<>();
        this.contextListener = contextListener;
    }

    @Override
    public SearchProgressActionListener getSearchProgressActionListener() {
        return searchProgressActionListener;
    }

    @Override
    public Stage getContextStage() {
        assert stage != null : "stage cannot be empty";
        return stage;
    }

    @Override
    public AsyncSearchId getAsyncSearchId() {
        return asyncSearchId.get();
    }

    public void setTask(SearchTask searchTask) {
        this.searchTask.set(searchTask);
        this.searchTask.get().setProgressListener(searchProgressActionListener);
        this.startTimeMillis = searchTask.getStartTime();
        this.asyncSearchId.set(new AsyncSearchId(nodeId, searchTask.getId(), getAsyncSearchContextId()));
        this.setStage(Stage.INIT);
    }

    public SearchTask getTask() {
        return searchTask.get();
    }

    public void setExpirationMillis(long expirationTimeMillis) {
        this.expirationTimeMillis = expirationTimeMillis;
    }

    public TimeValue getKeepAlive() {
        return keepAlive;
    }

    public boolean needsPersistence() {
        return keepOnCompletion;
    }


    @Override
    public SearchResponse getSearchResponse() {
        if (completed.get()) {
            return searchResponse.get();
        } else {
            return ((AsyncSearchProgressListener)searchProgressActionListener).partialResponse();
        }
    }

    @Override
    public long getExpirationTimeMillis() {
        return expirationTimeMillis;
    }

    @Override
    public long getStartTimeMillis() {
        return startTimeMillis;
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
                contextListener.onNewContext(getAsyncSearchContextId());
                break;
            case RUNNING:
                assert searchTask.get() != null : "search task cannot be null";
                contextListener.onContextRunning(getAsyncSearchContextId());
                validateAndSetStage(Stage.INIT, stage);
                break;
            case SUCCEEDED:
                assert searchTask.get() == null || searchTask.get().isCancelled() == false : "search task is cancelled";
                validateAndSetStage(Stage.RUNNING, stage);
                contextListener.onContextCompleted(getAsyncSearchContextId());
                break;
            case ABORTED:
                assert searchTask.get() == null || searchTask.get().isCancelled() : "search task should be cancelled";
                validateAndSetStage(Stage.RUNNING, stage);
                contextListener.onContextCancelled(getAsyncSearchContextId());
                break;
            case FAILED:
                validateAndSetStage(Stage.RUNNING, stage);
                contextListener.onContextFailed(getAsyncSearchContextId());
                break;
            case PERSISTED:
                validateAndSetStage(Stage.SUCCEEDED, stage);
                contextListener.onContextPersisted(getAsyncSearchContextId());
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
    public int hashCode() {
        return Objects.hash(asyncSearchId, completed, error, searchResponse, keepAlive, stage);
    }

    @Override
    public String toString() {
        return "ActiveAsyncSearchContext{" +
                ", completed=" + completed +
                ", error=" + error +
                ", searchResponse=" + searchResponse +
                ", searchTask=" + searchTask +
                ", expirationTimeNanos=" + expirationTimeMillis +
                ", keepOnCompletion=" + keepOnCompletion +
                ", keepAlive=" + keepAlive +
                ", stage=" + stage +
                ", asyncSearchContextPermit=" + asyncSearchContextPermit +
                ", progressActionListener=" + searchProgressActionListener +
                '}';
    }
}
