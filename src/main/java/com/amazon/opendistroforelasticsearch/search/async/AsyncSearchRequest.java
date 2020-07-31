package com.amazon.opendistroforelasticsearch.search.async;

import com.amazon.opendistroforelasticsearch.search.async.listener.AsyncSearchProgressActionListener;
import com.amazon.opendistroforelasticsearch.search.async.task.AsyncSearchTask;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.tasks.TaskId;

import java.util.Map;

public class AsyncSearchRequest extends SearchRequest {

    private final Logger logger = LogManager.getLogger(getClass());

    public static final int DEFAULT_PRE_FILTER_SHARD_SIZE = 1;
    public static final int DEFAULT_BATCHED_REDUCE_SIZE = 5;

    private int batchedReduceSize = DEFAULT_BATCHED_REDUCE_SIZE;
    private long waitForCompletionTimeout;

    public AsyncSearchRequest() {
        super();
    }

    public AsyncSearchRequest(StreamInput streamInput) {
        super();
    }

    @Override
    public AsyncSearchTask createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        // generating description in a lazy way since source can be quite big
        AsyncSearchProgressActionListener progressActionListener = new AsyncSearchProgressActionListener();
        logger.warn("Creating new AsyncSearchProgressActionListener {}", progressActionListener);
        AsyncSearchTask asyncSearchTask = new AsyncSearchTask(id, type, action, null, parentTaskId, headers) {
            @Override
            public String getDescription() {
                StringBuilder sb = new StringBuilder();
                return sb.toString();
            }
        };
        logger.warn("Setting new AsyncSearchProgressActionListener {}", progressActionListener);
        asyncSearchTask.setProgressListener(progressActionListener);
        return asyncSearchTask;
    }

    /**
     * Returns the number of shard results that should be reduced at once on the coordinating node. This value should be used as a
     * protection mechanism to reduce the memory overhead per search request if the potential number of shards in the request can be large.
     */
    public int getBatchedReduceSize() {
        return batchedReduceSize;
    }

    public long getWaitForCompletionTimeout() {
        return waitForCompletionTimeout;
    }

    public void setWaitForCompletionTimeout(long waitForCompletionTimeout) {
        this.waitForCompletionTimeout = waitForCompletionTimeout;
    }
}
