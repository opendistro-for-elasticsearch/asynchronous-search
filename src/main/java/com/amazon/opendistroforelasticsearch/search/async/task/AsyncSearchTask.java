package com.amazon.opendistroforelasticsearch.search.async.task;

import com.amazon.opendistroforelasticsearch.search.async.AsyncSearchContextId;
import org.elasticsearch.action.search.SearchTask;
import org.elasticsearch.tasks.TaskId;

import java.util.Map;
import java.util.function.Consumer;

public class AsyncSearchTask extends SearchTask {

    private final AsyncSearchContextId asyncSearchContextId;
    private final Consumer<AsyncSearchContextId> onCancelledContextConsumer;

    public static final String NAME = "indices:data/read/async_search";

    public AsyncSearchTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers,
                           AsyncSearchContextId asyncSearchContextId, Consumer<AsyncSearchContextId> onCancelledContextConsumer) {
        super(id, type, action, null, parentTaskId, headers);
        this.asyncSearchContextId = asyncSearchContextId;
        this.onCancelledContextConsumer = onCancelledContextConsumer;
    }

    @Override
    public String getDescription() {
        return super.getDescription();
    }

    @Override
    protected void onCancelled() {
        onCancelledContextConsumer.accept(asyncSearchContextId);
    }

}
