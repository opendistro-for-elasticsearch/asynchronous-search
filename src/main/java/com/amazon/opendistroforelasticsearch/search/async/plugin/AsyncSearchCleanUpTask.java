package com.amazon.opendistroforelasticsearch.search.async.plugin;

import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.tasks.TaskId;

import java.util.Map;

public class AsyncSearchCleanUpTask extends AllocatedPersistentTask {

    public AsyncSearchCleanUpTask(long id, String type, String action, String description,
                                  TaskId parentTask, Map<String, String> headers) {
        super(id, type, action, description, parentTask, headers);
    }
}
