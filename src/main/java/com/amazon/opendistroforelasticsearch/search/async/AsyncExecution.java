package com.amazon.opendistroforelasticsearch.search.async;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.tasks.TaskId;

import java.util.concurrent.Executor;

public class AsyncExecution {

    private static final Logger logger = LogManager.getLogger(AsyncExecution.class);

    public static <T> void runAsync(CheckedSupplier<T, Exception> command,  Executor executor, ActionListener<T> listener) {
        try {
            executor.execute(() -> {
                T result;
                try {
                    result = command.get();
                } catch (Exception exc) {
                    listener.onFailure(exc);
                    return;
                }
                listener.onResponse(result);
            });
        } catch (Exception exc) {
            listener.onFailure(exc);
        }
    }
}
