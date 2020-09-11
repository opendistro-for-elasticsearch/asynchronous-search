package com.amazon.opendistroforelasticsearch.search.async.action;

import com.amazon.opendistroforelasticsearch.search.async.listener.AsyncSearchProgressActionListener;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchTask;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.tasks.Task;

public class AsyncSearchActionFilter implements ActionFilter {
    @Override
    public int order() {
        return Integer.MIN_VALUE;
    }

    @Override
    public <Request extends ActionRequest, Response extends ActionResponse> void apply(Task task, String action, Request request, ActionListener<Response> listener, ActionFilterChain<Request, Response> chain) {
        if (task instanceof SearchTask) {
            if (listener instanceof AsyncSearchProgressActionListener) {
                AsyncSearchProgressActionListener progressListener = (AsyncSearchProgressActionListener) listener;
                ((SearchTask) task).setProgressListener(progressListener);
            }
        }
        chain.proceed(task, action, request, listener);
    }
}
