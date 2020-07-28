package com.amazon.opendistroforelasticsearch.search.async.plugin;

import com.amazon.opendistroforelasticsearch.search.async.listener.AsyncSearchProgressActionListener;
import com.amazon.opendistroforelasticsearch.search.async.task.AsyncSearchTask;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.tasks.Task;

public class AsyncSearchActionFilter implements ActionFilter {

    private final Logger logger = LogManager.getLogger(getClass());

    @Override
    public int order() {
        return Integer.MIN_VALUE;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <Request extends ActionRequest, Response extends ActionResponse> void apply(Task task, String action, Request request, ActionListener<Response> listener, ActionFilterChain<Request, Response> chain) {
        if (task instanceof AsyncSearchTask) {
            AsyncSearchProgressActionListener progressListener = ((AsyncSearchTask)task).getProgressActionListener();
            ActionListener<Response> asyncSearchListener = (ActionListener<Response>) progressListener;
            logger.warn("In action filter listener {}", asyncSearchListener);
            progressListener.setOriginalListener((ActionListener<SearchResponse>)listener);
            chain.proceed(task, action, request, asyncSearchListener);
        } else {
            chain.proceed(task, action, request, listener);
        }
    }
}
