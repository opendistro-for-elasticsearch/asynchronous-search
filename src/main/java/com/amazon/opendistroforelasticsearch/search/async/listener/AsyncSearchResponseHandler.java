package com.amazon.opendistroforelasticsearch.search.async.listener;

import com.amazon.opendistroforelasticsearch.search.async.AsyncSearchResponse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.function.Supplier;

public class AsyncSearchResponseHandler {

    private static ThreadPool threadPool;

    private static final Logger logger = LogManager.getLogger(AsyncSearchResponseHandler.class);

    private static AsyncSearchResponseHandler asyncSearchResponseHandler = new AsyncSearchResponseHandler();

    public static void initialize(ThreadPool threadPool){
        AsyncSearchResponseHandler.threadPool = threadPool;
    }

    public static AsyncSearchResponseHandler getInstance() {
        return asyncSearchResponseHandler;
    }

    public static void scheduleResponse(ActionListener<AsyncSearchResponse> listener, Supplier<SearchResponse> searchResponse) {
        logger.warn("Scheduling a search Response");
        threadPool.schedule(
                () -> {
                    logger.warn("Responding with a search response after 5s");
                    //listener.onResponse(searchResponse.get());
                },
                TimeValue.timeValueSeconds(5), ThreadPool.Names.GENERIC);
    }
}
