package com.amazon.opendistroforelasticsearch.search.async.action;

import com.amazon.opendistroforelasticsearch.search.async.response.AsyncSearchStatsResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.common.io.stream.Writeable;

public class AsyncSearchStatsAction extends ActionType<AsyncSearchStatsResponse> {


    public static final AsyncSearchStatsAction INSTANCE = new AsyncSearchStatsAction();
    public static final String NAME = "cluster:admin/async_search_stats_action";

    /**
     * Constructor
     */
    private AsyncSearchStatsAction() {
        super(NAME, AsyncSearchStatsResponse::new);
    }

    @Override
    public Writeable.Reader<AsyncSearchStatsResponse> getResponseReader() {
        return AsyncSearchStatsResponse::new;
    }
}
