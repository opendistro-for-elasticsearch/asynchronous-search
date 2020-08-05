package com.amazon.opendistroforelasticsearch.search.async.action;

import com.amazon.opendistroforelasticsearch.search.async.AsyncSearchResponse;
import org.elasticsearch.action.ActionType;

public class GetAsyncSearchAction extends ActionType<AsyncSearchResponse> {

    public static final GetAsyncSearchAction INSTANCE = new GetAsyncSearchAction();
    public static final String NAME = "indices:data/read/get_async_search";

    private GetAsyncSearchAction() {
        super(NAME, AsyncSearchResponse::new);
    }

}
