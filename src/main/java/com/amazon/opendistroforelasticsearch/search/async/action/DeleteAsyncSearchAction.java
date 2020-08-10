package com.amazon.opendistroforelasticsearch.search.async.action;

import com.amazon.opendistroforelasticsearch.search.async.AsyncSearchResponse;
import org.elasticsearch.action.ActionType;

public class DeleteAsyncSearchAction extends ActionType<AsyncSearchResponse> {

    public static final DeleteAsyncSearchAction INSTANCE = new DeleteAsyncSearchAction();
    public static final String NAME = "indices:data/read/delete_async_search";

    private DeleteAsyncSearchAction() {
        super(NAME, AsyncSearchResponse::new);
    }

}