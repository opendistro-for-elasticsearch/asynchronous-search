package com.amazon.opendistroforelasticsearch.search.async.action;

import com.amazon.opendistroforelasticsearch.search.async.response.AcknowledgedResponse;
import org.elasticsearch.action.ActionType;

public class DeleteExpiredAsyncSearchesAction extends ActionType<AcknowledgedResponse> {

    public static final DeleteExpiredAsyncSearchesAction INSTANCE = new DeleteExpiredAsyncSearchesAction();
    public static final String NAME = "indices:data/write/delete_expired_async_searches";

    private DeleteExpiredAsyncSearchesAction() {
        super(NAME, AcknowledgedResponse::new);
    }

}
