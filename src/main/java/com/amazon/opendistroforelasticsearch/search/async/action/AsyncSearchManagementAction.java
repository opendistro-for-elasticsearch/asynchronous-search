package com.amazon.opendistroforelasticsearch.search.async.action;

import com.amazon.opendistroforelasticsearch.search.async.response.AcknowledgedResponse;
import org.elasticsearch.action.ActionType;

public class AsyncSearchManagementAction extends ActionType<AcknowledgedResponse> {

    public static final AsyncSearchManagementAction INSTANCE = new AsyncSearchManagementAction();
    public static final String NAME = "indices:data/read/async_search/management";

    private AsyncSearchManagementAction() {
        super(NAME, AcknowledgedResponse::new);
    }

}
