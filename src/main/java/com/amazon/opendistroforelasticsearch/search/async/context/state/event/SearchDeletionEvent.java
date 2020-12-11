package com.amazon.opendistroforelasticsearch.search.async.context.state.event;

import com.amazon.opendistroforelasticsearch.search.async.context.active.AsyncSearchActiveContext;
import com.amazon.opendistroforelasticsearch.search.async.context.state.AsyncSearchContextEvent;

public class SearchDeletionEvent extends AsyncSearchContextEvent {

    public SearchDeletionEvent(AsyncSearchActiveContext asyncSearchActiveContext) {
        super(asyncSearchActiveContext);
    }
}
