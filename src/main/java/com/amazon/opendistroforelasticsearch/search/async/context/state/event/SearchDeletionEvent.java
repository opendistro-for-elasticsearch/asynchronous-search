package com.amazon.opendistroforelasticsearch.search.async.context.state.event;

import com.amazon.opendistroforelasticsearch.search.async.context.AsyncSearchContext;
import com.amazon.opendistroforelasticsearch.search.async.context.state.AsyncSearchContextEvent;

/**
 * Event triggered when an ongoing async search has been deleted.
 */
public class SearchDeletionEvent extends AsyncSearchContextEvent {

    public SearchDeletionEvent(AsyncSearchContext asyncSearchContext) {
        super(asyncSearchContext);
    }
}
