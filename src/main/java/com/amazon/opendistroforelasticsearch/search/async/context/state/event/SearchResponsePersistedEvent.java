package com.amazon.opendistroforelasticsearch.search.async.context.state.event;

import com.amazon.opendistroforelasticsearch.search.async.context.AsyncSearchContext;
import com.amazon.opendistroforelasticsearch.search.async.context.state.AsyncSearchContextEvent;

/**
 * Event triggered when async search response is successfully stored in system index.
 */
public class SearchResponsePersistedEvent extends AsyncSearchContextEvent {

    public SearchResponsePersistedEvent(AsyncSearchContext asyncSearchContext) {
        super(asyncSearchContext);
    }
}

