package com.amazon.opendistroforelasticsearch.search.async.context.state.event;

import com.amazon.opendistroforelasticsearch.search.async.context.AsyncSearchContext;
import com.amazon.opendistroforelasticsearch.search.async.context.state.AsyncSearchContextEvent;
import org.elasticsearch.action.search.SearchResponse;

/**
 * Event triggered when async search completes with a successful search response.
 */
public class SearchSuccessfulEvent extends AsyncSearchContextEvent {

    private SearchResponse searchResponse;

    public SearchSuccessfulEvent(AsyncSearchContext asyncSearchContext, SearchResponse searchResponse) {
        super(asyncSearchContext);
        this.searchResponse = searchResponse;
    }

    public SearchResponse getSearchResponse() {
        return searchResponse;
    }
}
