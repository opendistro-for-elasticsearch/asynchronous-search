package com.amazon.opendistroforelasticsearch.search.async.context.state.event;

import com.amazon.opendistroforelasticsearch.search.async.context.AsyncSearchContext;
import com.amazon.opendistroforelasticsearch.search.async.context.state.AsyncSearchContextEvent;
import org.elasticsearch.action.search.SearchResponse;

public class SearchSuccessfulEvent extends AsyncSearchContextEvent {

    private SearchResponse searchResponse;

    SearchSuccessfulEvent(AsyncSearchContext asyncSearchContext, SearchResponse searchResponse) {
        super(asyncSearchContext);
        this.searchResponse = searchResponse;
    }

    @Override
    public String eventName() {
        return "search_completed";
    }

    public SearchResponse getSearchResponse() {
        return searchResponse;
    }
}
