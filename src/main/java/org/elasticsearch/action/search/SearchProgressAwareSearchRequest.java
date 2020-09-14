package org.elasticsearch.action.search;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;

public class SearchProgressAwareSearchRequest extends SearchRequest {
    public SearchProgressAwareSearchRequest() {
    }

    public SearchProgressAwareSearchRequest(SearchRequest searchRequest) {
        super(searchRequest);
    }

    public SearchProgressAwareSearchRequest(String... indices) {
        super(indices);
    }

    public SearchProgressAwareSearchRequest(String[] indices, SearchSourceBuilder source) {
        super(indices, source);
    }

    public SearchProgressAwareSearchRequest(StreamInput in) throws IOException {
        super(in);
    }

    public SearchProgressActionListener getSearchProgressActionListener() {
        return searchProgressActionListener;
    }

    public void setSearchProgressActionListener(SearchProgressActionListener searchProgressActionListener) {
        this.searchProgressActionListener = searchProgressActionListener;
    }

    private SearchProgressActionListener searchProgressActionListener;
}
