package com.amazon.opendistroforelasticsearch.search.async.utils;

import com.amazon.opendistroforelasticsearch.search.async.AsyncSearchResponse;
import com.amazon.opendistroforelasticsearch.search.async.action.DeleteAsyncSearchAction;
import com.amazon.opendistroforelasticsearch.search.async.action.GetAsyncSearchAction;
import com.amazon.opendistroforelasticsearch.search.async.action.SubmitAsyncSearchAction;
import com.amazon.opendistroforelasticsearch.search.async.request.DeleteAsyncSearchRequest;
import com.amazon.opendistroforelasticsearch.search.async.request.GetAsyncSearchRequest;
import com.amazon.opendistroforelasticsearch.search.async.request.SubmitAsyncSearchRequest;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;

import static org.junit.Assert.assertEquals;

public class TestClientUtils {

    public static AsyncSearchResponse blockingSubmitAsyncSearch(Client client, SubmitAsyncSearchRequest request) {
        ActionFuture<AsyncSearchResponse> execute = submitAsyncSearch(client, request);
        return execute.actionGet(request.getWaitForCompletionTimeout().getMillis());
    }

    static ActionFuture<AsyncSearchResponse> submitAsyncSearch(Client client, SubmitAsyncSearchRequest request) {
        return client.execute(SubmitAsyncSearchAction.INSTANCE, request);
    }

    public static AsyncSearchResponse blockingGetAsyncSearchResponse(Client client, GetAsyncSearchRequest request) {
        ActionFuture<AsyncSearchResponse> execute = getAsyncSearch(client, request);
        return execute.actionGet(request.getWaitForCompletionTimeout().getMillis());
    }

    static ActionFuture<AsyncSearchResponse> getAsyncSearch(Client client, GetAsyncSearchRequest request) {
        return client.execute(GetAsyncSearchAction.INSTANCE, request);
    }

    public static AcknowledgedResponse blockingDeleteAsyncSearchRequest(Client client, DeleteAsyncSearchRequest request) {
        ActionFuture<org.elasticsearch.action.support.master.AcknowledgedResponse> execute = deleteAsyncSearch(client, request);
        return execute.actionGet(100);
    }

    static ActionFuture<org.elasticsearch.action.support.master.AcknowledgedResponse> deleteAsyncSearch(Client client,
                                                                                                        DeleteAsyncSearchRequest request) {
        return client.execute(DeleteAsyncSearchAction.INSTANCE, request);
    }

    /**
     * Match with submit async search response.
     */
    static AsyncSearchResponse blockingGetAsyncSearchResponse(Client client, AsyncSearchResponse submitResponse,
                                                              GetAsyncSearchRequest getAsyncSearchRequest) {
        AsyncSearchResponse getResponse = blockingGetAsyncSearchResponse(client, getAsyncSearchRequest);
        assert getResponse.getId().equals(submitResponse.getId());
        assert getResponse.getStartTimeMillis() == submitResponse.getStartTimeMillis();
        return getResponse;
    }

    public static AsyncSearchResponse getFinalAsyncSearchResponse(Client client, AsyncSearchResponse submitResponse,
                                                                  GetAsyncSearchRequest getAsyncSearchRequest) {
        AsyncSearchResponse getResponse;
        do {
            getResponse = blockingGetAsyncSearchResponse(client, submitResponse, getAsyncSearchRequest);
            assertEquals(submitResponse.getExpirationTimeMillis(), getResponse.getExpirationTimeMillis());
        } while (getResponse.isRunning());
        return getResponse;
    }
}
