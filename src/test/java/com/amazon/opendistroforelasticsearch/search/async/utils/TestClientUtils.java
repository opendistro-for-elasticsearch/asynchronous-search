package com.amazon.opendistroforelasticsearch.search.async.utils;

import com.amazon.opendistroforelasticsearch.search.async.action.DeleteAsyncSearchAction;
import com.amazon.opendistroforelasticsearch.search.async.action.GetAsyncSearchAction;
import com.amazon.opendistroforelasticsearch.search.async.action.SubmitAsyncSearchAction;
import com.amazon.opendistroforelasticsearch.search.async.request.DeleteAsyncSearchRequest;
import com.amazon.opendistroforelasticsearch.search.async.request.GetAsyncSearchRequest;
import com.amazon.opendistroforelasticsearch.search.async.request.SubmitAsyncSearchRequest;
import com.amazon.opendistroforelasticsearch.search.async.response.AcknowledgedResponse;
import com.amazon.opendistroforelasticsearch.search.async.response.AsyncSearchResponse;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.junit.Assert;

import java.util.Iterator;
import java.util.concurrent.CountDownLatch;

import static org.elasticsearch.common.unit.TimeValue.timeValueMillis;

public class TestClientUtils {
    static final String INDEX = ".async_search_response";
    static final BackoffPolicy STORE_BACKOFF_POLICY =
            BackoffPolicy.exponentialBackoff(timeValueMillis(250), 20);

    public static AsyncSearchResponse blockingSubmitAsyncSearch(Client client, SubmitAsyncSearchRequest request) {
        ActionFuture<AsyncSearchResponse> execute = submitAsyncSearch(client, request);
        return execute.actionGet();
    }

    static ActionFuture<AsyncSearchResponse> submitAsyncSearch(Client client, SubmitAsyncSearchRequest request) {
        return client.execute(SubmitAsyncSearchAction.INSTANCE, request);
    }

    public static AsyncSearchResponse blockingGetAsyncSearchResponse(Client client, GetAsyncSearchRequest request) {
        ActionFuture<AsyncSearchResponse> execute = getAsyncSearch(client, request);
        return execute.actionGet();
    }

    static ActionFuture<AsyncSearchResponse> getAsyncSearch(Client client, GetAsyncSearchRequest request) {
        return client.execute(GetAsyncSearchAction.INSTANCE, request);
    }

    public static AcknowledgedResponse blockingDeleteAsyncSearchRequest(Client client, DeleteAsyncSearchRequest request) {
        ActionFuture<AcknowledgedResponse> execute = deleteAsyncSearch(client, request);
        return execute.actionGet();
    }

    static ActionFuture<AcknowledgedResponse>
    deleteAsyncSearch(Client client, DeleteAsyncSearchRequest request) {
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
        } while (getResponse.isRunning());
        return getResponse;
    }

    public static void assertResponsePersistence(Client client, String id) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        Iterator<TimeValue> backoff = STORE_BACKOFF_POLICY.iterator();
        getResponseFromIndex(client, id, latch, backoff);
        latch.await();
    }

    public static void getResponseFromIndex(Client client, String id, CountDownLatch latch, Iterator<TimeValue> backoff) {
        client.get(new GetRequest(INDEX).id(id), new ActionListener<GetResponse>() {
            @Override
            public void onResponse(GetResponse getResponse) {
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                try {
                    if (!backoff.hasNext()) {
                        latch.countDown();
                        Assert.fail("Failed to persist async search response");
                    } else {
                        TimeValue wait = backoff.next();
                        Thread.sleep(wait.getMillis());
                        getResponseFromIndex(client,id, latch,backoff);
                    }
                } catch (InterruptedException ex) {
                    Assert.fail();
                    latch.countDown();
                }
            }
        });
    }
}
