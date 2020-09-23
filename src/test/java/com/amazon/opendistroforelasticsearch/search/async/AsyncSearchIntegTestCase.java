package com.amazon.opendistroforelasticsearch.search.async;

import com.amazon.opendistroforelasticsearch.search.async.action.DeleteAsyncSearchAction;
import com.amazon.opendistroforelasticsearch.search.async.action.GetAsyncSearchAction;
import com.amazon.opendistroforelasticsearch.search.async.action.SubmitAsyncSearchAction;
import com.amazon.opendistroforelasticsearch.search.async.plugin.AsyncSearchPlugin;
import com.amazon.opendistroforelasticsearch.search.async.request.DeleteAsyncSearchRequest;
import com.amazon.opendistroforelasticsearch.search.async.request.GetAsyncSearchRequest;
import com.amazon.opendistroforelasticsearch.search.async.request.SubmitAsyncSearchRequest;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Collection;
import java.util.Collections;

@ESIntegTestCase.ClusterScope(scope=ESIntegTestCase.Scope.SUITE, numDataNodes=1)
public abstract class AsyncSearchIntegTestCase extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(AsyncSearchPlugin.class);
    }

    @Override
    protected Collection<Class<? extends Plugin>> transportClientPlugins() {
        return Collections.singletonList(AsyncSearchPlugin.class);
    }

    AsyncSearchResponse blockingSubmitAsyncSearch(SubmitAsyncSearchRequest request) {
        ActionFuture<AsyncSearchResponse> execute = submitAsyncSearch(request);
        return execute.actionGet(request.getWaitForCompletionTimeout().getMillis());
    }

    ActionFuture<AsyncSearchResponse> submitAsyncSearch(SubmitAsyncSearchRequest request) {
        return client().execute(SubmitAsyncSearchAction.INSTANCE, request);
    }

    AsyncSearchResponse getAsyncSearchResponse(GetAsyncSearchRequest request) {
        ActionFuture<AsyncSearchResponse> execute = getAsyncSearch(request);
        return execute.actionGet(request.getWaitForCompletionTimeout().getMillis());
    }

    ActionFuture<AsyncSearchResponse> getAsyncSearch(GetAsyncSearchRequest request) {
        return client().execute(GetAsyncSearchAction.INSTANCE, request);
    }

    AcknowledgedResponse blockingDeleteAsyncSearchRequest(DeleteAsyncSearchRequest request) {
        ActionFuture<AcknowledgedResponse> execute = deleteAsyncSearch(request);
        return execute.actionGet(100);
    }

    ActionFuture<AcknowledgedResponse> deleteAsyncSearch(DeleteAsyncSearchRequest request) {
        return client().execute(DeleteAsyncSearchAction.INSTANCE, request);
    }

    AsyncSearchResponse getAsyncSearchResponse(AsyncSearchResponse submitResponse, GetAsyncSearchRequest getAsyncSearchRequest) {
        AsyncSearchResponse getResponse = getAsyncSearchResponse(getAsyncSearchRequest);
        assert getResponse.getId().equals(submitResponse.getId());
        assert getResponse.getStartTimeMillis() == submitResponse.getStartTimeMillis();
        return getResponse;
    }
}
