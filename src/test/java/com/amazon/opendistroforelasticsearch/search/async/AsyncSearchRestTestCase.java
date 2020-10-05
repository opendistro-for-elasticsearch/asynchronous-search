package com.amazon.opendistroforelasticsearch.search.async;

import com.amazon.opendistroforelasticsearch.search.async.request.DeleteAsyncSearchRequest;
import com.amazon.opendistroforelasticsearch.search.async.request.GetAsyncSearchRequest;
import com.amazon.opendistroforelasticsearch.search.async.request.SubmitAsyncSearchRequest;
import com.amazon.opendistroforelasticsearch.search.async.response.AsyncSearchResponse;
import com.amazon.opendistroforelasticsearch.search.async.utils.RestTestUtils;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.action.document.RestIndexAction;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Collections;


/**
 * Verifies async search APIs - submit, get, delete end to end using rest client
 */
public abstract class AsyncSearchRestTestCase extends ESRestTestCase {


    private final NamedXContentRegistry registry = new NamedXContentRegistry(
            new SearchModule(Settings.EMPTY, false, Collections.emptyList()).getNamedXContents());


    final <Req extends ActionRequest, Resp> void assert404(
            Req request, CheckedFunction<Req, Resp, Exception> apiRequestExecutor) {
        try {
            apiRequestExecutor.apply(request);
            fail();
        } catch (Exception e) {
            assertTrue(e instanceof ResponseException);
            assertEquals(404,
                    ((ResponseException) e).getResponse().getStatusLine().getStatusCode());
        }
    }

    Response deleteAsyncSearchApi(DeleteAsyncSearchRequest deleteAsyncSearchRequest) throws IOException {
        Request request = RestTestUtils.buildHttpRequest(deleteAsyncSearchRequest);
        return client().performRequest(request);
    }


    @Override
    protected boolean preserveClusterUponCompletion() {
        return true;
    }


    @Before
    public void indexDocuments() throws IOException {
        {
            Request doc1 = new Request(HttpPut.METHOD_NAME, "/test/type/1");
            doc1.setOptions(expectWarnings(RestIndexAction.TYPES_DEPRECATION_MESSAGE));
            doc1.setJsonEntity("{\"type\":\"type1\", \"id\":1, \"num\":10, \"num2\":50}");
            client().performRequest(doc1);
            Request doc2 = new Request(HttpPut.METHOD_NAME, "/test/type/2");
            doc2.setOptions(expectWarnings(RestIndexAction.TYPES_DEPRECATION_MESSAGE));
            doc2.setJsonEntity("{\"type\":\"type1\", \"id\":2, \"num\":20, \"num2\":40}");
            client().performRequest(doc2);
            Request doc3 = new Request(HttpPut.METHOD_NAME, "/test/type/3");
            doc3.setOptions(expectWarnings(RestIndexAction.TYPES_DEPRECATION_MESSAGE));
            doc3.setJsonEntity("{\"type\":\"type1\", \"id\":3, \"num\":50, \"num2\":35}");
            client().performRequest(doc3);
            Request doc4 = new Request(HttpPut.METHOD_NAME, "/test/type/4");
            doc4.setOptions(expectWarnings(RestIndexAction.TYPES_DEPRECATION_MESSAGE));
            doc4.setJsonEntity("{\"type\":\"type2\", \"id\":4, \"num\":100, \"num2\":10}");
            client().performRequest(doc4);
            Request doc5 = new Request(HttpPut.METHOD_NAME, "/test/type/5");
            doc5.setOptions(expectWarnings(RestIndexAction.TYPES_DEPRECATION_MESSAGE));
            doc5.setJsonEntity("{\"type\":\"type2\", \"id\":5, \"num\":100, \"num2\":10}");
            client().performRequest(doc5);
        }
        client().performRequest(new Request(HttpPost.METHOD_NAME, "/_refresh"));
    }

    AsyncSearchResponse getAsyncSearchApi(GetAsyncSearchRequest getAsyncSearchRequest) throws IOException {
        Request getRequest = RestTestUtils.buildHttpRequest(getAsyncSearchRequest);
        Response resp = client().performRequest(getRequest);
        return parseEntity(resp.getEntity(), AsyncSearchResponse::fromXContent);
    }

    AsyncSearchResponse submitAsyncSearchApi(@Nullable SubmitAsyncSearchRequest submitAsyncSearchRequest) throws IOException {
        Request request;
        if (submitAsyncSearchRequest == null) { //search across all indices with default params without search source
            request = new Request("POST", "/_async_search");
        } else {
            request = RestTestUtils.buildHttpRequest(submitAsyncSearchRequest);
        }
        Response resp = client().performRequest(request);
        return parseEntity(resp.getEntity(), AsyncSearchResponse::fromXContent);

    }

    @After
    public void closeClient() throws Exception {
        ESRestTestCase.closeClients();
    }

    protected final <Resp> Resp parseEntity(final HttpEntity entity,
                                            final CheckedFunction<XContentParser, Resp, IOException> entityParser)
            throws IOException {
        if (entity == null) {
            throw new IllegalStateException("Response body expected but not returned");
        }
        if (entity.getContentType() == null) {
            throw new IllegalStateException("Elasticsearch didn't return the [Content-Type] header, unable to parse response body");
        }
        XContentType xContentType = XContentType.fromMediaTypeOrFormat(entity.getContentType().getValue());
        if (xContentType == null) {
            throw new IllegalStateException("Unsupported Content-Type: " + entity.getContentType().getValue());
        }
        try (XContentParser parser = xContentType.xContent().createParser(
                registry, DeprecationHandler.IGNORE_DEPRECATIONS, entity.getContent())) {
            return entityParser.apply(parser);
        }
    }

    public AsyncSearchResponse getAsyncSearchResponse(AsyncSearchResponse submitResponse,
                                                      GetAsyncSearchRequest getAsyncSearchRequest) throws IOException {
        AsyncSearchResponse getResponse;
        getResponse = getAsyncSearchApi(getAsyncSearchRequest);
        assertEquals(submitResponse.getId(), getResponse.getId());
        assertEquals(submitResponse.getStartTimeMillis(), getResponse.getStartTimeMillis());
        return getResponse;
    }
}

