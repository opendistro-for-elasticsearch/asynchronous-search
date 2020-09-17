package com.amazon.opendistroforelasticsearch.search.async;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.logging.log4j.util.Strings;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.CheckedFunction;
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
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

//FIXME : Deserialize the response from http entity
public class AsyncSearchRestIT extends ESRestTestCase {


    private static RestClient restClient;
    private final NamedXContentRegistry registry = new NamedXContentRegistry(
            new SearchModule(Settings.EMPTY, false, Collections.emptyList()).getNamedXContents());
    ;

    @Override
    protected boolean preserveClusterUponCompletion() {
        return true;
    }


    @Before
    public void indexDocuments() throws IOException {
        {
            Request doc1 = new Request(HttpPut.METHOD_NAME, "/index/type/1");
            doc1.setOptions(expectWarnings(RestIndexAction.TYPES_DEPRECATION_MESSAGE));
            doc1.setJsonEntity("{\"type\":\"type1\", \"id\":1, \"num\":10, \"num2\":50}");
            client().performRequest(doc1);
            Request doc2 = new Request(HttpPut.METHOD_NAME, "/index/type/2");
            doc2.setOptions(expectWarnings(RestIndexAction.TYPES_DEPRECATION_MESSAGE));
            doc2.setJsonEntity("{\"type\":\"type1\", \"id\":2, \"num\":20, \"num2\":40}");
            client().performRequest(doc2);
            Request doc3 = new Request(HttpPut.METHOD_NAME, "/index/type/3");
            doc3.setOptions(expectWarnings(RestIndexAction.TYPES_DEPRECATION_MESSAGE));
            doc3.setJsonEntity("{\"type\":\"type1\", \"id\":3, \"num\":50, \"num2\":35}");
            client().performRequest(doc3);
            Request doc4 = new Request(HttpPut.METHOD_NAME, "/index/type/4");
            doc4.setOptions(expectWarnings(RestIndexAction.TYPES_DEPRECATION_MESSAGE));
            doc4.setJsonEntity("{\"type\":\"type2\", \"id\":4, \"num\":100, \"num2\":10}");
            client().performRequest(doc4);
            Request doc5 = new Request(HttpPut.METHOD_NAME, "/index/type/5");
            doc5.setOptions(expectWarnings(RestIndexAction.TYPES_DEPRECATION_MESSAGE));
            doc5.setJsonEntity("{\"type\":\"type2\", \"id\":5, \"num\":100, \"num2\":10}");
            client().performRequest(doc5);
        }
        client().performRequest(new Request(HttpPost.METHOD_NAME, "/_refresh"));
    }


    @Test
    public void submitAsyncSearch() throws Exception {
        Request request = new Request("POST", "/_async_search");
        Response resp = client().performRequest(request);
        AsyncSearchResponse submitResponse = parseEntity(resp.getEntity(), AsyncSearchResponse::fromXContent);

        Request getRequest = new Request("GET", "/_async_search/" +submitResponse.getId());
        resp = client().performRequest(getRequest);
        AsyncSearchResponse getResponse = parseEntity(resp.getEntity(), AsyncSearchResponse::fromXContent);

        assertEquals(submitResponse.getId(), getResponse.getId());

    }

    @After
    public void closeClient() throws Exception {
        ESRestTestCase.closeClients();
    }

    public static class WaitFor {
        /**
         * Waits at most the specified time for the given task to evaluate to true
         *
         * @param task    The task which we hope evaluates to true before the time limit
         * @param maxWait The max amount of time to wait for the task to evaluate for true
         * @param unit    The time unit of the maxWait parameter
         * @throws Exception If the time limit expires before the task evaluates to true
         */
        public static void waitFor(Callable<Boolean> task, long maxWait, TimeUnit unit) throws Exception {
            long maxWaitMillis = TimeUnit.MILLISECONDS.convert(maxWait, unit);
            long pollTime = System.currentTimeMillis();
            long curTime;
            while (!task.call() && maxWaitMillis >= 0) {
                curTime = System.currentTimeMillis();
                maxWaitMillis -= (curTime - pollTime);
                pollTime = curTime;
            }
            if (maxWait < 0) {
                throw new TimeoutException("WaitFor timed out before task evaluated to true");
            }
        }

    }

    public static Response makeRequest(
            RestClient client,
            String method,
            String endpoint,
            Map<String, String> params,
            String jsonEntity,
            List<Header> headers
    ) throws IOException {
        HttpEntity httpEntity = Strings.isBlank(jsonEntity) ? null : new NStringEntity(jsonEntity, ContentType.APPLICATION_JSON);
        return makeRequest(client, method, endpoint, params, httpEntity, headers);
    }

    public static Response makeRequest(
            RestClient client,
            String method,
            String endpoint,
            Map<String, String> params,
            HttpEntity entity,
            List<Header> headers
    ) throws IOException {
        Request request = new Request(method, endpoint);

        RequestOptions.Builder options = RequestOptions.DEFAULT.toBuilder();
        if (headers != null) {
            headers.forEach(header -> options.addHeader(header.getName(), header.getValue()));
        }
        request.setOptions(options.build());

        if (params != null) {
            params.entrySet().forEach(it -> request.addParameter(it.getKey(), it.getValue()));
        }
        if (entity != null) {
            request.setEntity(entity);
        }
        return client.performRequest(request);
    }

    protected final <Resp> Resp parseEntity(final HttpEntity entity,
                                            final CheckedFunction<XContentParser, Resp, IOException> entityParser) throws IOException {
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


}