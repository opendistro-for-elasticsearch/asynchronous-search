/*
 *   Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */

package com.amazon.opendistroforelasticsearch.search.asynchronous.restIT;

import com.amazon.opendistroforelasticsearch.search.asynchronous.request.DeleteAsynchronousSearchRequest;
import com.amazon.opendistroforelasticsearch.search.asynchronous.request.GetAsynchronousSearchRequest;
import com.amazon.opendistroforelasticsearch.search.asynchronous.request.SubmitAsynchronousSearchRequest;
import com.amazon.opendistroforelasticsearch.search.asynchronous.response.AsynchronousSearchResponse;
import com.amazon.opendistroforelasticsearch.search.asynchronous.utils.RestTestUtils;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.logging.log4j.LogManager;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchModule;
import org.junit.AfterClass;
import org.junit.Before;

import javax.management.MBeanServerInvocationHandler;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;

import static org.hamcrest.Matchers.containsString;


/**
 * Verifies asynchronous search APIs - submit, get, delete end to end using rest client
 */
public abstract class AsynchronousSearchRestTestCase extends ODFERestTestCase {

    private final NamedXContentRegistry registry = new NamedXContentRegistry(
            new SearchModule(Settings.EMPTY, false, Collections.emptyList()).getNamedXContents());


    @Before
    public void indexDocuments() throws IOException {
        {
            {
                Request doc1 = new Request(HttpPut.METHOD_NAME, "/test/_doc/1");
                doc1.setJsonEntity("{\"id\":1, \"num\":10, \"num2\":50}");
                client().performRequest(doc1);
                Request doc2 = new Request(HttpPut.METHOD_NAME, "/test/_doc/2");
                doc2.setJsonEntity("{ \"id\":2, \"num\":20, \"num2\":40}");
                client().performRequest(doc2);
                Request doc3 = new Request(HttpPut.METHOD_NAME, "/test/_doc/3");
                doc3.setJsonEntity("{ \"id\":3, \"num\":50, \"num2\":35}");
                client().performRequest(doc3);
                Request doc4 = new Request(HttpPut.METHOD_NAME, "/test/_doc/4");
                doc4.setJsonEntity("{ \"id\":4, \"num\":100, \"num2\":10}");
                client().performRequest(doc4);
                Request doc5 = new Request(HttpPut.METHOD_NAME, "/test/_doc/5");
                doc5.setJsonEntity("{ \"id\":5, \"num\":100, \"num2\":10}");
                client().performRequest(doc5);
            }

            {
                Request doc6 = new Request(HttpPut.METHOD_NAME, "/test1/_doc/1");
                doc6.setJsonEntity("{ \"id\":1, \"num\":10, \"num2\":50}");
                client().performRequest(doc6);
            }
            {
                Request roleMapping = new Request(HttpPut.METHOD_NAME,
                        "/_opendistro/_security/api/rolesmapping/asynchronous_search_full_access");
                roleMapping.setJsonEntity("{ \"users\": [\"admin\"] }");
                try {
                    LogManager.getLogger().info("create asynchronous search role mapping");
                    LogManager.getLogger().info(client().performRequest(roleMapping));
                } catch (Exception e) { //security plugin not enabled
                    LogManager.getLogger().error(e);
                }
                Request rolesMappings = new Request(HttpGet.METHOD_NAME,
                        "/_opendistro/_security/api/rolesmapping");
                LogManager.getLogger().info("get all security role mappings");
                LogManager.getLogger().info(client().performRequest(rolesMappings));
                Request users = new Request(HttpGet.METHOD_NAME,
                        "/_opendistro/_security/api/internalusers");
                LogManager.getLogger().info("get security users");
                LogManager.getLogger().info(client().performRequest(users));
                Request roles = new Request(HttpGet.METHOD_NAME,
                        "/_opendistro/_security/api/roles");
                LogManager.getLogger().info("get security roles");
                LogManager.getLogger().info(client().performRequest(roles));
            }
        }
        client().performRequest(new Request(HttpPost.METHOD_NAME, "/_refresh"));

    }

    AsynchronousSearchResponse executeGetAsynchronousSearch(GetAsynchronousSearchRequest getAsynchronousSearchRequest) throws IOException {
        Request getRequest = RestTestUtils.buildHttpRequest(getAsynchronousSearchRequest);
        Response resp = client().performRequest(getRequest);
        return parseEntity(resp.getEntity(), AsynchronousSearchResponse::fromXContent);
    }

    AsynchronousSearchResponse executeSubmitAsynchronousSearch(@Nullable SubmitAsynchronousSearchRequest submitAsynchronousSearchRequest)
            throws IOException {
        Request request = RestTestUtils.buildHttpRequest(submitAsynchronousSearchRequest);
        Response resp = client().performRequest(request);
        return parseEntity(resp.getEntity(), AsynchronousSearchResponse::fromXContent);
    }

    Response executeDeleteAsynchronousSearch(DeleteAsynchronousSearchRequest deleteAsynchronousSearchRequest) throws IOException {
        Request request = RestTestUtils.buildHttpRequest(deleteAsynchronousSearchRequest);
        return client().performRequest(request);
    }

    /**
     * We need to be able to dump the jacoco coverage before cluster is shut down.
     * The new internal testing framework removed some of the gradle tasks we were listening to
     * to choose a good time to do it. This will dump the executionData to file after each test.
     * TODO: This is also currently just overwriting integTest.exec with the updated execData without
     * resetting after writing each time. This can be improved to either write an exec file per test
     * or by letting jacoco append to the file
     */
    public interface IProxy {
        byte[] getExecutionData(boolean reset);

        void dump(boolean reset);

        void reset();
    }


    @AfterClass
    public static void dumpCoverage() throws IOException, MalformedObjectNameException {
        // jacoco.dir is set in esplugin-coverage.gradle, if it doesn't exist we don't
        // want to collect coverage so we can return early
        String jacocoBuildPath = System.getProperty("jacoco.dir");
        if (Strings.isNullOrEmpty(jacocoBuildPath)) {
            return;
        }

        String serverUrl = "service:jmx:rmi:///jndi/rmi://127.0.0.1:7777/jmxrmi";
        try (JMXConnector connector = JMXConnectorFactory.connect(new JMXServiceURL(serverUrl))) {
            IProxy proxy = MBeanServerInvocationHandler.newProxyInstance(
                    connector.getMBeanServerConnection(), new ObjectName("org.jacoco:type=Runtime"), IProxy.class,
                    false);

            Path path = org.elasticsearch.common.io.PathUtils.get(jacocoBuildPath + "/integTestRunner.exec");
            Files.write(path, proxy.getExecutionData(false));
        } catch (Exception ex) {
            throw new RuntimeException("Failed to dump coverage: " + ex);
        }
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

    protected AsynchronousSearchResponse getAssertedAsynchronousSearchResponse(AsynchronousSearchResponse submitResponse,
                                                                               GetAsynchronousSearchRequest getAsynchronousSearchRequest)
            throws IOException {
        AsynchronousSearchResponse getResponse;
        getResponse = executeGetAsynchronousSearch(getAsynchronousSearchRequest);
        assertEquals(submitResponse.getId(), getResponse.getId());
        assertEquals(submitResponse.getStartTimeMillis(), getResponse.getStartTimeMillis());
        return getResponse;
    }

    protected void assertRnf(Exception e) {
        assertTrue(e instanceof ResponseException);
        assertThat(e.getMessage(), containsString("resource_not_found_exception"));
        assertEquals(((ResponseException) e).getResponse().getStatusLine().getStatusCode(), 404);
    }

    protected static void assertHitCount(SearchResponse countResponse, long expectedHitCount) {
        final TotalHits totalHits = countResponse.getHits().getTotalHits();
        if (totalHits.relation != TotalHits.Relation.EQUAL_TO || totalHits.value != expectedHitCount) {
            fail("Count is " + totalHits + " but " + expectedHitCount
                    + " was expected. " + countResponse.toString());
        }
    }

    @Override
    protected boolean preserveClusterUponCompletion() {
        return true;
    }

    protected void updateClusterSettings(String settingKey, Object value) throws Exception {
        XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject()
                .startObject("persistent")
                .field(settingKey, value)
                .endObject()
                .endObject();
        Request request = new Request("PUT", "_cluster/settings");
        request.setJsonEntity(Strings.toString(builder));
        Response response = client().performRequest(request);
        assertEquals(RestStatus.OK, RestStatus.fromCode(response.getStatusLine().getStatusCode()));
    }

}
