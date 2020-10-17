package com.amazon.opendistroforelasticsearch.search.async;

import com.amazon.opendistroforelasticsearch.search.async.persistence.AsyncSearchPersistenceContext;
import com.amazon.opendistroforelasticsearch.search.async.persistence.AsyncSearchPersistenceModel;
import com.amazon.opendistroforelasticsearch.search.async.persistence.AsyncSearchPersistenceService;
import com.amazon.opendistroforelasticsearch.search.async.request.SubmitAsyncSearchRequest;
import com.amazon.opendistroforelasticsearch.search.async.response.AsyncSearchResponse;
import com.amazon.opendistroforelasticsearch.search.async.utils.TestClientUtils;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.junit.After;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class AsyncSearchPersistenceServiceIT extends AsyncSearchSingleNodeTestCase {

    private ThreadPool threadPool;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool("persistenceServiceTests");
    }

    public void testCreateAndGetAndDelete() throws IOException, InterruptedException {

        AsyncSearchPersistenceService persistenceService = getInstanceFromNode(AsyncSearchPersistenceService.class);
        TransportService transportService = getInstanceFromNode(TransportService.class);
        NamedWriteableRegistry namedWriteableRegistry = writableRegistry();
        AsyncSearchResponse asyncSearchResponse = getAsyncSearchResponse();

        AsyncSearchContextId asyncSearchContextId = new AsyncSearchContextId(UUIDs.base64UUID(), randomInt(100));
        AsyncSearchId newAsyncSearchId = new AsyncSearchId(transportService.getLocalNode().getId(), 1,
                asyncSearchContextId);
        String id = AsyncSearchId.buildAsyncId(newAsyncSearchId);
        AsyncSearchResponse newAsyncSearchResponse = new AsyncSearchResponse(id,
                asyncSearchResponse.isRunning(),
                asyncSearchResponse.getStartTimeMillis(), asyncSearchResponse.getExpirationTimeMillis(),
                asyncSearchResponse.getSearchResponse(),
                asyncSearchResponse.getError());
        createDoc(persistenceService, newAsyncSearchResponse);

        CountDownLatch getLatch = new CountDownLatch(1);
        persistenceService.getResponse(newAsyncSearchResponse.getId(), ActionListener.wrap(
                r -> verifyResponse(newAsyncSearchResponse, getLatch, r), exception -> failure(getLatch)));
        getLatch.await();

        CountDownLatch deleteLatch = new CountDownLatch(1);
        persistenceService.deleteResponse(newAsyncSearchResponse.getId(),
                ActionListener.wrap((Boolean b) -> {
                    deleteLatch.countDown();
                }, exception -> failure(deleteLatch)));
        deleteLatch.await();

        //assert failure
        CountDownLatch getLatch1 = new CountDownLatch(1);
        persistenceService.getResponse(newAsyncSearchResponse.getId(), ActionListener.wrap(
                (AsyncSearchPersistenceContext r) -> failure(getLatch1),
                exception -> {
                    assertRnf(getLatch, exception);
                }));
        getLatch.await();

    }

    public void testGetAndDeleteNonExistentId() throws InterruptedException, IOException {
        AsyncSearchPersistenceService persistenceService = getInstanceFromNode(AsyncSearchPersistenceService.class);
        TransportService transportService = getInstanceFromNode(TransportService.class);
        NamedWriteableRegistry namedWriteableRegistry = writableRegistry();
        AsyncSearchId asyncSearchId = generateNewAsynSearchId(transportService);
        AsyncSearchPersistenceContext model1 = new AsyncSearchPersistenceContext(asyncSearchId, new AsyncSearchPersistenceModel(System.currentTimeMillis(),
                System.currentTimeMillis() + new TimeValue(10, TimeUnit.DAYS).getMillis(),
                getBytesReferenceObject(asyncSearchId)));
        CountDownLatch createLatch = new CountDownLatch(1);
        persistenceService.createResponse(model1, ActionListener.wrap(
                response -> createLatch.countDown(), ex -> failure(createLatch)));
        createLatch.await();
        CountDownLatch latch = new CountDownLatch(2);
        //assert failure
        persistenceService.getResponse("id", ActionListener.wrap(
                (AsyncSearchPersistenceContext r) -> failure(latch),
                exception -> {
                    assertRnf(latch, exception);
                }));
        //assert failure
        persistenceService.deleteResponse("id", ActionListener.wrap(
                (r) -> {
                    assert !r;
                    latch.countDown();
                },
                exception -> {
                    failure(latch);
                }));
        latch.await();

    }


    public void testCreateConcurrentDocsWhenIndexNotExists() throws InterruptedException, IOException {
        AsyncSearchPersistenceService persistenceService = getInstanceFromNode(AsyncSearchPersistenceService.class);
        TransportService transportService = getInstanceFromNode(TransportService.class);
        NamedWriteableRegistry namedWriteableRegistry = writableRegistry();

        AsyncSearchId asyncSearchId1 = generateNewAsynSearchId(transportService);
        AsyncSearchId asyncSearchId2 = generateNewAsynSearchId(transportService);
        AsyncSearchPersistenceContext model1 = new AsyncSearchPersistenceContext(asyncSearchId1, new AsyncSearchPersistenceModel(System.currentTimeMillis(),
                System.currentTimeMillis() + new TimeValue(10, TimeUnit.DAYS).getMillis(), getBytesReferenceObject(asyncSearchId1)));

        AsyncSearchPersistenceContext model2 = new AsyncSearchPersistenceContext(asyncSearchId2, new AsyncSearchPersistenceModel(System.currentTimeMillis(),
                System.currentTimeMillis() + new TimeValue(10, TimeUnit.DAYS).getMillis(),
                getBytesReferenceObject(asyncSearchId1)));
        CountDownLatch createLatch = new CountDownLatch(2);
        threadPool.generic().execute(() -> persistenceService.createResponse(model1, ActionListener.wrap(
                response -> createLatch.countDown(), ex -> failure(createLatch))));
        threadPool.generic().execute(() -> persistenceService.createResponse(model2, ActionListener.wrap(
                response -> createLatch.countDown(), ex -> failure(createLatch))));
        createLatch.await();

        CountDownLatch getLatch1 = new CountDownLatch(1);
        String id1 = AsyncSearchId.buildAsyncId(asyncSearchId1);
        String id2 = AsyncSearchId.buildAsyncId(asyncSearchId2);
        persistenceService.getResponse(id1, ActionListener.wrap(
                (AsyncSearchPersistenceContext r) -> getLatch1.countDown(), exception -> failure(getLatch1)));
        getLatch1.await();

        CountDownLatch getLatch2 = new CountDownLatch(1);
        persistenceService.getResponse(id2, ActionListener.wrap(
                (AsyncSearchPersistenceContext r) -> getLatch2.countDown(), exception -> failure(getLatch2)));
        getLatch2.await();
    }


    public void testUpdateExpiration() throws InterruptedException, IOException {
        AsyncSearchPersistenceService persistenceService = getInstanceFromNode(AsyncSearchPersistenceService.class);
        AsyncSearchResponse asyncSearchResponse = getAsyncSearchResponse();

        CountDownLatch updateLatch = new CountDownLatch(1);
        long newExpirationTime = System.currentTimeMillis() + new TimeValue(10, TimeUnit.DAYS).getMillis();
        persistenceService.updateExpirationTime(asyncSearchResponse.getId(),
                newExpirationTime, ActionListener.wrap(r -> updateLatch.countDown(),
                        e -> failure(updateLatch)));
        updateLatch.await();

        CountDownLatch getLatch = new CountDownLatch(1);
        persistenceService.getResponse(asyncSearchResponse.getId(), ActionListener.wrap(r -> {
            AsyncSearchResponse asyncSearchResponse1 = r.getAsyncSearchResponse();
            assert asyncSearchResponse1.getExpirationTimeMillis() == newExpirationTime;
            assert asyncSearchResponse1.getId().equals(asyncSearchResponse.getId());
            getLatch.countDown();
        }, e -> failure(getLatch)));
        getLatch.await();
    }

    public void testGetExpiredDocThrowsRnf() throws InterruptedException, IOException {
        TransportService transportService = getInstanceFromNode(TransportService.class);
        AsyncSearchPersistenceService persistenceService = getInstanceFromNode(AsyncSearchPersistenceService.class);
        NamedWriteableRegistry namedWriteableRegistry = writableRegistry();
        AsyncSearchResponse asyncSearchResponse = getAsyncSearchResponse();

        AsyncSearchId asyncSearchId = generateNewAsynSearchId(transportService);
        AsyncSearchResponse newAsyncSearchResponse = new AsyncSearchResponse(AsyncSearchId.buildAsyncId(asyncSearchId),
                asyncSearchResponse.isRunning(),
                asyncSearchResponse.getStartTimeMillis(), System.currentTimeMillis(),
                asyncSearchResponse.getSearchResponse(),
                asyncSearchResponse.getError());
        createDoc(persistenceService, newAsyncSearchResponse);
        CountDownLatch getLatch = new CountDownLatch(1);
        persistenceService.getResponse(newAsyncSearchResponse.getId(), ActionListener.wrap(r -> failure(getLatch),
                e -> assertRnf(getLatch, e)));
        getLatch.await();
    }

    private void assertRnf(CountDownLatch latch, Exception exception) {
        assert exception instanceof ResourceNotFoundException;
        latch.countDown();
    }

    private void failure(CountDownLatch latch) {
        latch.countDown();
        fail();
    }

    private void verifyResponse(AsyncSearchResponse asyncSearchResponse, CountDownLatch latch,
                                AsyncSearchPersistenceContext asyncSearchPersistenceContext) {
        assertEquals(asyncSearchPersistenceContext.getAsyncSearchId().toString(), (AsyncSearchId.parseAsyncId(asyncSearchResponse.getId()).toString()));
        assertEquals(asyncSearchPersistenceContext.getAsyncSearchResponse().getExpirationTimeMillis(), asyncSearchResponse.getExpirationTimeMillis());
        assertEquals(asyncSearchPersistenceContext.getSearchResponse(), asyncSearchResponse.getSearchResponse());
        latch.countDown();
    }

    private void createDoc(AsyncSearchPersistenceService persistenceService,
                           AsyncSearchResponse asyncSearchResponse) throws IOException, InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        persistenceService.createResponse(new AsyncSearchPersistenceContext(AsyncSearchId.parseAsyncId(asyncSearchResponse.getId()),
                new AsyncSearchPersistenceModel(asyncSearchResponse.getStartTimeMillis(), asyncSearchResponse.getExpirationTimeMillis(),
                        asyncSearchResponse.getSearchResponse())),
                ActionListener.wrap(r -> latch.countDown(), e -> failure(latch)));
        latch.await();
    }


    private AsyncSearchResponse getAsyncSearchResponse() throws InterruptedException {
        SearchRequest searchRequest = new SearchRequest().indices("index").source(new SearchSourceBuilder());
        SubmitAsyncSearchRequest request = new SubmitAsyncSearchRequest(searchRequest);
        request.waitForCompletionTimeout(new TimeValue(4, TimeUnit.SECONDS));
        request.keepOnCompletion(true);
        AsyncSearchResponse asyncSearchResponse = TestClientUtils.blockingSubmitAsyncSearch(client(), request);
        TestClientUtils.assertResponsePersistence(client(), asyncSearchResponse.getId());
        return asyncSearchResponse;
    }

    private BytesReference getBytesReferenceObject(AsyncSearchId asyncSearchId) throws IOException {
        return XContentHelper.toXContent(new AsyncSearchResponse(AsyncSearchId.buildAsyncId(asyncSearchId), false,
                        System.currentTimeMillis(), System.currentTimeMillis() + new TimeValue(10, TimeUnit.DAYS).getMillis(),
                        null, null),
                Requests.INDEX_CONTENT_TYPE,
                true);
    }

    private AsyncSearchId generateNewAsynSearchId(TransportService transportService) {
        AsyncSearchContextId asyncSearchContextId = new AsyncSearchContextId(UUIDs.base64UUID(), randomInt(100));
        return new AsyncSearchId(transportService.getLocalNode().getId(), 1,
                asyncSearchContextId);

    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        ThreadPool.terminate(threadPool, 1, TimeUnit.SECONDS);
    }

    @After
    public void clear() throws InterruptedException {
        client().admin().indices().prepareDelete(INDEX).setTimeout(new TimeValue(10, TimeUnit.SECONDS)).get();
        Thread.sleep(2000);
    }
}
