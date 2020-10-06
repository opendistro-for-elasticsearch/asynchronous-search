package com.amazon.opendistroforelasticsearch.search.async;

import com.amazon.opendistroforelasticsearch.search.async.persistence.AsyncSearchPersistenceModel;
import com.amazon.opendistroforelasticsearch.search.async.persistence.AsyncSearchPersistenceService;
import com.amazon.opendistroforelasticsearch.search.async.request.SubmitAsyncSearchRequest;
import com.amazon.opendistroforelasticsearch.search.async.response.AsyncSearchResponse;
import com.amazon.opendistroforelasticsearch.search.async.utils.TestClientUtils;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.unit.TimeValue;
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
        AsyncSearchId newAsyncSearchId = new AsyncSearchId(transportService.getLocalNode().getId(),
                asyncSearchContextId);
        String id = AsyncSearchId.buildAsyncId(newAsyncSearchId);
        AsyncSearchResponse newAsyncSearchResponse = new AsyncSearchResponse(id,
                asyncSearchResponse.isPartial(),
                asyncSearchResponse.isRunning(),
                asyncSearchResponse.getStartTimeMillis(), asyncSearchResponse.getExpirationTimeMillis(),
                asyncSearchResponse.getSearchResponse(),
                asyncSearchResponse.getError());
        createDoc(persistenceService, namedWriteableRegistry, newAsyncSearchResponse);

        CountDownLatch getLatch = new CountDownLatch(1);
        persistenceService.getResponse(newAsyncSearchResponse.getId(), ActionListener.wrap(
                (AsyncSearchPersistenceModel r) -> verifyResponse(newAsyncSearchResponse, getLatch, r), exception -> failure(getLatch)));
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
                (AsyncSearchPersistenceModel r) -> failure(getLatch1),
                exception -> {
                    assertRnf(getLatch, exception);
                }));
        getLatch.await();

    }

    public void testGetAndDeleteNonExistentId() throws InterruptedException {
        AsyncSearchPersistenceService persistenceService = getInstanceFromNode(AsyncSearchPersistenceService.class);
        TransportService transportService = getInstanceFromNode(TransportService.class);
        NamedWriteableRegistry namedWriteableRegistry = writableRegistry();
        AsyncSearchId asyncSearchId = generateNewAsynSearchId(transportService);
        AsyncSearchPersistenceModel model1 = new AsyncSearchPersistenceModel(namedWriteableRegistry, asyncSearchId,
                System.nanoTime() + 10000, "responseString");
        CountDownLatch createLatch = new CountDownLatch(1);
        persistenceService.createResponse(model1, ActionListener.wrap(
                response -> createLatch.countDown(), ex -> failure(createLatch)));
        createLatch.await();
        CountDownLatch latch = new CountDownLatch(2);
        //assert failure
        persistenceService.getResponse("id", ActionListener.wrap(
                (AsyncSearchPersistenceModel r) -> failure(latch),
                exception -> {
                    assertRnf(latch, exception);
                }));
        //assert failure
        persistenceService.deleteResponse("id", ActionListener.wrap(
                (r) -> failure(latch),
                exception -> {
                    assertRnf(latch, exception);
                }));
        latch.await();

    }

    public void testCreateConcurrentDocsWhenIndexNotExists() throws InterruptedException {
        AsyncSearchPersistenceService persistenceService = getInstanceFromNode(AsyncSearchPersistenceService.class);
        TransportService transportService = getInstanceFromNode(TransportService.class);
        NamedWriteableRegistry namedWriteableRegistry = writableRegistry();

        AsyncSearchId asyncSearchId1 = generateNewAsynSearchId(transportService);
        AsyncSearchId asyncSearchId2 = generateNewAsynSearchId(transportService);
        AsyncSearchPersistenceModel model1 = new AsyncSearchPersistenceModel(namedWriteableRegistry, asyncSearchId1,
                System.nanoTime() + 1000000000000L, "responseString");
        AsyncSearchPersistenceModel model2 = new AsyncSearchPersistenceModel(namedWriteableRegistry, asyncSearchId2,
                System.nanoTime() + 1000000000000L, "responseString");
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
                (AsyncSearchPersistenceModel r) -> getLatch1.countDown(), exception -> failure(getLatch1)));
        getLatch1.await();

        CountDownLatch getLatch2 = new CountDownLatch(1);
        persistenceService.getResponse(id2, ActionListener.wrap(
                (AsyncSearchPersistenceModel r) -> getLatch2.countDown(), exception -> failure(getLatch2)));
        getLatch2.await();
    }

    public void testUpdateExpiration() throws InterruptedException, IOException {
        AsyncSearchPersistenceService persistenceService = getInstanceFromNode(AsyncSearchPersistenceService.class);
        AsyncSearchResponse asyncSearchResponse = getAsyncSearchResponse();

        CountDownLatch updateLatch = new CountDownLatch(1);
        long newExpirationTime = System.nanoTime() +10000000000000000L;
        persistenceService.updateExpirationTime(asyncSearchResponse.getId(),
                newExpirationTime, ActionListener.wrap(r -> updateLatch.countDown(),
                        e -> failure(updateLatch)));
        updateLatch.await();

        CountDownLatch getLatch = new CountDownLatch(1);
        persistenceService.getResponse(asyncSearchResponse.getId(), ActionListener.wrap(r -> {
            AsyncSearchResponse asyncSearchResponse1 = r.getAsyncSearchResponse();
            assert asyncSearchResponse1.getExpirationTimeMillis() == TimeUnit.NANOSECONDS.toMillis(newExpirationTime);
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
                asyncSearchResponse.isPartial(),
                asyncSearchResponse.isRunning(),
                asyncSearchResponse.getStartTimeMillis(), System.nanoTime(),
                asyncSearchResponse.getSearchResponse(),
                asyncSearchResponse.getError());
        createDoc(persistenceService, namedWriteableRegistry, newAsyncSearchResponse);
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
                                AsyncSearchPersistenceModel asyncSearchPersistenceModel) {
        assert asyncSearchPersistenceModel.getAsyncSearchId().toString().equals(AsyncSearchId.parseAsyncId(asyncSearchResponse.getId()).toString());
        assert asyncSearchPersistenceModel.getAsyncSearchResponse().getExpirationTimeMillis()
                == asyncSearchResponse.getExpirationTimeMillis();
        latch.countDown();
    }

    private void createDoc(AsyncSearchPersistenceService persistenceService, NamedWriteableRegistry namedWriteableRegistry,
                           AsyncSearchResponse asyncSearchResponse) throws IOException, InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        persistenceService.createResponse(new AsyncSearchPersistenceModel(namedWriteableRegistry, asyncSearchResponse,
                        asyncSearchResponse.getExpirationTimeMillis()*1000000L),
                ActionListener.wrap(r -> {
                    latch.countDown();
                }, e -> {
                    failure(latch);
                }));
        latch.await();
    }


    private AsyncSearchResponse getAsyncSearchResponse() throws InterruptedException {
        SearchRequest searchRequest = new SearchRequest().indices("index").source(new SearchSourceBuilder());
        SubmitAsyncSearchRequest request = new SubmitAsyncSearchRequest(searchRequest);
        AsyncSearchResponse asyncSearchResponse = TestClientUtils.blockingSubmitAsyncSearch(client(), request);
        TestClientUtils.assertResponsePersistence(client(), asyncSearchResponse.getId());
        return asyncSearchResponse;
    }

    private AsyncSearchId generateNewAsynSearchId(TransportService transportService) {
        AsyncSearchContextId asyncSearchContextId = new AsyncSearchContextId(UUIDs.base64UUID(), randomInt(100));
        return new AsyncSearchId(transportService.getLocalNode().getId(),
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
