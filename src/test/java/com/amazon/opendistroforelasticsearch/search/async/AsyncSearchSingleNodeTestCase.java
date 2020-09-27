package com.amazon.opendistroforelasticsearch.search.async;

import com.amazon.opendistroforelasticsearch.search.async.plugin.AsyncSearchPlugin;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.client.Client;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.util.Collection;
import java.util.Collections;
import java.util.function.BiFunction;


public abstract class AsyncSearchSingleNodeTestCase extends ESSingleNodeTestCase {
    static final String INDEX = ".async_search_response";

    @Override
    protected boolean resetNodeAfterTest() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Collections.singletonList(AsyncSearchPlugin.class);
    }

    @Override
    public void tearDown() throws Exception {
//        client().admin().indices().prepareDelete(INDEX).setTimeout(new TimeValue(10, TimeUnit.SECONDS)).get();
        Thread.sleep(1000);
        super.tearDown();
    }

    <C extends Client,Req,R> void assertRNF(BiFunction<C,Req,R> function, C client, Req req) {
        try {
            function.apply(client, req);
        } catch (ResourceNotFoundException e) {
            return;
        }
        fail("Should have entered catch block");
    }
}
