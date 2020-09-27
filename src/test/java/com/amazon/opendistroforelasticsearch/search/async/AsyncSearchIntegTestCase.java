package com.amazon.opendistroforelasticsearch.search.async;

import com.amazon.opendistroforelasticsearch.search.async.plugin.AsyncSearchPlugin;
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
    protected boolean ignoreExternalCluster() {
        return true;
    }

//    @Override
//    protected Collection<Class<? extends Plugin>> transportClientPlugins() {
//        return Collections.singletonList(AsyncSearchPlugin.class);
//    }

}
