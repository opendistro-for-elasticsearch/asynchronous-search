package com.amazon.opendistroforelasticsearch.search.async;

import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.common.component.LifecycleListener;
import org.elasticsearch.index.shard.IndexingOperationListener;

public class ExpiredSearchCleanupService extends LifecycleListener implements IndexingOperationListener, ClusterStateListener {
    @Override
    public void clusterChanged(ClusterChangedEvent event) {

    }
}
