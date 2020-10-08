package com.amazon.opendistroforelasticsearch.search.async.reaper;

import com.amazon.opendistroforelasticsearch.search.async.persistence.AsyncSearchPersistenceService;
import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.OriginSettingClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.persistent.PersistentTaskParams;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTasksExecutor;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.function.Predicate;

import static com.amazon.opendistroforelasticsearch.search.async.reaper.AsyncSearchReaperPersistentTaskExecutor.AsyncSearchReaperParams;

public class AsyncSearchReaperPersistentTaskExecutor extends PersistentTasksExecutor<AsyncSearchReaperParams> {

    public static final String NAME = "cluster:admin/persistent/async_search_reaper";

    private static final Logger logger = LogManager.getLogger(AsyncSearchReaperPersistentTaskExecutor.class);

    private final ClusterService clusterService;
    private final AsyncSearchPersistenceService asyncSearchPersistenceService;
    private final OriginSettingClient client;

    public AsyncSearchReaperPersistentTaskExecutor(ClusterService clusterService, Client client,
                                                   AsyncSearchPersistenceService asyncSearchPersistenceService) {
        super(NAME, ThreadPool.Names.GENERIC);
        this.clusterService = clusterService;
        this.client = new OriginSettingClient(client, "persistentTasks");
        this.asyncSearchPersistenceService = asyncSearchPersistenceService;
    }

    public static class AsyncSearchReaperParams implements PersistentTaskParams {
        public AsyncSearchReaperParams() {

        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public Version getMinimalSupportedVersion() {
            return Version.CURRENT;
        }

        @Override
        public void writeTo(StreamOutput out) {
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) {
            return builder;
        }

        public static AsyncSearchReaperParams fromXContent(XContentParser xContentParser, Object o) {
            return new AsyncSearchReaperParams();
        }
    }

    /**
     * @return least loaded data node
     */
    @Override
    protected DiscoveryNode selectLeastLoadedNode(ClusterState clusterState, Predicate<DiscoveryNode> selector) {
        long minLoad = Long.MAX_VALUE;
        DiscoveryNode minLoadedNode = null;
        PersistentTasksCustomMetadata persistentTasks = clusterState.getMetadata().custom(PersistentTasksCustomMetadata.TYPE);
        for (ObjectCursor<DiscoveryNode> nodeObjectCursor : clusterState.getNodes().getDataNodes().values()) {
            DiscoveryNode node = nodeObjectCursor.value;
            if (selector.test(node)) {
                if (persistentTasks == null) {
                    return node;
                }
                long numberOfTasks = persistentTasks.getNumberOfTasksOnNode(node.getId(), NAME);
                if (minLoad > numberOfTasks) {
                    minLoad = numberOfTasks;
                    minLoadedNode = node;
                }
            }
        }
        return minLoadedNode;
    }

    @Override
    protected void nodeOperation(AllocatedPersistentTask task,
                                 AsyncSearchReaperParams params, PersistentTaskState state) {
        asyncSearchPersistenceService.deleteExpiredResponses(new ActionListener<BulkByScrollResponse>() {
            @Override
            public void onResponse(BulkByScrollResponse bulkByScrollResponse) {
                logger.info("COMPLETING TASK");
                task.markAsCompleted();
            }

            @Override
            public void onFailure(Exception e) {
                task.markAsFailed(e);
            }
        });
    }
}

