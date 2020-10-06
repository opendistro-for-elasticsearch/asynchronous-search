package com.amazon.opendistroforelasticsearch.search.async.memory;

import com.amazon.opendistroforelasticsearch.search.async.AbstractAsyncSearchContext;
import com.amazon.opendistroforelasticsearch.search.async.AsyncSearchContextId;
import com.amazon.opendistroforelasticsearch.search.async.stats.AsyncSearchStats;
import com.amazon.opendistroforelasticsearch.search.async.stats.StatNames;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.concurrent.ConcurrentMapLong;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.common.unit.TimeValue.timeValueMinutes;
import static org.elasticsearch.common.util.concurrent.ConcurrentCollections.newConcurrentMapLongWithAggressiveConcurrency;

public class AsyncSearchInMemoryService extends AbstractLifecycleComponent {

    private static Logger logger = LogManager.getLogger(AsyncSearchInMemoryService.class);

    private final Scheduler.Cancellable keepAliveReaper;
    private final ThreadPool threadPool;
    private final ClusterService clusterService;
    private final AsyncSearchStats asyncSearchStats;

    public static final Setting<TimeValue> KEEPALIVE_INTERVAL_SETTING =
            Setting.positiveTimeSetting("async_search.keep_alive_interval", timeValueMinutes(1), Setting.Property.NodeScope);

    private final ConcurrentMapLong<ActiveAsyncSearchContext> activeContexts = newConcurrentMapLongWithAggressiveConcurrency();

    public AsyncSearchInMemoryService(ThreadPool threadPool, ClusterService clusterService, AsyncSearchStats asyncSearchStats) {
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        Settings settings = clusterService.getSettings();
        this.keepAliveReaper = threadPool.scheduleWithFixedDelay(new Reaper(), KEEPALIVE_INTERVAL_SETTING.get(settings), ThreadPool.Names.GENERIC);
        this.asyncSearchStats = asyncSearchStats;
    }

    public Optional<ActiveAsyncSearchContext> findActiveContext(AsyncSearchContextId asyncSearchContextId) {
        final ActiveAsyncSearchContext asyncSearchContext = getContext(asyncSearchContextId);
        return Optional.ofNullable(asyncSearchContext);
    }

    public ActiveAsyncSearchContext getContext(AsyncSearchContextId contextId) {
        final ActiveAsyncSearchContext context = activeContexts.get(contextId.getId());
        if (context == null) {
            return null;
        }
        if (context.getAsyncSearchContextId().getContextId().equals(contextId.getContextId())) {
            return context;
        }
        return null;
    }

    /**
     * AsyncSearchStats are updated from here. hence to maintain stat correctness, we can't put new context in map from elsewhere.
     */
    public void putContext(AsyncSearchContextId asyncSearchContextId, ActiveAsyncSearchContext asyncSearchContext) {
        activeContexts.put(asyncSearchContextId.getId(), asyncSearchContext);
        asyncSearchStats.getStat(StatNames.RUNNING_ASYNC_SEARCH_COUNT.getName()).increment();
    }

    public Map<Long, ActiveAsyncSearchContext> getAllContexts() {
        return CollectionUtils.copyMap(activeContexts);
    }


    public void removeContext(AsyncSearchContextId asyncSearchContextId) {
        activeContexts.remove(asyncSearchContextId.getId());
        asyncSearchStats.getStat(StatNames.RUNNING_ASYNC_SEARCH_COUNT.getName()).decrement();
    }

    public boolean freeCachedContext(AsyncSearchContextId asyncSearchContextId) {
        AbstractAsyncSearchContext abstractAsyncSearchContext = activeContexts.get(asyncSearchContextId.getId());
        if (abstractAsyncSearchContext != null) {
            logger.debug("Removing {} from context map", asyncSearchContextId);
            activeContexts.remove(asyncSearchContextId.getId());
            return true;
        }
        return false;
    }

    @Override
    protected void doStart() {
    }

    @Override
    protected void doStop() {
        for (final AbstractAsyncSearchContext context : activeContexts.values()) {
            freeCachedContext(context.getAsyncSearchContextId());
        }
    }

    @Override
    protected void doClose() {
        doStop();
        keepAliveReaper.cancel();
    }

    /***
     * Reaps the contexts ready to be expunged
     */
    class Reaper implements Runnable {

        @Override
        public void run() {
            try {
                Set<ActiveAsyncSearchContext> toReap = activeContexts.values().stream()
                        .filter(a -> a.getStage().equals(ActiveAsyncSearchContext.Stage.ABORTED) || a.getStage().equals(ActiveAsyncSearchContext.Stage.PERSISTED))
                        .collect(Collectors.toSet());
                toReap.forEach(a -> freeCachedContext(a.getAsyncSearchContextId()));
            } catch (Exception e) {
                logger.error("Exception while reaping contexts");
            }
        }
    }
}
