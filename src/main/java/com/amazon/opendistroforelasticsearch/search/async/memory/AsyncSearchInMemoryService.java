package com.amazon.opendistroforelasticsearch.search.async.memory;

import com.amazon.opendistroforelasticsearch.search.async.AbstractAsyncSearchContext;
import com.amazon.opendistroforelasticsearch.search.async.AsyncSearchContextId;
import com.amazon.opendistroforelasticsearch.search.async.stats.AsyncSearchStats;
import com.amazon.opendistroforelasticsearch.search.async.stats.AsyncSearchStatNames;
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
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.common.unit.TimeValue.timeValueMinutes;
import static org.elasticsearch.common.util.concurrent.ConcurrentCollections.newConcurrentMapLongWithAggressiveConcurrency;

/**
 *  Once the response have been persisted or otherwise ready to be expunged, the {@link AsyncSearchInMemoryService.Reaper} frees up the in-memory contexts maintained
 *  on the coordinator node
 */
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

    /**
     * Finds an active context if one is present
     * @param asyncSearchContextId
     * @return
     */
    public Optional<ActiveAsyncSearchContext> findActiveContext(AsyncSearchContextId asyncSearchContextId) {
        final ActiveAsyncSearchContext asyncSearchContext = getContext(asyncSearchContextId);
        return Optional.ofNullable(asyncSearchContext);
    }

    public ActiveAsyncSearchContext getContext(AsyncSearchContextId contextId) {
        final ActiveAsyncSearchContext context = activeContexts.get(contextId.getId());
        if (context == null) {
            return null;
        }
        if (context.getContextId().getContextId().equals(contextId.getContextId())) {
            return context;
        }
        return null;
    }

    /**
     * AsyncSearchStats are updated from here. hence to maintain stat correctness, we can't put new context in map from elsewhere.
     */
    public void putContext(AsyncSearchContextId asyncSearchContextId, ActiveAsyncSearchContext asyncSearchContext) {
        activeContexts.putIfAbsent(asyncSearchContextId.getId(), asyncSearchContext);
        asyncSearchStats.getStat(AsyncSearchStatNames.RUNNING_ASYNC_SEARCH_COUNT.getName()).increment();
    }

    /**
     * Updates the context if one exists.
     * @param asyncSearchContextId
     * @param expirationTimeMillis
     * @return
     */
    public boolean updateContext(AsyncSearchContextId asyncSearchContextId, long expirationTimeMillis) {
        ActiveAsyncSearchContext activeAsyncSearchContext = activeContexts.computeIfPresent(asyncSearchContextId.getId(), (k, v) -> {
            v.setExpirationMillis(expirationTimeMillis);
            return v;
        });
        asyncSearchStats.getStat(AsyncSearchStatNames.RUNNING_ASYNC_SEARCH_COUNT.getName()).increment();
        return Objects.nonNull(activeAsyncSearchContext);
    }

    /**
     * Returns a copy of all active contexts
     * @return
     */
    public Map<Long, ActiveAsyncSearchContext> getAllContexts() {
        return CollectionUtils.copyMap(activeContexts);
    }


    /**
     * Removes an ctive context
     * @param asyncSearchContextId
     */
    public void removeContext(AsyncSearchContextId asyncSearchContextId) {
        activeContexts.remove(asyncSearchContextId.getId());
        asyncSearchStats.getStat(AsyncSearchStatNames.RUNNING_ASYNC_SEARCH_COUNT.getName()).decrement();
    }

    /**
     * Frees the active context
     * @param asyncSearchContextId
     * @return
     */
    public boolean freeContext(AsyncSearchContextId asyncSearchContextId) {
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
            freeContext(context.getContextId());
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
                toReap.forEach(a -> freeContext(a.getContextId()));
            } catch (Exception e) {
                logger.error("Exception while reaping contexts");
            }
        }
    }
}
