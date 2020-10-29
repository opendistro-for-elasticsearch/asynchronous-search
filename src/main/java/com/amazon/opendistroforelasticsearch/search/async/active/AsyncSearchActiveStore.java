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
package com.amazon.opendistroforelasticsearch.search.async.active;

import static org.elasticsearch.common.util.concurrent.ConcurrentCollections.newConcurrentMapLongWithAggressiveConcurrency;

import java.util.Map;
import java.util.Optional;

import com.amazon.opendistroforelasticsearch.search.async.AsyncSearchContext;
import com.amazon.opendistroforelasticsearch.search.async.AsyncSearchContextId;
import com.amazon.opendistroforelasticsearch.search.async.AsyncSearchRejectedException;
import com.amazon.opendistroforelasticsearch.search.async.AsyncSearchStage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.concurrent.ConcurrentMapLong;


public class AsyncSearchActiveStore {

    private static Logger logger = LogManager.getLogger(AsyncSearchActiveStore.class);
    private volatile int maxRunningContext;

    public static final Setting<Integer> MAX_RUNNING_CONTEXT =
            Setting.intSetting("async_search.max_running_context", 100, 0, Setting.Property.Dynamic, Setting.Property.NodeScope);

    private final ConcurrentMapLong<AsyncSearchActiveContext> activeContexts = newConcurrentMapLongWithAggressiveConcurrency();


    public AsyncSearchActiveStore(ClusterService clusterService) {
        Settings settings = clusterService.getSettings();
        maxRunningContext = MAX_RUNNING_CONTEXT.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(MAX_RUNNING_CONTEXT, this::setMaxRunningContext);
    }

    private void setMaxRunningContext(int maxRunningContext) {
        this.maxRunningContext = maxRunningContext;
    }

    public void putContext(AsyncSearchContextId asyncSearchContextId, AsyncSearchActiveContext asyncSearchContext) {
        if (activeContexts.values().stream().filter(context -> context.isRunning()).distinct().count() > maxRunningContext) {
            throw new AsyncSearchRejectedException("Trying to create too many running contexts. Must be less than or equal to: [" +
                 maxRunningContext + "]. " + "This limit can be set by changing the [" + MAX_RUNNING_CONTEXT.getKey() + "] setting.", maxRunningContext);
        }
        activeContexts.put(asyncSearchContextId.getId(), asyncSearchContext);
    }

    public Optional<AsyncSearchActiveContext> getContext(AsyncSearchContextId contextId) {
        AsyncSearchActiveContext context = activeContexts.get(contextId.getId());
        if (context == null) {
            return Optional.empty();
        }
        if (context.getContextId().getContextId().equals(contextId.getContextId())) {
            return Optional.of(context);
        }
        return Optional.empty();
    }


    public Map<Long, AsyncSearchActiveContext> getAllContexts() {
        return CollectionUtils.copyMap(activeContexts);
    }

    public boolean freeContext(AsyncSearchContextId asyncSearchContextId) {
        AsyncSearchActiveContext asyncSearchContext = activeContexts.get(asyncSearchContextId.getId());
        if (asyncSearchContext != null) {
            logger.debug("Removing {} from context map", asyncSearchContextId);
            activeContexts.remove(asyncSearchContextId.getId());
            asyncSearchContext.advanceStage(AsyncSearchStage.DELETED);
            return true;
        }
        return false;
    }

    public void freeAllContexts() {
        for (final AsyncSearchContext context : activeContexts.values()) {
            freeContext(context.getContextId());
        }
    }
}
