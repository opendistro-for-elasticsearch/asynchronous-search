package com.amazon.opendistroforelasticsearch.search.async.listener;

import org.elasticsearch.common.unit.TimeValue;

public interface ContextPersistedTimeoutListener {

    void onContextPersisted();

    void onTimeout(TimeValue timeout);

}
