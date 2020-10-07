package com.amazon.opendistroforelasticsearch.search.async.stats;

import java.util.HashSet;
import java.util.Set;

/**
 * Enum containing names of all async search stats
 */
public enum AsyncSearchStatNames {
    RUNNING_ASYNC_SEARCH_COUNT("running_async_search_count"),
    ABORTED_ASYNC_SEARCH_COUNT("aborted_async_search_count"),
    FAILED_ASYNC_SEARCH_COUNT("failed_async_search_count"),
    COMPLETED_ASYNC_SEARCH_COUNT("completed_async_search_count"),
    PERSISTED_ASYNC_SEARCH_COUNT("persisted_async_search_count");

    private String name;

    AsyncSearchStatNames(String name) {
        this.name = name;
    }

    /**
     * Get stat name
     *
     * @return name
     */
    public String getName() {
        return name;
    }

    /**
     * Get set of stat names
     *
     * @return set of stat names
     */
    public static Set<String> getNames() {
        Set<String> names = new HashSet<>();

        for (AsyncSearchStatNames statName : AsyncSearchStatNames.values()) {
            names.add(statName.getName());
        }
        return names;
    }
}
