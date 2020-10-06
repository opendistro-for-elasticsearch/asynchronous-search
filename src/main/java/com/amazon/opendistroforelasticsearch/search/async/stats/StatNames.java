package com.amazon.opendistroforelasticsearch.search.async.stats;

import java.util.HashSet;
import java.util.Set;

/**
 * Enum containing names of all stats
 */
public enum StatNames {
    RUNNING_ASYNC_SEARCH_COUNT("running_async_search_count"),
    STORED_ASYNC_SEARCH_COUNT("stored_async_search_count");

    private String name;

    StatNames(String name) {
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

        for (StatNames statName : StatNames.values()) {
            names.add(statName.getName());
        }
        return names;
    }
}
