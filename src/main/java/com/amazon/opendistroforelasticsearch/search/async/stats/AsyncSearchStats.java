package com.amazon.opendistroforelasticsearch.search.async.stats;

import java.util.HashMap;
import java.util.Map;

/**
 * Class represents all stats the plugin keeps track of
 */
public class AsyncSearchStats {

    private Map<String, AsyncSearchStat<?>> asyncSearchStats;

    /**
     * Constructor
     *
     * @param asyncSearchStats Map that maps name of stat to AsyncSearchStat object
     */
    public AsyncSearchStats(Map<String, AsyncSearchStat<?>> asyncSearchStats) {
        this.asyncSearchStats = asyncSearchStats;
    }

    /**
     * Get the stats
     *
     * @return all of the stats
     */
    public Map<String, AsyncSearchStat<?>> getStats() {
        return asyncSearchStats;
    }

    /**
     * Get individual stat by stat name
     *
     * @param key Name of stat
     * @throws IllegalArgumentException thrown on illegal statName
     */
    public AsyncSearchStat<?> getStat(String key) throws IllegalArgumentException {
        if (!asyncSearchStats.keySet().contains(key)) {
            throw new IllegalArgumentException("Stat=\"" + key + "\" does not exist");
        }
        return asyncSearchStats.get(key);
    }

    /**
     * Get a map of the stats that are kept at the node level
     *
     * @return Map of stats kept at the node level
     */
    public Map<String, AsyncSearchStat<?>> getNodeStats() {
        return getClusterOrNodeStats(false);
    }

    /**
     * Get a map of the stats that are kept at the cluster level
     *
     * @return Map of stats kept at the cluster level
     */
    public Map<String, AsyncSearchStat<?>> getClusterStats() {
        return getClusterOrNodeStats(true);
    }

    private Map<String, AsyncSearchStat<?>> getClusterOrNodeStats(Boolean getClusterStats) {
        Map<String, AsyncSearchStat<?>> statsMap = new HashMap<>();

        for (Map.Entry<String, AsyncSearchStat<?>> entry : asyncSearchStats.entrySet()) {
            if (entry.getValue().isClusterLevel() == getClusterStats) {
                statsMap.put(entry.getKey(), entry.getValue());
            }
        }
        return statsMap;
    }
}
