package com.amazon.opendistroforelasticsearch.search.async.request;

import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class AsyncSearchStatsRequest extends BaseNodesRequest<AsyncSearchStatsRequest> {

    public static final String ALL_STATS_KEY = "_all";

    private Set<String> validStats;
    private Set<String> statsToBeRetrieved;

    /**
     * Empty constructor needed for AsyncSearchStatsTransportAction
     */
    public AsyncSearchStatsRequest() {
        super((String[]) null);
    }

    /**
     * Constructor
     *
     * @param in input stream
     * @throws IOException in case of I/O errors
     */
    public AsyncSearchStatsRequest(StreamInput in) throws IOException {
        super(in);
        validStats = in.readSet(StreamInput::readString);
        statsToBeRetrieved = in.readSet(StreamInput::readString);
    }

    public AsyncSearchStatsRequest(Set<String> validStats, String... nodeIds) {
        super(nodeIds);
        this.validStats = validStats;
        statsToBeRetrieved = new HashSet<>();
    }

    /**
     * Add all stats to be retrieved
     */
    public void all() {
        statsToBeRetrieved.addAll(validStats);
    }

    /**
     * Remove all stats from retrieval set
     */
    public void clear() {
        statsToBeRetrieved.clear();
    }

    /**
     * Sets a stats retrieval status to true if it is a valid stat
     * @param stat stat name
     * @return true if the stats's retrieval status is successfully update; false otherwise
     */
    public boolean addStat(String stat) {
        if (validStats.contains(stat)) {
            statsToBeRetrieved.add(stat);
            return true;
        }
        return false;
    }

    /**
     * Get the set that tracks which stats should be retrieved
     *
     * @return the set that contains the stat names marked for retrieval
     */
    public Set<String> getStatsToBeRetrieved() {
        return statsToBeRetrieved;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringCollection(validStats);
        out.writeStringCollection(statsToBeRetrieved);
    }

}
