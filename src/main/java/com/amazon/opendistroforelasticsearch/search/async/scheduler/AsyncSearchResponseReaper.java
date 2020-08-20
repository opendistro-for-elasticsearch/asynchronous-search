/*
package com.amazon.opendistroforelasticsearch.search.async.scheduler;

import com.amazon.opendistroforelasticsearch.jobscheduler.spi.JobExecutionContext;
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.ScheduledJobParameter;
import com.amazon.opendistroforelasticsearch.jobscheduler.spi.ScheduledJobRunner;

public class AsyncSearchResponseReaper implements ScheduledJobRunner {

    @Override
    public void runJob(ScheduledJobParameter job, JobExecutionContext context) {
        if(!(jobParameter instanceof AsyncSearchResponseReaper)) {
            throw new IllegalStateException("Job parameter is not instance of SampleJobParameter, type: "
                    + jobParameter.getClass().getCanonicalName());
        }

        if(this.clusterService == null) {
            throw new IllegalStateException("ClusterService is not initialized.");
        }

        if (this.threadPool == null) {
            throw new IllegalStateException("ThreadPool is not initialized.");
        }

        final LockService lockService = context.getLockService();

        Runnable runnable = () -> {
            if (jobParameter.getLockDurationSeconds() != null) {
                lockService.acquireLock(jobParameter, context, ActionListener.wrap(
                        lock -> {
                            if (lock == null) {
                                return;
                            }

                            SampleJobParameter parameter = (SampleJobParameter) jobParameter;
                            StringBuilder msg = new StringBuilder();
                            msg.append("Watching index ").append(parameter.getIndexToWatch()).append("\n");

                            List<ShardRouting> shardRoutingList = this.clusterService.state()
                                    .routingTable().allShards(parameter.getIndexToWatch());
                            for(ShardRouting shardRouting : shardRoutingList) {
                                msg.append(shardRouting.shardId().getId()).append("\t").append(shardRouting.currentNodeId()).append("\t")
                                        .append(shardRouting.active() ? "active" : "inactive").append("\n");
                            }
                            log.info(msg.toString());

                            lockService.release(lock, ActionListener.wrap(
                                    released -> {
                                        log.info("Released lock for job {}", jobParameter.getName());
                                    },
                                    exception -> {
                                        throw new IllegalStateException("Failed to release lock.");
                                    }
                            ));
                        },
                        exception -> {
                            throw new IllegalStateException("Failed to acquire lock.");
                        }
                ));
            }
        };

        threadPool.generic().submit(runnable);
    }
    }
}
*/
