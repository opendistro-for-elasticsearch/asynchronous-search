package com.amazon.opendistroforelasticsearch.search.async;

import java.util.concurrent.atomic.AtomicReference;

public class AsyncSearchStatus {

     public enum Stage {
          INIT,
          RUNNING,
          COMPLETED,
          FAILED,
          PERSISTED,
          EXPIRED,
          ABORTED
     }

     private String failure;
     private final AtomicReference<Stage> stage = new AtomicReference<>(null);

     public AsyncSearchStatus() {
          this.stage.compareAndSet(Stage.INIT, null);
     }

     public AsyncSearchStatus moveToRunning() {
          return null;
     }


}
