package com.fintechstudios;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class BoundedIncrementingRecordSource implements SourceFunction<Record>, CheckpointedFunction {
  private final long maxCount;
  private long count = 0L;
  private volatile boolean isRunning = true;

  private transient ListState<Long> checkpointedCount;

  public BoundedIncrementingRecordSource(long maxCount) {
    this.maxCount = maxCount;
  }

  public void run(SourceContext<Record> ctx) throws InterruptedException {
    while (isRunning && count <= maxCount) {
      // this synchronized block ensures that state checkpointing,
      // internal state updates and emission of elements are an atomic operation
      synchronized (ctx.getCheckpointLock()) {
        ctx.collect(new Record(Long.toString(count)));
        count++;
        Thread.sleep(10);
      }
    }
  }

  public void cancel() {
    isRunning = false;
  }

  public void initializeState(FunctionInitializationContext context) throws Exception {
    this.checkpointedCount = context
        .getOperatorStateStore()
        .getListState(new ListStateDescriptor<>("count", Long.class));

    if (context.isRestored()) {
      for (Long count : this.checkpointedCount.get()) {
        this.count = count;
      }
    }
  }

  public void snapshotState(FunctionSnapshotContext context) throws Exception {
    this.checkpointedCount.clear();
    this.checkpointedCount.add(count);
  }
}
