package org.apache.arrow.datafusion;

import java.util.concurrent.CompletableFuture;
import org.apache.arrow.memory.BufferAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Wrapper for DF ParquetExec */
public class ParquetExec extends AbstractProxy {

  private static final Logger logger = LoggerFactory.getLogger(ParquetExec.class);
  private final SessionContext context;

  public ParquetExec(SessionContext context, long pointer) {
    super(pointer);
    this.context = context;
  }

  static native void exec(
      String parquetPath, String field, int value, long runtime, ObjectResultCallback callback);

  /**
   * Executes the query on the given parquet file. Experimental and not fully functional.
   *
   * @param parquetPath Path to parquet file
   * @param field Name of the field to be queried and fetched
   * @param value Value of the field to query, currently it equates for the file
   * @param allocator Buffer allocator
   * @return result stream
   */
  public CompletableFuture<RecordBatchStream> execute(
      String parquetPath, String field, int value, BufferAllocator allocator) {
    CompletableFuture<RecordBatchStream> result = new CompletableFuture<>();
    long runtimePointer = context.getRuntime().getPointer();
    exec(
        parquetPath,
        field,
        value,
        runtimePointer,
        (errString, streamId) -> {
          if (ErrorUtil.containsError(errString)) {
            result.completeExceptionally(new RuntimeException(errString));
          } else {
            result.complete(new DefaultRecordBatchStream(context, streamId, allocator));
          }
        });
    return result;
  }

  @Override
  void doClose(long pointer) throws Exception {
    logger.info("Closing Parquet Executor");
  }
}
