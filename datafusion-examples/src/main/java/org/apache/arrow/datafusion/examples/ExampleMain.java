package org.apache.arrow.datafusion.examples;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.arrow.datafusion.DataFrame;
import org.apache.arrow.datafusion.ParquetExec;
import org.apache.arrow.datafusion.RecordBatchStream;
import org.apache.arrow.datafusion.SessionContext;
import org.apache.arrow.datafusion.SessionContexts;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExampleMain {

  private static final Logger logger = LoggerFactory.getLogger(ExampleMain.class);

  /**
   * public static void testParquetExec() throws Exception { try (SessionContext context =
   * SessionContexts.create(); BufferAllocator allocator = new RootAllocator()) { ParquetExec exec =
   * new ParquetExec(context, context.getPointer()); CompletableFuture<RecordBatchStream> result =
   * exec.execute( "/Users/gbh/Documents/1728892707_zstd_32mb_rg_v2.parquet", "target_status_code",
   * 404, allocator); RecordBatchStream stream = result.join(); VectorSchemaRoot root =
   * stream.getVectorSchemaRoot(); while (stream.loadNextBatch().get()) { List<FieldVector> vectors
   * = root.getFieldVectors(); for (FieldVector vector : vectors) { // logger.info( // "Field - {},
   * {}, {}, {}", // vector.getField().getName(), // vector.getField().getType(), //
   * vector.getValueCount(), // vector); IntVector v = (IntVector) vector; int sum = 0; for (int i =
   * 0; i < v.getValueCount(); i++) { sum += (int) v.get(i); } //logger.info("sum : {}", sum); } }
   * stream.close(); } }
   */
  public static void testParquetExec() throws Exception {
    try (SessionContext context = SessionContexts.create();
        BufferAllocator allocator = new RootAllocator()) {
      ParquetExec exec = new ParquetExec(context, context.getPointer());
      CompletableFuture<RecordBatchStream> result =
          exec.execute(
              "/Users/gbh/Documents/1728892707_zstd_32mb_rg_v2.parquet",
              "status",
              200,
              allocator);
      RecordBatchStream stream = result.join();
      VectorSchemaRoot root = stream.getVectorSchemaRoot();
      long s = System.currentTimeMillis();
      long sum = 0;
      while (stream.loadNextBatch().get()) {
        List<FieldVector> vectors = root.getFieldVectors();
        for (FieldVector vector : vectors) {
          // Log the vector type to debug
          //                  logger.info(
          //                          "Field - {}, Type: {}",
          //                          vector.getField().getName(),
          //                          vector.getField().getType()
          //                  );

          // Handle different vector types appropriately
          if (vector instanceof IntVector) {
            IntVector v = (IntVector) vector;

            for (int i = 0; i < v.getValueCount(); i++) {
              int a = v.get(i);
              //logger.info("val : {}" , a);
              sum += a;
            }
            // logger.info("sum (Int): {}", sum);
          } else if (vector instanceof VarCharVector) {
            VarCharVector v = (VarCharVector) vector;
            // Handle VarChar values appropriately
            for (int i = 0; i < v.getValueCount(); i++) {
              String value = new String(v.get(i));
              // Process string value
            }
          } else {
            logger.warn("Unexpected vector type: {}", vector.getClass().getName());
          }
        }
      }
      logger.info("sum : {}", sum);
      logger.info("stream took {} ms", System.currentTimeMillis() - s);
      stream.close();
    }
  }

  public static void testParquetExecRange() throws Exception {
    try (SessionContext context = SessionContexts.create();
         BufferAllocator allocator = new RootAllocator()) {
      ParquetExec exec = new ParquetExec(context, context.getPointer());
      CompletableFuture<RecordBatchStream> result =
              exec.execute(
                      "/Users/gbh/Documents/1728892707_zstd_32mb_rg_v2.parquet",
                      "timestamp",
                      1724860800,
                      1724904000,
                      allocator);
      RecordBatchStream stream = result.join();
      VectorSchemaRoot root = stream.getVectorSchemaRoot();
      long s = System.currentTimeMillis();
      while (stream.loadNextBatch().get()) {
        List<FieldVector> vectors = root.getFieldVectors();
        for (FieldVector vector : vectors) {
          // Log the vector type to debug
          //                  logger.info(
          //                          "Field - {}, Type: {}",
          //                          vector.getField().getName(),
          //                          vector.getField().getType()
          //                  );

          // Handle different vector types appropriately
          if (vector instanceof IntVector) {
            IntVector v = (IntVector) vector;
            int sum = 0;
            for (int i = 0; i < v.getValueCount(); i++) {
              sum += v.get(i);
            }
            // logger.info("sum (Int): {}", sum);
          } else if (vector instanceof VarCharVector) {
            VarCharVector v = (VarCharVector) vector;
            // Handle VarChar values appropriately
            for (int i = 0; i < v.getValueCount(); i++) {
              String value = new String(v.get(i));
              // Process string value
            }
          } else if (vector instanceof BigIntVector) {
            BigIntVector v = (BigIntVector) vector;
            long sum = 0;
            for (int i = 0; i < v.getValueCount(); i++) {
              sum += v.get(i);
            }
             logger.info("sum (Int): {}", sum);
          } else {
            logger.warn("Unexpected vector type: {}", vector.getClass().getName());
          }
        }
      }
      logger.info("stream took {} ms", System.currentTimeMillis() - s);
      stream.close();
    }
  }

  public static void testParquetExecRangeBoolean() throws Exception {
    try (SessionContext context = SessionContexts.create();
         BufferAllocator allocator = new RootAllocator()) {
      ParquetExec exec = new ParquetExec(context, context.getPointer());
      CompletableFuture<RecordBatchStream> result =
              exec.execute(
                      "/Users/gbh/Documents/1728892707_zstd_32mb_rg_v2.parquet",
                      "timestamp",
                      1724860800,
                      1724904000,
                      "target_status_code",
                      404,
                      allocator);
      RecordBatchStream stream = result.join();
      VectorSchemaRoot root = stream.getVectorSchemaRoot();
      long s = System.currentTimeMillis();
      long sum = 0;
      while (stream.loadNextBatch().get()) {
        List<FieldVector> vectors = root.getFieldVectors();
        for (FieldVector vector : vectors) {
          // Log the vector type to debug
          //                  logger.info(
          //                          "Field - {}, Type: {}",
          //                          vector.getField().getName(),
          //                          vector.getField().getType()
          //                  );

          // Handle different vector types appropriately
          if (vector instanceof IntVector) {
            IntVector v = (IntVector) vector;

            for (int i = 0; i < v.getValueCount(); i++) {
              sum += v.get(i);
            }
            break;
            // logger.info("sum (Int): {}", sum);
          } else if (vector instanceof VarCharVector) {
            VarCharVector v = (VarCharVector) vector;
            // Handle VarChar values appropriately
            for (int i = 0; i < v.getValueCount(); i++) {
              String value = new String(v.get(i));
              // Process string value
            }
          } else if (vector instanceof BigIntVector) {
            BigIntVector v = (BigIntVector) vector;
            for (int i = 0; i < v.getValueCount(); i++) {
              sum += v.get(i);
            }
            logger.info("sum (Int): {}", sum);
          } else {
            logger.warn("Unexpected vector type: {}", vector.getClass().getName());
          }
        }
      }
      logger.info("sum (Int): {}", sum);
      logger.info("stream took {} ms", System.currentTimeMillis() - s);
      stream.close();
    }
  }

  public static void testParquetSql() throws Exception {
    try (SessionContext context = SessionContexts.create();
         BufferAllocator allocator = new RootAllocator()) {

      // Register the parquet file as a table
      context.registerParquet(
              "alb_logs",
              Paths.get("/Users/gbh/Documents/1728892707_zstd_32mb_rg_v2.parquet")
      ).join();

      // Create and execute SQL query
      CompletableFuture<DataFrame> dfFuture = context.sql(
              "SELECT target_status_code, target_processing_time " +
                      "FROM alb_logs " +
                      "WHERE target_status_code = 404"
      );

      // Get record batch stream
      CompletableFuture<ArrowReader> readerFuture = dfFuture.thenComposeAsync(df ->
              df.collect(allocator)
      );

      ArrowReader reader = readerFuture.join();
      VectorSchemaRoot root = reader.getVectorSchemaRoot();

      long startTime = System.currentTimeMillis();
      int totalRows = 0;

      while (reader.loadNextBatch()) {
        List<FieldVector> vectors = root.getFieldVectors();
        totalRows += root.getRowCount();

        for (FieldVector vector : vectors) {
          if (vector instanceof IntVector) {
            IntVector v = (IntVector) vector;
            for (int i = 0; i < v.getValueCount(); i++) {
              // Process integer values
              int value = v.get(i);
              // logger.debug("Value: {}", value);
            }
          } else if (vector instanceof VarCharVector) {
            VarCharVector v = (VarCharVector) vector;
            for (int i = 0; i < v.getValueCount(); i++) {
              // Process string values
              String value = new String(v.get(i));
              // logger.debug("Value: {}", value);
            }
          }
        }
      }

      logger.info("Processed {} rows in {} ms",
              totalRows,
              System.currentTimeMillis() - startTime);

      reader.close();
    }
  }

  public static void runQueries() throws Exception {
    try (SessionContext context = SessionContexts.create();
         BufferAllocator allocator = new RootAllocator()) {

      context.registerParquet("logs", Paths.get("/Users/gbh/Documents/1728892707_zstd_32mb_rg_v2.parquet")).join();

      // 1. Match all (default)
      runQuery(context,
              "SELECT COUNT(*) FROM logs"
      );

      // 2. Term query for HTTP version
      runQuery(context,
              "SELECT COUNT(*) FROM logs " +
                      "WHERE http_version = 'HTTP/1.0'"
      );

      // 3. Multi-term filter
      runQuery(context,
              "SELECT COUNT(*) FROM logs " +
                      "WHERE http_version = 'HTTP/1.0' " +
                      "AND elb_status_code = 200"
      );

      // 4. Range query
      runQuery(context,
              "SELECT COUNT(*) FROM logs " +
                      "WHERE timestamp >= 1724860800 " +  // 2024-08-29 00:00:00 UTC
                      "AND timestamp < 1724947200"        // 2024-08-30 00:00:00 UTC
      );

      // 5. 200s in range
      runQuery(context,
              "SELECT COUNT(*) FROM logs " +
                      "WHERE timestamp >= 1724860800 " +
                      "AND timestamp < 1724947200 " +
                      "AND elb_status_code = 200"
      );

      // 6. 400s in range
      runQuery(context,
              "SELECT COUNT(*) FROM logs " +
                      "WHERE timestamp >= 1724860800 " +
                      "AND timestamp < 1724947200 " +
                      "AND elb_status_code = 400"
      );

      // 7. Hourly aggregation
      runQuery(context,
              "SELECT timestamp / 3600 * 3600 as hour, COUNT(*) " +
                      "FROM logs " +
                      "GROUP BY timestamp / 3600 * 3600 " +
                      "ORDER BY hour"
      );

      // 8. Multi-term aggregation
      runQuery(context,
              "SELECT client_ip, elb_status_code, request_processing_time, COUNT(*) " +
                      "FROM logs " +
                      "WHERE timestamp >= '2024-08-29T00:00:00+00:00' " +
                      "AND timestamp < '2024-08-30T00:00:00+00:00' " +
                      "GROUP BY client_ip, elb_status_code, request_processing_time"
      );

      // 9. Sort by processing time desc
      runQuery(context,
              "SELECT target_status_code FROM logs ORDER BY request_processing_time DESC LIMIT 1000"
      );

      // 10. Sort by processing time asc
      runQuery(context,
              "SELECT target_status_code FROM logs ORDER BY request_processing_time ASC LIMIT 1000"
      );

      // 11. Sort by timestamp desc
      runQuery(context,
              "SELECT * FROM logs ORDER BY timestamp DESC LIMIT 1000"
      );

      // 12. Sort by timestamp asc
      runQuery(context,
              "SELECT * FROM logs ORDER BY timestamp ASC LIMIT 1000"
      );

      runQuery(context,
              "SELECT * FROM logs LIMIT 1000"
      );

//      // 13. Sort with after timestamp desc
//      runQuery(context,
//              "SELECT target_status_code FROM logs " +
//                      "WHERE timestamp > 1724860800 "  // 2024-08-29 00:00:00 UT
//      );
//
//      // 14. Sort with after timestamp asc
//      runQuery(context,
//              "SELECT target_status_code as s FROM logs " +
//                      "WHERE timestamp > 1724904000 " // 2024-08-29 12:00:00 UTC //1724860800"
//      );
    }
  }

  public static void runHttpQueries1() throws Exception {
    try (SessionContext context = SessionContexts.create();
         BufferAllocator allocator = new RootAllocator()) {

      context.registerParquet("logs", Paths.get("/Users/gbh/Documents/httplogs/*.parquet")).join();

      // 1. Default (match_all)
      runQuery(context,
              "SELECT COUNT(*) FROM logs  LIMIT 10"
      );

      // 2. Term query
      runQuery(context,
              "SELECT COUNT(*) FROM logs " +
                      "WHERE request = 'GET / HTTP/1.0'  LIMIT 10"
      );

      // 3. Multi-term filter
      runQuery(context,
              "SELECT COUNT(*) FROM logs " +
                      "WHERE request = 'GET / HTTP/1.0' " +
                      "AND status = 200  LIMIT 10"
      );

      // 4. Range query
      // Note: 894240000 is epoch for '1998-05-15'
      runQuery(context,
              "SELECT COUNT(*) FROM logs " +
                      "WHERE timestamp >= 894240000 " +
                      "AND timestamp < " + System.currentTimeMillis()/1000 + " LIMIT 10"
      );

      // 5. 200s in range
      // 894240000 = '1998-05-01T00:00:00Z'
      // 894326400 = '1998-05-02T00:00:00Z'
      runQuery(context,
              "SELECT COUNT(*) FROM logs " +
                      "WHERE timestamp >= 894240000 " +
                      "AND timestamp < 894326400 " +
                      "AND status = 200 LIMIT 10"
      );

      // 6. 400s in range
      runQuery(context,
              "SELECT COUNT(*) FROM logs " +
                      "WHERE timestamp >= 894240000 " +
                      "AND timestamp < 894326400 " +
                      "AND status = 400 LIMIT 10"
      );

      // 7. Hourly aggregation
      runQuery(context,
              "SELECT (timestamp - (timestamp % 3600)) as hour, COUNT(*) " +
                      "FROM logs " +
                      "GROUP BY (timestamp - (timestamp % 3600)) " +
                      "ORDER BY hour"
      );

      // 8. Multi-term aggregation
      // 894499200 = '1998-05-03T00:00:00Z'
      // 894844800 = '1998-05-07T00:00:00Z'
      runQuery(context,
              "SELECT clientip, status, size, COUNT(*) " +
                      "FROM logs " +
                      "WHERE timestamp >= 894499200 " +
                      "AND timestamp < 894844800 " +
                      "GROUP BY clientip, status, size"
      );

      // 9. Scroll (paginated query)
      runQuery(context,
              "SELECT * FROM logs " +
                      "LIMIT 1000 OFFSET 0"  // Repeat with different OFFSET values for pagination
      );

      // 10. Sort by size descending
      runQuery(context,
              "SELECT * FROM logs " +
                      "ORDER BY size DESC LIMIT 10"
      );

      // 11. Sort by size ascending
      runQuery(context,
              "SELECT * FROM logs " +
                      "ORDER BY size ASC LIMIT 10"
      );

      // 12. Sort by timestamp descending
      runQuery(context,
              "SELECT * FROM logs " +
                      "ORDER BY timestamp DESC LIMIT 10"
      );

      // 13. Sort by timestamp desc with search_after
      // 897436800 = '1998-06-10'
      runQuery(context,
              "SELECT * FROM logs " +
                      "WHERE timestamp > 897436800 " +
                      "ORDER BY timestamp DESC LIMIT 10"
      );

      // 14. Sort by timestamp ascending
      runQuery(context,
              "SELECT * FROM logs " +
                      "ORDER BY timestamp ASC LIMIT 10"
      );

      // 15. Sort by timestamp asc with search_after
      runQuery(context,
              "SELECT * FROM logs " +
                      "WHERE timestamp > 897436800 " +
                      "ORDER BY timestamp ASC LIMIT 10"
      );
    }
  }

  public static void runHttpQueries() throws Exception {
    try (SessionContext context = SessionContexts.create();
         BufferAllocator allocator = new RootAllocator()) {

      context.registerParquet("logs", Paths.get("/Users/gbh/Documents/httplogs/*.parquet")).join();
      runQuery(context,
              "SELECT status FROM logs  LIMIT 10"
      );
      // 1. Default (match_all)
      runQuery(context,
              "SELECT status FROM logs  LIMIT 10"
      );

      // 2. Term query
      runQuery(context,
              "SELECT status FROM logs " +
                      "WHERE request = 'GET / HTTP/1.0'  LIMIT 10"
      );

      // 3. Multi-term filter
      runQuery(context,
              "SELECT status FROM logs " +
                      "WHERE request = 'GET / HTTP/1.0' " +
                      "AND status = 200  LIMIT 10"
      );

      // 4. Range query
      // Note: 894240000 is epoch for '1998-05-15'
      runQuery(context,
              "SELECT status FROM logs " +
                      "WHERE timestamp >= 894240000 " +
                      "AND timestamp < " + System.currentTimeMillis()/1000 + " LIMIT 10"
      );

      // 5. 200s in range
      // 894240000 = '1998-05-01T00:00:00Z'
      // 894326400 = '1998-05-02T00:00:00Z'
      runQuery(context,
              "SELECT status FROM logs " +
                      "WHERE timestamp >= 894240000 " +
                      "AND timestamp < 894326400 " +
                      "AND status = 200 LIMIT 10"
      );

      // 6. 400s in range
      runQuery(context,
              "SELECT status FROM logs " +
                      "WHERE timestamp >= 894240000 " +
                      "AND timestamp < 894326400 " +
                      "AND status = 400 LIMIT 10"
      );

      // 7. Hourly aggregation
      runQuery(context,
              "SELECT (timestamp - (timestamp % 3600)) as hour, COUNT(*) " +
                      "FROM logs " +
                      "GROUP BY (timestamp - (timestamp % 3600)) " +
                      "ORDER BY hour"
      );

      // 8. Multi-term aggregation
      // 894499200 = '1998-05-03T00:00:00Z'
      // 894844800 = '1998-05-07T00:00:00Z'
      runQuery(context,
              "SELECT clientip, status, size, COUNT(*) " +
                      "FROM logs " +
                      "WHERE timestamp >= 894499200 " +
                      "AND timestamp < 894844800 " +
                      "GROUP BY clientip, status, size"
      );

      // 9. Scroll (paginated query)
      runQuery(context,
              "SELECT status FROM logs " +
                      "LIMIT 1000 OFFSET 0"  // Repeat with different OFFSET values for pagination
      );

      // 10. Sort by size descending
      runQuery(context,
              "SELECT status FROM logs " +
                      "ORDER BY size DESC LIMIT 10"
      );

      // 11. Sort by size ascending
      runQuery(context,
              "SELECT status FROM logs " +
                      "ORDER BY size ASC LIMIT 10"
      );

      // 12. Sort by timestamp descending
      runQuery(context,
              "SELECT status FROM logs " +
                      "ORDER BY timestamp DESC LIMIT 10"
      );

      // 13. Sort by timestamp desc with search_after
      // 897436800 = '1998-06-10'
      runQuery(context,
              "SELECT status FROM logs " +
                      "WHERE timestamp > 897436800 " +
                      "ORDER BY timestamp DESC LIMIT 10"
      );

      // 14. Sort by timestamp ascending
      runQuery(context,
              "SELECT status FROM logs " +
                      "ORDER BY timestamp ASC LIMIT 10"
      );

      // 15. Sort by timestamp asc with search_after
      runQuery(context,
              "SELECT status FROM logs " +
                      "WHERE timestamp > 897436800 " +
                      "ORDER BY timestamp ASC LIMIT 10"
      );
    }
  }

//  private static void printBatch(VectorSchemaRoot root) {
//    StringBuilder sb = new StringBuilder();
//
//    // Print column names
//    for (Field field : root.getSchema().getFields()) {
//      sb.append(String.format("%-20s", field.getName()));
//    }
//    logger.info("\nColumns: \n{}", sb.toString());
//
//    // Print values
//    for (int row = 0; row < Math.min(root.getRowCount(), 5); row++) {
//      sb.setLength(0);
//      for (FieldVector vector : root.getFieldVectors()) {
//        String value = "null";
//        if (!vector.isNull(row)) {
//          if (vector instanceof VarCharVector) {
//            value = new String(((VarCharVector) vector).get(row));
//          } else {
//            value = vector.getObject(row).toString();
//          }
//        }
//        sb.append(String.format("%-20s", value));
//      }
//      logger.info(sb.toString());
//    }
//  }

  public static void runQueries1() throws Exception {
    try (SessionContext context = SessionContexts.create();
         BufferAllocator allocator = new RootAllocator()) {

      // Register the parquet file as a table
      context.registerParquet(
              "logs",
              Paths.get("/Users/gbh/Documents/1728892707_zstd_32mb_rg_v2.parquet")
      ).join();

      // 1. Match All (default query)
      runQuery(context, "SELECT COUNT(*) FROM logs");

      // 2. Term query
      runQuery(context,
              "SELECT COUNT(*) FROM logs " +
                      "WHERE http_version = 'HTTP/1.1'"
      );

      // 3. Multi-term filter
      runQuery(context,
              "SELECT COUNT(*) FROM logs " +
                      "WHERE http_version = 'HTTP/1.1' " +
                      "AND target_status_code = 200"
      );

      // 4. Range query with status code
      runQuery(context,
              "SELECT COUNT(*) FROM logs " +
                      "WHERE target_status_code = 200 " +
                      "AND timestamp BETWEEN '2024-08-29 20:59:58' AND '2024-08-29 23:59:58'"
      );

      // 5. Hourly aggregation
      runQuery(context,
              "SELECT date_trunc('hour', timestamp) as hour, COUNT(*) " +
                      "FROM logs " +
                      "GROUP BY date_trunc('hour', timestamp) " +
                      "ORDER BY hour"
      );

      // 6. Multi-term aggregation
      runQuery(context,
              "SELECT client_ip, target_status_code, COUNT(*) as count " +
                      "FROM logs " +
                      "WHERE timestamp BETWEEN '2024-08-29 20:59:58' AND '2024-08-29 23:59:58' " +
                      "GROUP BY client_ip, target_status_code " +
                      "ORDER BY request_processing_time DESC " +
                      "LIMIT 10"
      );

      // 7. Sorted queries
      runQuery(context,
              "SELECT sent_bytes FROM logs ORDER BY sent_bytes DESC LIMIT 10"
      );

      runQuery(context,
              "SELECT timestamp FROM logs ORDER BY timestamp DESC LIMIT 10"
      );
    }
  }

  private static void runQuery(SessionContext context, String sql) {
    try {
      long startTime = System.currentTimeMillis();

      CompletableFuture<DataFrame> dfFuture = context.sql(sql);
      CompletableFuture<ArrowReader> readerFuture = dfFuture.thenComposeAsync(df ->
              df.collect(new RootAllocator())
      );

      ArrowReader reader = readerFuture.join();
      VectorSchemaRoot root = reader.getVectorSchemaRoot();

      int totalRows = 0;
      int sum = 0;
      while (reader.loadNextBatch()) {
        totalRows += root.getRowCount();
        // sum
        for (int row = 0; row < root.getRowCount(); row++) {
          for (FieldVector vector : root.getFieldVectors()) {
            if (vector instanceof IntVector) {
              sum += ((IntVector) vector).get(row);
            }
          }
        }
        // Print first few rows of results
        if (totalRows <= 5) {
          printBatch(root);
          logger.info("Sum: {}", sum);
        }

      }

      long duration = System.currentTimeMillis() - startTime;
      logger.info("Query: {}", sql);
      logger.info("Total rows: {}, Duration: {} ms", totalRows, duration);
      logger.info("------------------------------------------------------");

      reader.close();
    } catch (Exception e) {
      logger.error("Error executing query: " + sql, e);
    }
  }

  private static void printBatch(VectorSchemaRoot root) {
    StringBuilder sb = new StringBuilder();

    for (Field field : root.getSchema().getFields()) {
      sb.append(field.getName()).append("\t");
    }
    logger.info(sb.toString());

    for (int row = 0; row < Math.min(root.getRowCount(), 5); row++) {
      sb.setLength(0);
      for (FieldVector vector : root.getFieldVectors()) {
        if (vector.isNull(row)) {
          sb.append("null\t");
        } else if (vector instanceof IntVector) {
          sb.append(((IntVector) vector).get(row)).append("\t");
        } else if (vector instanceof VarCharVector) {
          sb.append(new String(((VarCharVector) vector).get(row))).append("\t");
        } else {
          sb.append(vector.getObject(row)).append("\t");
        }
      }
      logger.info(sb.toString());
    }
  }

  public static void main(String[] args) throws Exception {
    // Initialize logging
    System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "ERROR");

    // Run queries
    try {
      runHttpQueries();
      //testParquetExec();
//      testParquetExec();
//      testParquetExec();
//      testParquetExec();
    } catch (Exception e) {
      logger.error("Error running queries", e);
    }
    logger.info("arrow.enable_unsafe_memory_access: {}",
            System.getProperty("arrow.enable_unsafe_memory_access"));
    logger.info("arrow.enable_null_check_for_get: {}",
            System.getProperty("arrow.enable_null_check_for_get"));
  }

  public static void main1(String[] args) throws Exception {
    try (SessionContext context = SessionContexts.create();
        BufferAllocator allocator = new RootAllocator()) {
      loadConstant(context).join();

      context.registerCsv("test_csv", Paths.get("src/main/resources/test_table.csv")).join();
      context.sql("select * from test_csv limit 3").thenComposeAsync(DataFrame::show).join();

      context
          .registerParquet(
              "test_parquet", Paths.get("src/main/resources/aggregate_test_100.parquet"))
          .join();
      context.sql("select * from test_parquet limit 5").thenComposeAsync(DataFrame::show).join();

      context
          .sql("select * from test_csv")
          .thenComposeAsync(df -> df.collect(allocator))
          .thenAccept(ExampleMain::consumeReader)
          .join();

      Path tempPath = Files.createTempDirectory("datafusion-examples");

      context
          .sql("select * from test_parquet limit 3")
          .thenComposeAsync(df -> df.writeCsv(tempPath.resolve("csv-out")))
          .join();

      context
          .sql("select * from test_parquet limit 3")
          .thenComposeAsync(df -> df.writeParquet(tempPath.resolve("parquet-out")))
          .join();

      context
          .sql("select * from test_parquet limit 3")
          .thenAccept(
              df -> {
                try {
                  boolean previouslyRegistered =
                      context.registerTable("test_parquet_limited", df.intoView()).isPresent();
                  assert !previouslyRegistered;
                } catch (Exception e) {
                  throw new RuntimeException(e);
                }
              })
          .join();

      context.sql("select * from test_parquet_limited").thenComposeAsync(DataFrame::show).join();
    }
    long st = System.currentTimeMillis();
    testParquetExec();
    logger.info("testParquetExec took {} ms", System.currentTimeMillis() - st);
    for (int i = 0; i < 100; i++) {
      st = System.currentTimeMillis();
      testParquetExec();
      logger.info("testParquetExec took {} ms", System.currentTimeMillis() - st);
    }
  }

  private static void consumeReader(ArrowReader reader) {
    try {
      VectorSchemaRoot root = reader.getVectorSchemaRoot();
      while (reader.loadNextBatch()) {
        VarCharVector nameVector = (VarCharVector) root.getVector(0);
        logger.info(
            "name vector size {}, row count {}, value={}",
            nameVector.getValueCount(),
            root.getRowCount(),
            nameVector);
        BigIntVector ageVector = (BigIntVector) root.getVector(1);
        logger.info(
            "age vector size {}, row count {}, value={}",
            ageVector.getValueCount(),
            root.getRowCount(),
            ageVector);
      }
      reader.close();
    } catch (IOException e) {
      logger.warn("got IO Exception", e);
    }
  }

  private static CompletableFuture<Void> loadConstant(SessionContext context) {
    return context
        .sql("select 1 + 2")
        .thenComposeAsync(
            dataFrame -> {
              logger.info("successfully loaded data frame {}", dataFrame);
              return dataFrame.show();
            });
  }
}
