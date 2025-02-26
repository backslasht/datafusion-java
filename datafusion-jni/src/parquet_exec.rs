use crate::util::{set_object_result, set_object_result_error, set_object_result_ok};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::ScalarValue;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::datasource::physical_plan::{FileScanConfig};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::expressions;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_plan::ExecutionPlan;
use jni::objects::{JClass, JObject, JString};
use jni::sys::{jint, jlong};
use jni::JNIEnv;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use datafusion::datasource::physical_plan::parquet::ParquetExecBuilder;
use datafusion::error::DataFusionError;
use datafusion::physical_plan::filter::FilterExec;
use tokio::runtime::Runtime;

#[no_mangle]
pub extern "system" fn Java_org_apache_arrow_datafusion_ParquetExec_exec(
    mut env: JNIEnv,
    _class: JClass,
    path: JString,
    field: JString,
    value: jint,
    runtime: jlong,
    callback: JObject,
) {
    let path: String = env.get_string(&path).expect("Couldn't get path as string!").into();
    //let field: String = env.get_string(&field).expect("Couldn't get field as string!").into();
    let parquet_path = Path::new(&path);

    // TODO : HTTP logs schema - couldn't ingest @timestamp because of avro schema limitation
    let schema = Arc::new(Schema::new(vec![
        Field::new("timestamp", DataType::Int64, false),
        Field::new("clientip", DataType::Utf8, false),
        Field::new("request", DataType::Utf8, false),
        Field::new("status", DataType::Int32, false),
        Field::new("size", DataType::Int32, false),
    ]));


    // TODO : hardcoded
    let column = Arc::new(Column::new("status", 0)); // Adjust index based on schema
    let literal = Arc::new(expressions::Literal::new(ScalarValue::Int32(Some(value))));
    let predicate = Arc::new(expressions::BinaryExpr::new(
        column,
        Operator::Eq,
        literal,
    ));

    // TODO : hardcoded
    let files = vec![
        "/Users/gbh/Documents/httplogs/documents-181998.parquet",
        "/Users/gbh/Documents/httplogs/documents-191998.parquet",
        "/Users/gbh/Documents/httplogs/documents-201998.parquet",
        "/Users/gbh/Documents/httplogs/documents-211998.parquet",
        "/Users/gbh/Documents/httplogs/documents-221998.parquet",
        "/Users/gbh/Documents/httplogs/documents-231998.parquet",
        "/Users/gbh/Documents/httplogs/documents-241998.parquet",
    ];

    // Create a vector of PartitionedFile
    let partitioned_files: Vec<PartitionedFile> = files
        .iter()
        .map(|file_path| {
            let path = Path::new(file_path);
            PartitionedFile::new(
                path.to_str().unwrap().to_string(),
                fs::metadata(path).unwrap().len() as u64
            )
        })
        .collect();

    let file_scan_config = FileScanConfig::new(
        ObjectStoreUrl::local_filesystem(),
        schema.clone(),
    )
        .with_file_group(partitioned_files)
        .with_projection(Some(vec![3]));  // TODO : hardcoded

    let parquet_exec = ParquetExecBuilder::new(file_scan_config)
        .with_predicate(predicate.clone())
        .build();

    // Create FilterExec and handle the Result immediately
    let filter_exec = match FilterExec::try_new(
        predicate.clone(),
        Arc::new(parquet_exec.clone()),
    ) {
        Ok(filter) => Arc::new(filter),
        Err(e) => {
            let err_ptr: Result<*mut SendableRecordBatchStream, DataFusionError> = Err(e);
            set_object_result(
                &mut env,
                callback,
                err_ptr
            );
            return;
        }
    };

    let task_ctx = Arc::new(TaskContext::default());

    let runtime = unsafe { &mut *(runtime as *mut Runtime) };
    runtime.block_on(async {
        match filter_exec.execute(0, task_ctx) {
            Ok(stream) => {
                let boxed_stream = Box::new(stream);
                let stream_ptr = Box::into_raw(boxed_stream);
                set_object_result_ok(
                    &mut env,
                    callback,
                    stream_ptr
                );
            }
            Err(e) => {
                set_object_result_error(
                    &mut env,
                    callback,
                    &e
                );
            }
        }
    });

}

#[no_mangle]
pub extern "system" fn Java_org_apache_arrow_datafusion_ParquetExec_execRange(
    mut env: JNIEnv,
    _class: JClass,
    path: JString,
    field: JString,
    value1: jlong,
    value2: jlong,
    runtime: jlong,
    callback: JObject,
) {
    let path: String = env.get_string(&path).expect("Couldn't get path as string!").into();
    //let field: String = env.get_string(&field).expect("Couldn't get field as string!").into();
    let parquet_path = Path::new(&path);

    let schema = Arc::new(Schema::new(vec![
        Field::new("backend_ip", DataType::Utf8, false),
        Field::new("backend_port", DataType::Int32, false),
        Field::new("backend_processing_time", DataType::Float32, false),
        Field::new("backend_status_code", DataType::Int32, false),
        Field::new("client_ip", DataType::Utf8, false),
        Field::new("client_port", DataType::Int32, false),
        Field::new("connection_time", DataType::Float32, false),
        Field::new("destination_ip", DataType::Utf8, false),
        Field::new("destination_port", DataType::Int32, false),
        Field::new("elb_status_code", DataType::Int32, false),
        Field::new("http_port", DataType::Int32, false),
        Field::new("http_version", DataType::Utf8, false),
        Field::new("matched_rule_priority", DataType::Int32, false),
        Field::new("received_bytes", DataType::Int32, false),
        Field::new("request_creation_time", DataType::Int64, false),
        Field::new("request_processing_time", DataType::Float32, false),
        Field::new("response_processing_time", DataType::Float32, false),
        Field::new("sent_bytes", DataType::Int32, false),
        Field::new("target_ip", DataType::Utf8, false),
        Field::new("target_port", DataType::Int32, false),
        Field::new("target_processing_time", DataType::Float32, false),
        Field::new("target_status_code", DataType::Int32, false),
        Field::new("timestamp", DataType::Int64, false),
    ]));

    // TODO : hardcoded
    let column = Arc::new(Column::new("timestamp", 0)); // Adjust index based on schema
    let literal = Arc::new(expressions::Literal::new(ScalarValue::Int64(Some(value1))));
    let predicate1 = Arc::new(expressions::BinaryExpr::new(
        column,
        Operator::GtEq,
        literal,
    ));

    // TODO : hardcoded
    let column = Arc::new(Column::new("timestamp", 0)); // Adjust index based on schema
    let literal = Arc::new(expressions::Literal::new(ScalarValue::Int64(Some(value2))));
    let predicate2 = Arc::new(expressions::BinaryExpr::new(
        column,
        Operator::LtEq,
        literal,
    ));

    let predicate = Arc::new(expressions::BinaryExpr::new(
        predicate1,
        Operator::And,
        predicate2,
    ));

    let file_scan_config = FileScanConfig::new(
        ObjectStoreUrl::local_filesystem(),
        schema.clone(),
    ).with_file(PartitionedFile::new(
        parquet_path.to_str().unwrap().to_string(), // Convert Path to String
        fs::metadata(parquet_path).unwrap().len() as u64
    )).with_projection(Some(vec![22]));

    let parquet_exec = ParquetExecBuilder::new(file_scan_config)
        .with_predicate(predicate.clone())
        .build();

    // Create FilterExec and handle the Result immediately
    let filter_exec = match FilterExec::try_new(
        predicate.clone(),
        Arc::new(parquet_exec.clone()),
    ) {
        Ok(filter) => Arc::new(filter),
        Err(e) => {
            let err_ptr: Result<*mut SendableRecordBatchStream, DataFusionError> = Err(e);
            set_object_result(
                &mut env,
                callback,
                err_ptr
            );
            return;
        }
    };

    let task_ctx = Arc::new(TaskContext::default());

    let runtime = unsafe { &mut *(runtime as *mut Runtime) };
    runtime.block_on(async {
        match filter_exec.execute(0, task_ctx) {
            Ok(stream) => {
                let boxed_stream = Box::new(stream);
                let stream_ptr = Box::into_raw(boxed_stream);
                set_object_result_ok(
                    &mut env,
                    callback,
                    stream_ptr
                );
            }
            Err(e) => {
                set_object_result_error(
                    &mut env,
                    callback,
                    &e
                );
            }
        }
    });

}

#[no_mangle]
pub extern "system" fn Java_org_apache_arrow_datafusion_ParquetExec_execRangeBool(
    mut env: JNIEnv,
    _class: JClass,
    path: JString,
    field: JString,
    value1: jlong,
    value2: jlong,
    field1: JString,
    value3: jint,
    runtime: jlong,
    callback: JObject,
) {
    let path: String = env.get_string(&path).expect("Couldn't get path as string!").into();
    //let field: String = env.get_string(&field).expect("Couldn't get field as string!").into();
    let parquet_path = Path::new(&path);

    let schema = Arc::new(Schema::new(vec![
        Field::new("backend_ip", DataType::Utf8, false),
        Field::new("backend_port", DataType::Int32, false),
        Field::new("backend_processing_time", DataType::Float32, false),
        Field::new("backend_status_code", DataType::Int32, false),
        Field::new("client_ip", DataType::Utf8, false),
        Field::new("client_port", DataType::Int32, false),
        Field::new("connection_time", DataType::Float32, false),
        Field::new("destination_ip", DataType::Utf8, false),
        Field::new("destination_port", DataType::Int32, false),
        Field::new("elb_status_code", DataType::Int32, false),
        Field::new("http_port", DataType::Int32, false),
        Field::new("http_version", DataType::Utf8, false),
        Field::new("matched_rule_priority", DataType::Int32, false),
        Field::new("received_bytes", DataType::Int32, false),
        Field::new("request_creation_time", DataType::Int64, false),
        Field::new("request_processing_time", DataType::Float32, false),
        Field::new("response_processing_time", DataType::Float32, false),
        Field::new("sent_bytes", DataType::Int32, false),
        Field::new("target_ip", DataType::Utf8, false),
        Field::new("target_port", DataType::Int32, false),
        Field::new("target_processing_time", DataType::Float32, false),
        Field::new("target_status_code", DataType::Int32, false),
        Field::new("timestamp", DataType::Int64, false),
    ]));

    let column = Arc::new(Column::new("timestamp", 1)); // Adjust index based on schema
    let literal = Arc::new(expressions::Literal::new(ScalarValue::Int64(Some(value1))));
    let predicate1 = Arc::new(expressions::BinaryExpr::new(
        column,
        Operator::GtEq,
        literal,
    ));

    let column = Arc::new(Column::new("timestamp", 1)); // Adjust index based on schema
    let literal = Arc::new(expressions::Literal::new(ScalarValue::Int64(Some(value2))));
    let predicate2 = Arc::new(expressions::BinaryExpr::new(
        column,
        Operator::LtEq,
        literal,
    ));

    let column = Arc::new(Column::new("target_status_code", 0)); // Adjust index based on schema
    let literal = Arc::new(expressions::Literal::new(ScalarValue::Int32(Some(value3))));
    let predicate3 = Arc::new(expressions::BinaryExpr::new(
        column,
        Operator::Eq,
        literal,
    ));

    let predicate4 = Arc::new(expressions::BinaryExpr::new(
        predicate1,
        Operator::And,
        predicate2,
    ));

    let predicate = Arc::new(expressions::BinaryExpr::new(
        predicate4,
        Operator::And,
        predicate3,
    ));

    let file_scan_config = FileScanConfig::new(
        ObjectStoreUrl::local_filesystem(),
        schema.clone(),
    ).with_file(PartitionedFile::new(
        parquet_path.to_str().unwrap().to_string(), // Convert Path to String
        fs::metadata(parquet_path).unwrap().len() as u64
    )).with_projection(Some(vec![21,22]));

    let parquet_exec = ParquetExecBuilder::new(file_scan_config)
        .with_predicate(predicate.clone())
        .build();

    // Create FilterExec and handle the Result immediately
    let filter_exec = match FilterExec::try_new(
        predicate.clone(),
        Arc::new(parquet_exec.clone()),
    ) {
        Ok(filter) => Arc::new(filter),
        Err(e) => {
            let err_ptr: Result<*mut SendableRecordBatchStream, DataFusionError> = Err(e);
            set_object_result(
                &mut env,
                callback,
                err_ptr
            );
            return;
        }
    };

    let task_ctx = Arc::new(TaskContext::default());

    let runtime = unsafe { &mut *(runtime as *mut Runtime) };
    runtime.block_on(async {
        match filter_exec.execute(0, task_ctx) {
            Ok(stream) => {
                let boxed_stream = Box::new(stream);
                let stream_ptr = Box::into_raw(boxed_stream);
                set_object_result_ok(
                    &mut env,
                    callback,
                    stream_ptr
                );
            }
            Err(e) => {
                set_object_result_error(
                    &mut env,
                    callback,
                    &e
                );
            }
        }
    });

}

