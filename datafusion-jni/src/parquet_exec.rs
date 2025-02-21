use crate::util::set_object_result;
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::ScalarValue;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::datasource::physical_plan::{FileScanConfig, ParquetExec};
use datafusion::execution::TaskContext;
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
    let field: String = env.get_string(&field).expect("Couldn't get field as string!").into();
    let parquet_path = Path::new(&path);

    let schema = Arc::new(Schema::new(vec![
        Field::new(&field, DataType::Int64, true),
    ]));

    let file_scan_config = FileScanConfig::new(
        ObjectStoreUrl::local_filesystem(),
        schema,
    )
        .with_file(PartitionedFile::new(
            &path,
            fs::metadata(parquet_path).unwrap().len() as usize as u64,
        ));

    let column = Arc::new(Column::new(&field, 0));
    let literal = Arc::new(expressions::Literal::new(ScalarValue::Int64(Some(value as i64))));

    let predicate = Arc::new(expressions::BinaryExpr::new(
        column,
        Operator::Eq,
        literal,
    ));

    let parquet_executor = ParquetExec::builder(file_scan_config).with_predicate(predicate).build();
    let task_ctx = Arc::new(TaskContext::default());

    let runtime = unsafe { &mut *(runtime as *mut Runtime) };
    runtime.block_on(async {
        let stream_result = parquet_executor.with_pushdown_filters(true).execute(0, task_ctx);
        set_object_result(
            &mut env,
            callback,
            stream_result.map(|stream| Box::into_raw(Box::new(stream))),
        );
    });
}
