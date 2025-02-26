use arrow::array::Array;
use arrow::array::StructArray;
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::stream::TryStreamExt;
use jni::objects::{JClass, JObject};
use jni::sys::jlong;
use jni::JNIEnv;
use std::convert::Into;
use std::ptr::addr_of_mut;
use tokio::runtime::Runtime;

use crate::util::{set_object_result_error, set_object_result_ok};

#[no_mangle]
pub extern "system" fn Java_org_apache_arrow_datafusion_DefaultRecordBatchStream_next(
    mut env: JNIEnv,
    _class: JClass,
    runtime: jlong,
    stream: jlong,
    callback: JObject,
) {
    //let start = std::time::Instant::now();
    let runtime = unsafe { &mut *(runtime as *mut Runtime) };
    let stream = unsafe { &mut *(stream as *mut SendableRecordBatchStream) };
    runtime.block_on(async {
        //let fetch_start = std::time::Instant::now();
        let next = stream.try_next().await;
        //let fetch_time = fetch_start.elapsed();
        match next {
            Ok(Some(batch)) => {
                //let convert_start = std::time::Instant::now();
                // Convert to struct array for compatibility with FFI
                //println!("Num rows : {}", batch.num_rows());
                let struct_array: StructArray = batch.into();
                let array_data = struct_array.into_data();
                let mut ffi_array = FFI_ArrowArray::new(&array_data);
                //let convert_time = convert_start.elapsed();
                // ffi_array must remain alive until after the callback is called
               // let callback_start = std::time::Instant::now();
                set_object_result_ok(&mut env, callback, addr_of_mut!(ffi_array));
               // let callback_time = callback_start.elapsed();
                // println!("Fetch: {:?}, Convert: {:?}, Callback: {:?}",
                //          fetch_time, convert_time, callback_time);
            }
            Ok(None) => {
                set_object_result_ok(&mut env, callback, 0 as *mut FFI_ArrowSchema);
            }
            Err(err) => {
                set_object_result_error(&mut env, callback, &err);
            }
        }
        //println!("Total time: {:?}", start.elapsed());
    });
}

#[no_mangle]
pub extern "system" fn Java_org_apache_arrow_datafusion_DefaultRecordBatchStream_getSchema(
    mut env: JNIEnv,
    _class: JClass,
    stream: jlong,
    callback: JObject,
) {
    let stream = unsafe { &mut *(stream as *mut SendableRecordBatchStream) };
    let schema = stream.schema();
    let ffi_schema = FFI_ArrowSchema::try_from(&*schema);
    match ffi_schema {
        Ok(mut ffi_schema) => {
            // ffi_schema must remain alive until after the callback is called
            set_object_result_ok(&mut env, callback, addr_of_mut!(ffi_schema));
        }
        Err(err) => {
            set_object_result_error(&mut env, callback, &err);
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_org_apache_arrow_datafusion_DefaultRecordBatchStream_destroy(
    _env: JNIEnv,
    _class: JClass,
    pointer: jlong,
) {
    let _ = unsafe { Box::from_raw(pointer as *mut SendableRecordBatchStream) };
}
