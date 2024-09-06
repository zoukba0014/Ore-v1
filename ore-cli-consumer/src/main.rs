use fern::colors::{Color, ColoredLevelConfig};
use futures_util::stream::StreamExt;
use lapin::message::Delivery;
use lapin::options::BasicAckOptions;
use lapin::options::BasicNackOptions;
use lapin::options::BasicPublishOptions;
use lapin::{
    options::{BasicConsumeOptions, QueueDeclareOptions},
    types::FieldTable,
    Connection, ConnectionProperties,
};
use lapin::{publisher_confirm::Confirmation, BasicProperties, Result};
use log::LevelFilter;
use log::*;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use solana_sdk::keccak::hashv;
use solana_sdk::keccak::Hash;
use solana_sdk::pubkey::Pubkey;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::sync::mpsc::Sender;
use std::sync::Arc;
use std::sync::Mutex;
use std::thread;
use std::thread::JoinHandle;
use tokio::runtime::Runtime;
pub const PROOF_QUEUE: &str = "PROOF_QUEUE";
pub const HASH_QUEUE: &str = "HASH_QUEUE";
// use lapin::options::BasicQosOptions;

#[derive(Debug, Clone)]
pub struct Env {
    pub mq_ip: String,
    pub mq_port: String,
    pub mq_username: String,
    pub mq_password: String,
    pub mq_link: String, // 添加 mq_link 字段
    pub task_size: u16,
    pub miner_name: String,
    pub threads: u64,
}
// #[derive(Debug, Clone, Serialize, Deserialize)]
// pub struct HeartBreak {
//     pub msg_type: String,
// }
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewTask {
    #[serde(rename = "type")]
    pub msg_type: String,
    pub pubkey: [u8; 32],
    pub hash: [u8; 32],
    pub difficulty: [u8; 32],
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FoundSolution {
    pub pubkey: [u8; 32],
    pub hash: [u8; 32],
    pub non: u64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Posion {
    #[serde(rename = "type")]
    pub msg_type: String,
}
#[derive(Debug)]
pub struct TaskControl {
    stop_signal: Arc<AtomicBool>,
    thread_handle: thread::JoinHandle<()>,
}
const INITIAL_TASK: &str = "initial_task";
const UPDATE_TASK: &str = "update_task";
const POSION: &str = "posion";

fn main() {
    dotenv::dotenv().ok();

    setup_logger().expect("Failed to set up logger");

    info!("Consumer Miner Start");

    let env = Arc::new(Env::new());

    let env_clone_for_thread = Arc::clone(&env);
    // globel task controller
    let task_manager: Arc<Mutex<HashMap<Hash, TaskControl>>> = Arc::new(Mutex::new(HashMap::new()));
    // 创建一个新线程来监听队列
    let (tx, rx) = mpsc::channel::<Delivery>();
    let rt = Runtime::new().unwrap();
    let message_listener_handle: JoinHandle<()> = thread::spawn(move || {
        rt.block_on(async {
            if let Err(err) = listen_for_messages(tx, &env_clone_for_thread).await {
                error!("Error listening for messages: {:?}", err);
            }
        });
    });
    let env_clone = Arc::clone(&env);
    let task_manager = task_manager.clone();
    thread::spawn(move || {
        while let Ok(message) = rx.recv() {
            info!("std rx recevied message success");
            match serde_json::from_slice::<Value>(&message.data) {
                Ok(msg) => {
                    // 检查消息类型并调用相应的处理函数
                    match msg.get("type").and_then(|t| t.as_str()) {
                        Some(INITIAL_TASK) => {
                            if let Ok(new_task) = serde_json::from_value::<NewTask>(msg.clone()) {
                                initial_task(new_task, &task_manager, &env_clone);
                            }
                        }
                        Some(UPDATE_TASK) => {
                            if let Ok(new_task) = serde_json::from_value::<NewTask>(msg.clone()) {
                                update_task(new_task, &task_manager, &env_clone);
                            }
                        }
                        Some(POSION) => {
                            info!("Got posion shutdown all the hash task");
                            shutdown_all_tasks(&task_manager);
                        }
                        // Some("update") => {}
                        _ => warn!("Unknown message type"),
                    }
                }
                Err(e) => {
                    error!("Failed to parse message: {:?}", e);
                }
            }

            
        }
    });
    message_listener_handle.join().unwrap();
    info!("All threads have completed.");
}
fn update_task(
    task: NewTask,
    task_manager: &Arc<Mutex<HashMap<Hash, TaskControl>>>,
    env_clone: &Arc<Env>,
) {
    let pubkey_hash = Hash::new(&task.pubkey);
    info!("update new proof for {}", &pubkey_hash);
    let task_control_to_stop;
    // 解锁并尝试移除现有的任务控制器
    {
        let mut manager = task_manager.lock().unwrap();
        task_control_to_stop = manager.remove(&pubkey_hash);
    } // 通过限制 MutexGuard 的作用域，这里解锁 manager
    if let Some(task_control) = task_control_to_stop {
        task_control.stop_signal.store(true, Ordering::SeqCst);
        task_control.thread_handle.join().unwrap(); // 等待线程结束可能是一个长时间操作
        warn!("Worker for {} has been killed.", pubkey_hash);
    }
    let stop_signal = Arc::new(AtomicBool::new(false));
    let clone_env = Arc::clone(&env_clone);
    let thread_handle = thread::spawn({
        let stop_signal = stop_signal.clone();
        move || {
            let difficulty = Hash::new(&task.difficulty);
            let threads = clone_env.threads;
            let pubkey = Pubkey::new_from_array(task.pubkey);
            let hash = Hash::new(&task.hash);
            (0..threads).into_par_iter().for_each(|index| {
                if stop_signal.load(Ordering::SeqCst) {
                    return;
                }
                find_next_hash_par(
                    pubkey,
                    hash,
                    difficulty,
                    index,
                    threads,
                    stop_signal.clone(),
                    &clone_env,
                );
            });
        }
    });
    {
        let mut manager = task_manager.lock().unwrap();
        manager.insert(
            pubkey_hash,
            TaskControl {
                stop_signal,
                thread_handle,
            },
        );
    }
    info!("start new task for {:?}", pubkey_hash);
}
fn initial_task(
    task: NewTask,
    task_manager: &Arc<Mutex<HashMap<Hash, TaskControl>>>,
    env_clone: &Arc<Env>,
) {
    let pubkey_hash = Hash::new(&task.pubkey);
    info!("Innitializ proof for miner {}", &pubkey_hash);
    let stop_signal = Arc::new(AtomicBool::new(false));
    let clone_env = Arc::clone(&env_clone);
    let thread_handle = thread::spawn({
        let stop_signal = stop_signal.clone();
        move || {
            let difficulty = Hash::new(&task.difficulty);
            let threads = clone_env.threads;
            let pubkey = Pubkey::new_from_array(task.pubkey);
            let hash = Hash::new(&task.hash);
            (0..threads).into_par_iter().for_each(|index| {
                if stop_signal.load(Ordering::SeqCst) {
                    return;
                }
                find_next_hash_par(
                    pubkey,
                    hash,
                    difficulty,
                    index,
                    threads,
                    stop_signal.clone(),
                    &clone_env,
                );
            });
        }
    });
    {
        let mut manager = task_manager.lock().unwrap();
        manager.insert(
            pubkey_hash,
            TaskControl {
                stop_signal,
                thread_handle,
            },
        );
    }
    info!("start new task for {:?}", pubkey_hash);
}
async fn listen_for_messages(tx: Sender<Delivery>, env: &Env) -> Result<()> {
    let conn = Connection::connect(&env.mq_link, ConnectionProperties::default()).await?;

    let channel = conn.create_channel().await?;

    // channel
    //     .basic_qos(env.task_size.into(), BasicQosOptions::default())
    //     .await?;
    let queue_name = PROOF_QUEUE; // 更改为你的队列名称

    let _queue = channel
        .queue_declare(
            queue_name,
            QueueDeclareOptions::default(),
            FieldTable::default(),
        )
        .await?;

    let mut consumer = channel
        .basic_consume(
            queue_name,
            &env.miner_name, // 消费者标签，随意命名
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await?;
    let mut task_count = 0;
    info!("Waiting for messages. Press Ctrl+C to exit.");
    let mut miner_list: Vec<Hash> = vec![];
    while let Some(delivery) = consumer.next().await {
        match delivery {
            Ok((channel, delivery)) => {
                // let data = &delivery.data;
                match serde_json::from_slice::<Value>(&delivery.data) {
                    Ok(msg) => {
                        // 检查消息类型并调用相应的处理函数
                        match msg.get("type").and_then(|t| t.as_str()) {
                            Some(UPDATE_TASK) => {
                                if let Ok(new_task) = serde_json::from_value::<NewTask>(msg.clone())
                                {
                                    let miner_pubkey = Hash::new(&new_task.pubkey);
                                    if miner_list.contains(&miner_pubkey) {
                                        info!("Got new proof for miner {}", &miner_pubkey);
                                        if let Err(e) = tx.send(delivery.clone()) {
                                            error!("Failed to send delivery to processing thread: {:?}", e);
                                        }
                                        if let Err(ack_error) = channel
                                            .basic_ack(
                                                delivery.delivery_tag,
                                                BasicAckOptions::default(),
                                            )
                                            .await
                                        {
                                            error!("Error acknowledging message: {:?}", ack_error);
                                        }
                                    } else {
                                        info!("new proof for miner {} is not belong to this node, send back to the queue",&miner_pubkey);
                                        if let Err(ack_error) = channel
                                            .basic_nack(
                                                delivery.delivery_tag,
                                                BasicNackOptions {
                                                    requeue: true,
                                                    multiple: false,
                                                },
                                            )
                                            .await
                                        {
                                            error!("Error acknowledging message: {:?}", ack_error);
                                        }
                                    }
                                }
                            }

                            Some(INITIAL_TASK) => {
                                if &task_count < &env.task_size {
                                    if let Ok(new_task) =
                                        serde_json::from_value::<NewTask>(msg.clone())
                                    {
                                        let miner_pubkey = Hash::new(&new_task.pubkey);
                                        info!(
                                            "Initialize insert miner {} to miners list",
                                            &miner_pubkey
                                        );
                                        miner_list.push(miner_pubkey);
                                        if let Err(e) = tx.send(delivery.clone()) {
                                            error!(
                                            "Failed to send delivery to processing thread: {:?}",
                                            e
                                        );
                                        }
                                        if let Err(ack_error) = channel
                                            .basic_ack(
                                                delivery.delivery_tag,
                                                BasicAckOptions::default(),
                                            )
                                            .await
                                        {
                                            error!("Error acknowledging message: {:?}", ack_error);
                                        }
                                        task_count += 1;
                                    }
                                } else {
                                    info!("already got enough task for this node so push back to the queue");
                                    if let Err(ack_error) = channel
                                        .basic_nack(
                                            delivery.delivery_tag,
                                            BasicNackOptions {
                                                requeue: true,
                                                multiple: false,
                                            },
                                        )
                                        .await
                                    {
                                        error!("Error acknowledging message: {:?}", ack_error);
                                    }
                                }
                            }
                            Some(POSION) => {
                                if let Err(e) = tx.send(delivery.clone()) {
                                    error!("Failed to send delivery to processing thread: {:?}", e);
                                }
                                // warn!("get postion send back borad to every node");
                                // if let Err(ack_error) = channel
                                //     .basic_nack(
                                //         delivery.delivery_tag,
                                //         BasicNackOptions {
                                //             requeue: true,
                                //             multiple: false,
                                //         },
                                //     )
                                //     .await
                                // {
                                //     error!("Error acknowledging message: {:?}", ack_error);
                                // }
                            }

                            _ => {
                                if let Err(e) = tx.send(delivery.clone()) {
                                    error!("Failed to send delivery to processing thread: {:?}", e);
                                }

                                if let Err(ack_error) = channel
                                    .basic_ack(delivery.delivery_tag, BasicAckOptions::default())
                                    .await
                                {
                                    error!("Error acknowledging message: {:?}", ack_error);
                                }
                            }
                        }
                    }
                    Err(e) => {
                        error!("Failed to parse message: {:?}", e);
                        if let Err(ack_error) = channel
                            .basic_ack(delivery.delivery_tag, BasicAckOptions::default())
                            .await
                        {
                            error!("Error acknowledging message: {:?}", ack_error);
                        }
                    }
                }

                // if let Err(e) = tx.send(delivery.clone()) {
                //     error!("Failed to send delivery to processing thread: {:?}", e);
                // }

                // // Acknowledge message in any case
                // if let Err(ack_error) = channel
                //     .basic_ack(delivery.delivery_tag, BasicAckOptions::default())
                //     .await
                // {
                //     error!("Error acknowledging message: {:?}", ack_error);
                // }
            }
            Err(e) => {
                error!("Error caught in consumer: {:?}", e);
            }
        }
    }

    Ok(())
}
async fn publish_message(message: &str, env: &Env) -> Result<Confirmation> {
    let conn = Connection::connect(&env.mq_link, ConnectionProperties::default())
        .await
        .expect("Connection failure");

    let channel = conn
        .create_channel()
        .await
        .expect("Failed to create channel");
    let payload = message.as_bytes().to_vec();

    channel
        .basic_publish(
            "", // 默认的交换机
            HASH_QUEUE,
            BasicPublishOptions::default(),
            payload,
            BasicProperties::default(),
        )
        .await?
        .await
}
fn shutdown_all_tasks(task_manager: &Arc<Mutex<HashMap<Hash, TaskControl>>>) {
    let mut manager = task_manager.lock().unwrap();

    // drain() 移除所有元素，同时允许我们操作它们
    for (hash, task_control) in manager.drain() {
        warn!("start kill {}'s thread", &hash);
        task_control.stop_signal.store(true, Ordering::SeqCst);
        task_control.thread_handle.join().unwrap();
        warn!("kill {} thread successd", &hash);
    }
    info!("All tasks have been stopped and cleared.");
}
pub fn find_next_hash_par(
    pubkey: Pubkey,
    hash: Hash,
    difficulty: Hash,
    number: u64,
    threads: u64,
    stop_signal: Arc<AtomicBool>,
    env: &Env,
) {
    info!("workser_{}_{} start", &pubkey.to_string(), &number);

    let thread_nonce_range = u64::MAX.saturating_div(threads).saturating_mul(number);

    let mut next_hash: Hash;
    let mut nonce: u64 = thread_nonce_range;

    loop {
        next_hash = hashv(&[
            hash.to_bytes().as_slice(),
            pubkey.to_bytes().as_slice(),
            nonce.to_le_bytes().as_slice(),
        ]);
        if nonce % 10_000 == 0 {
            if stop_signal.load(Ordering::SeqCst) {
                info!("proof changed");
                break;
            }
        }

        if next_hash.le(&difficulty) {
            if stop_signal.load(Ordering::SeqCst) {
                info!("proof changed");
                break;
            }
            let solution = FoundSolution {
                pubkey: pubkey.to_bytes(),
                hash: next_hash.to_bytes(),
                non: nonce,
            };

            info!(
                "worker {} find solution: {:?}",
                &pubkey.to_string(),
                &next_hash.to_string()
            );
            let hash_json = serde_json::to_string(&solution).unwrap();
            let rt = Runtime::new().unwrap();
            rt.block_on(async {
                publish_message(&hash_json, &env)
                    .await
                    .expect("Failed to publish message");
            });
        }
        nonce += 1;
    }
    info!("workser_{}_{} shutdown", &pubkey.to_string(), &number);
}
pub fn setup_logger() -> Result<()> {
    let colors = ColoredLevelConfig {
        trace: Color::Cyan,
        debug: Color::Magenta,
        info: Color::Green,
        warn: Color::Red,
        error: Color::BrightRed,
        ..ColoredLevelConfig::new()
    };

    fern::Dispatch::new()
        .format(move |out, message, record| {
            out.finish(format_args!(
                "{}[{}] {}",
                chrono::Local::now().format("[%H:%M:%S]"),
                colors.color(record.level()),
                message
            ))
        })
        .chain(std::io::stdout())
        .level(log::LevelFilter::Info)
        .level_for("MINER COMSUMER", LevelFilter::Info)
        .apply()
        .unwrap();

    Ok(())
}
impl Env {
    pub fn new() -> Self {
        let ip = get_env("MQ_IP");
        let port = get_env("MQ_PORT");
        let username = get_env("MQ_USERNAME");
        let password = get_env("MQ_PASSWORD");
        let task_size = get_env("TASK_SIZE")
            .parse()
            .expect("Failed to parse TASK_SIZE as u16");
        let miner_name = get_env("MINER_NAME");
        let threads = get_env("THREADS")
            .parse()
            .expect("Failed to parse THREADS as u16");

        let mq_link = format!(
            "amqp://{}:{}@{}:{}/%2f", // 注意格式化字符串的正确性
            username, password, ip, port
        );

        Env {
            mq_ip: ip,
            mq_port: port,
            mq_username: username,
            mq_password: password,
            mq_link, // 初始化 mq_link
            task_size,
            miner_name,
            threads,
        }
    }
}
pub fn get_env(key: &str) -> String {
    std::env::var(key).unwrap_or(String::from(""))
}
