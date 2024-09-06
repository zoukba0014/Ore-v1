mod cu_limits;
mod mine;
mod register;
mod send_and_confirm;

mod utils;
use lapin::{options::*, types::FieldTable, BasicProperties, Connection, ConnectionProperties};

use crate::cu_limits::HASH_QUEUE;
use crate::cu_limits::PROOF_QUEUE;
use crate::cu_limits::RECEVIER;
use crate::mine::FoundSolution;
use crate::mine::Posion;
use crate::mine::POSION;
use crate::utils::setup_logger;
use crate::utils::Env;
use log::*;
use serde_json;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::{
    commitment_config::CommitmentConfig,
    signature::{read_keypair_file, Keypair},
};
use std::collections::HashMap;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use tokio::fs;
use tokio::signal;
use tokio::sync::mpsc;

struct Miner {
    pub priority_fee: u64,
    pub rpc_client: Arc<RpcClient>,
    pub rpc_client_send: Arc<RpcClient>,
    pub client_inf: HashMap<Pubkey, String>,
}

#[tokio::main]
async fn main() {
    dotenv::dotenv().ok();

    setup_logger().expect("Failed to set up logger");
    info!("Producer Miner Start");
    let env = Env::new();

    let (tx, mut rx) = mpsc::channel::<Vec<FoundSolution>>(32);
    let conn = Connection::connect(&env.mq_link, ConnectionProperties::default())
        .await
        .unwrap();
    debug!("Connected to RabbitMQ");

    let channel = conn.create_channel().await.unwrap();
    debug!("Channel created");

    // 声明队列
    // let queue_name = "send_proof";
    let _queue = channel
        .queue_declare(
            PROOF_QUEUE,
            QueueDeclareOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();
    // let queue1_name = "send_succeed";
    let _queue = channel
        .queue_declare(
            HASH_QUEUE,
            QueueDeclareOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();

    let path = PathBuf::from(&env.key_file_path);
    let mut pubkey_map: HashMap<Pubkey, String> = HashMap::new();
    match fs::read_dir(path).await {
        Ok(dir) => {
            let mut entries = dir;
            while let Ok(Some(entry)) = entries.next_entry().await {
                let path = entry.path();
                if path.is_file()
                    && path.extension().and_then(std::ffi::OsStr::to_str) == Some("json")
                {
                    if let Ok(absolute_path) = fs::canonicalize(&path).await {
                        let file_name = path
                            .file_stem()
                            .and_then(std::ffi::OsStr::to_str)
                            .unwrap_or_default()
                            .to_string();

                        if let Ok(pubkey) = Pubkey::from_str(&file_name) {
                            pubkey_map.insert(pubkey, absolute_path.to_string_lossy().to_string());
                        }
                    }
                }
            }
            // 打印结果验证
            for (pubkey, file_path) in &pubkey_map {
                info!("pubkey: {}, private keypair {}", pubkey, file_path);
            }
        }
        Err(e) => {
            error!("cant not load key file, the error is {}", e);
        }
    }

    let rpc_client = RpcClient::new_with_commitment(env.info_rpc, CommitmentConfig::finalized());
    let rpc_client_send =
        RpcClient::new_with_commitment(env.send_rpc, CommitmentConfig::finalized());
    let miner = Arc::new(Miner::new(
        Arc::new(rpc_client),
        env.priority_fee,
        Arc::new(rpc_client_send),
        pubkey_map,
    ));

    let listen_minner = miner.clone();
    let send_channl = channel.clone();

    tokio::spawn(async move {
        listen_minner
            .get_mutiple_current_proof_from_contract(&send_channl)
            .await;
    });
    let moniter_pool_miner = miner.clone();

    tokio::spawn(async move {
        moniter_pool_miner.moniter_mutiple_reset_proch().await;
    });
    let consumer_minner = miner.clone();
    let consumer_channel = channel.clone();
    let consumer = consumer_channel
        .basic_consume(
            HASH_QUEUE,
            RECEVIER,
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();
    let consumner_clone = consumer.clone();
    // let send_channl_val = channel.clone();
    tokio::spawn(async move {
        consumer_minner
            .collect_all_the_solution(consumner_clone, tx)
            .await;
    });
    let sender_mine = miner.clone();
    tokio::spawn(async move {
        while let Some(send_task) = rx.recv().await {
            info!("recevied sender_task delever to send");
            sender_mine.send_mine_with_mutiple(send_task).await;
        }
    });
    warn!("Press Ctrl+C to exit...");
    signal::ctrl_c()
        .await
        .expect("Failed to listen for ctrl_c signal");

    info!("Sending poison pill...");
    let payload = Posion {
        msg_type: POSION.to_string(),
    };
    let poison_message = serde_json::to_string(&payload).unwrap();
    for _ in 1..=3 {
        consumer_channel
            .basic_publish(
                "",
                PROOF_QUEUE,
                BasicPublishOptions::default(),
                poison_message.as_bytes().to_vec(),
                BasicProperties::default(),
            )
            .await
            .expect("Failed to publish poison message");
    }
    tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;

    info!("Cleaning up...");
    channel
        .queue_delete(PROOF_QUEUE, QueueDeleteOptions::default())
        .await
        .expect("Failed to delete queue");
    channel
        .queue_delete(HASH_QUEUE, QueueDeleteOptions::default())
        .await
        .expect("Failed to delete queue1");

    info!("Cleanup complete, exiting.");
    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
}

impl Miner {
    pub fn new(
        rpc_client: Arc<RpcClient>,
        priority_fee: u64,
        rpc_client_send: Arc<RpcClient>,
        client_inf: HashMap<Pubkey, String>,
    ) -> Self {
        Self {
            rpc_client,
            // keypair_filepath,
            priority_fee,
            rpc_client_send,
            client_inf,
        }
    }


    pub fn get_signer(&self, pubkey: &Pubkey) -> Keypair {
        match self.client_inf.get(pubkey) {
            Some(filepath) => read_keypair_file(filepath).unwrap(),
            None => panic!("No keypair provided"),
        }
    }
}
