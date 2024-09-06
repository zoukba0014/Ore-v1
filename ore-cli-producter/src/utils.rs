use anyhow::Result;
use cached::proc_macro::cached;
use fern::colors::*;
use log::LevelFilter;
use ore::{
    self,
    state::{Proof, Treasury},
    utils::AccountDeserialize,
    MINT_ADDRESS, PROOF, TREASURY_ADDRESS,
};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_program::{pubkey::Pubkey, sysvar};
use solana_sdk::clock::Clock;
use spl_associated_token_account::get_associated_token_address;

pub async fn get_treasury(client: &RpcClient) -> Treasury {
    let data = client
        .get_account_data(&TREASURY_ADDRESS)
        .await
        .expect("Failed to get treasury account");
    *Treasury::try_from_bytes(&data).expect("Failed to parse treasury account")
}

pub async fn get_proof(client: &RpcClient, authority: Pubkey) -> Proof {
    let proof_address = proof_pubkey(authority);
    let data = client
        .get_account_data(&proof_address)
        .await
        .expect("Failed to get miner account");
    *Proof::try_from_bytes(&data).expect("Failed to parse miner account")
}

pub async fn get_clock_account(client: &RpcClient) -> Clock {
    let data = client
        .get_account_data(&sysvar::clock::ID)
        .await
        .expect("Failed to get miner account");
    bincode::deserialize::<Clock>(&data).expect("Failed to deserialize clock")
}

#[cached]
pub fn proof_pubkey(authority: Pubkey) -> Pubkey {
    Pubkey::find_program_address(&[PROOF, authority.as_ref()], &ore::ID).0
}

#[cached]
pub fn treasury_tokens_pubkey() -> Pubkey {
    get_associated_token_address(&TREASURY_ADDRESS, &MINT_ADDRESS)
}
pub struct Env {
    pub mq_ip: String,
    pub mq_port: String,
    pub mq_username: String,
    pub mq_password: String,
    pub mq_link: String, // 添加 mq_link 字段
    pub key_file_path: String,
    pub info_rpc: String,
    pub send_rpc: String,
    pub priority_fee: u64,
}
impl Env {
    pub fn new() -> Self {
        let ip = get_env("MQ_IP");
        let port = get_env("MQ_PORT");
        let username = get_env("MQ_USERNAME");
        let password = get_env("MQ_PASSWORD");
        let key_file_path = get_env("KEY_FILE_PATH");
        let info_rpc = get_env("INFO_RPC");
        let send_rpc = get_env("SEND_RPC");
        let priority_fee: u64 = get_env("PRIORITY_FEE")
            .parse()
            .expect("Failed to parse PRIORITY_FEE as u64");
        let mq_link = format!(
            "amqp://{}:{}@{}:{}/%2f", // 注意格式化字符串的正确性
            username, password, ip, port
        );

        Env {
            mq_ip: ip,
            mq_port: port,
            mq_username: username,
            mq_password: password,
            mq_link,
            key_file_path,
            info_rpc,
            send_rpc,
            priority_fee,
        }
    }
}
pub fn get_env(key: &str) -> String {
    std::env::var(key).unwrap_or(String::from(""))
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
