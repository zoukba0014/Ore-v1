use crate::Miner;
use crate::{
    cu_limits::CU_LIMIT_MINE,
    utils::{get_proof, get_treasury},
};

use crate::utils::get_clock_account;
use crate::PROOF_QUEUE;
use anyhow::anyhow;
use anyhow::Result;
use futures::StreamExt;
use lapin::options::BasicAckOptions;
use lapin::options::BasicPublishOptions;
use lapin::BasicProperties;
use lapin::Channel;
use log::info;
use log::{error, warn};
use ore::state::Hash;
use ore::utils::AccountDeserialize;
use ore::{self, state::Bus, BUS_ADDRESSES};
use serde::Deserialize;
use serde::Serialize;
use solana_program::keccak::hashv;
use solana_program::keccak::HASH_BYTES;
// use solana_sdk::instruction::Instruction;
use solana_sdk::program_memory::sol_memcmp;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Keypair;
use solana_sdk::{compute_budget::ComputeBudgetInstruction, keccak::Hash as KeccakHash};
use std::collections::HashMap;
use std::collections::VecDeque;
use tokio::sync::mpsc::Sender;
use tokio::time::sleep;
use tokio::time::Duration;
// Odds of being selected to submit a reset tx
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
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeartBreak {
    pub msg_type: String,
}
pub const INITIAL_TASK: &str = "initial_task";
pub const UPDATE_TASK: &str = "update_task";
pub const POSION: &str = "posion";

impl Miner {
    pub async fn send_mine_with_mutiple(&self, solutions: Vec<FoundSolution>) {
        let mut instructions = Vec::new();
        let mut signers: Vec<Keypair> = Vec::new();
        let bus_id = self.find_bus_id().await;
        let cu_limit_ix = ComputeBudgetInstruction::set_compute_unit_limit(CU_LIMIT_MINE * 5);
        let cu_price_ix = ComputeBudgetInstruction::set_compute_unit_price(self.priority_fee);
        instructions.push(cu_limit_ix);
        instructions.push(cu_price_ix);
        for item in solutions {
            let pubkey = Pubkey::new_from_array(item.pubkey);
            let ix_mine = ore::instruction::mine(
                pubkey,
                BUS_ADDRESSES[bus_id as usize],
                KeccakHash::new(&item.hash).into(),
                item.non,
            );
            instructions.push(ix_mine);
            signers.push(self.get_signer(&pubkey));
        }

        let signers_refs: Vec<&Keypair> = signers.iter().collect();
        info!(
            "instructions {:?}, singers {:?}",
            instructions.len(),
            signers.len(),
        );
        let mut attemp = 0;
        loop {
            match &self
                .send_and_confirm_mutiple(&instructions, &signers_refs, true)
                .await
            {
                Ok(sig) => {
                    info!("Success send transaction: {}", sig);
                    break;
                }
                Err(e) => {
                    error!("Failed to send transaction: {:?}", e);
                    if attemp > 5 {
                        break;
                    }
                    attemp += 1;
                    sleep(Duration::from_secs(2)).await;
                }
            }
        }
    }
    pub async fn collect_all_the_solution(
        &self,
        mut consumer: lapin::Consumer,
        tx: Sender<Vec<FoundSolution>>,
        // send_channel: Channel,
    ) {
        let mut list: VecDeque<Vec<FoundSolution>> = VecDeque::new();
        list.push_front(Vec::new());
        while let Some(solution) = consumer.next().await {
            match solution {
                Ok((channel, solution)) => {
                    match serde_json::from_slice::<FoundSolution>(&solution.data) {
                        Ok(task) => {
                            let proof_ = get_proof(&self.rpc_client, task.pubkey.into()).await;
                            let treasury = get_treasury(&self.rpc_client).await;
                            let kekehash: KeccakHash = KeccakHash::new(&task.hash);
                            if self.validate_hash(
                                kekehash.into(),
                                proof_.hash.into(),
                                task.pubkey.into(),
                                task.non,
                                treasury.difficulty.into(),
                            ) {
                                info!(
                                    "got a new validate solution from miner {}",
                                    KeccakHash::new_from_array(task.pubkey)
                                );
                                let mut add = true;
                                for vec in list.iter_mut() {
                                    if !vec.iter().any(|item| item.pubkey == task.pubkey) {
                                        vec.push(task.clone());
                                        add = false;
                                        break;
                                    }
                                }
                                if add {
                                    list.push_back(vec![task]);
                                }
                                if let Some(head) = list.front() {
                                    if head.len() == 5 {
                                        if let Err(e) = tx.send(head.to_vec()).await {
                                            error!(
                                                "send head to sender failed the error is {:?}",
                                                e
                                            );
                                        }
                                        info!("head package send to send function");
                                        //pop_first
                                        list.pop_front();
                                    } else {
                                        info!("right now we got {} solutions waited", head.len());
                                    }
                                }
                            } else {
                                warn!("Hash already validated! An earlier transaction must have landed. purge the queue...");

                                for vec in list.iter_mut() {
                                    vec.retain(|item| item.hash != task.hash);
                                }
                                list.retain(|vec| !vec.is_empty());
                                // warn!(
                                //     "Send the update information to miner {}",
                                //     KeccakHash::new(&task.pubkey)
                                // );
                                // let payload = NewTask {
                                //     pubkey: task.pubkey,
                                //     hash: proof_.hash.0,
                                //     difficulty: treasury.difficulty.0,
                                // };
                                // let task_json = serde_json::to_string(&payload).unwrap();
                                // let _confirm = send_channel
                                //     .basic_publish(
                                //         "",          // 默认交换机
                                //         PROOF_QUEUE, // 队列名称
                                //         BasicPublishOptions::default(),
                                //         task_json.as_bytes().to_vec(),
                                //         BasicProperties::default(),
                                //     )
                                //     .await
                                //     .unwrap()
                                //     .await
                                //     .unwrap();
                            }
                        }
                        Err(e) => {
                            error!("cant seriazed the message: {:?}", e)
                        }
                    }
                    // Acknowledge the message whether it was successfully processed or not
                    if let Err(ack_error) = channel
                        .basic_ack(solution.delivery_tag, BasicAckOptions::default())
                        .await
                    {
                        error!("Error acknowledging message: {:?}", ack_error);
                    }
                }
                Err(e) => {
                    error!("read message error is {:?}", e)
                }
            }
        }
    }
    //not nessary
    pub async fn moniter_mutiple_reset_proch(&self) {
        loop {
            let last_treasury = get_treasury(&self.rpc_client).await;
            let clock = get_clock_account(&self.rpc_client).await;
            // let threshold = last_treasury.last_reset_at.saturating_add(70);
            let during_time = clock.unix_timestamp - last_treasury.last_reset_at;
            info!("after last rest time : {}s", &during_time);
            // if clock.unix_timestamp.ge(&threshold) {
            //     info!("reset the reward time get in to the pool");
            //     for (pubkey, _keypair_path) in self.client_inf.iter() {
            //         let mut instructions: Vec<Instruction> = Vec::new();
            //         let mut signers: Vec<Keypair> = Vec::new();
            //         let cu_limit_ix =
            //             ComputeBudgetInstruction::set_compute_unit_limit(CU_LIMIT_RESET);
            //         let cu_price_ix = ComputeBudgetInstruction::set_compute_unit_price(1);
            //         instructions.push(cu_limit_ix);
            //         instructions.push(cu_price_ix);
            //         let reset_ix = ore::instruction::reset(*pubkey);
            //         instructions.push(reset_ix);
            //         signers.push(self.get_signer(pubkey));
            //         let signers_refs: Vec<&Keypair> = signers.iter().collect();
            //         if let Err(e) = self
            //             .send_and_confirm_mutiple(&instructions, &signers_refs, true)
            //             .await
            //         {
            //             error!("sending moniter reset proch failed the error is: {:?}", e);
            //         }
            //     }

            //     info!("reset the clock");
            // }
            let sleep_time = 60 - during_time;
            sleep(Duration::from_secs(sleep_time as u64)).await;
        }
    }
    pub async fn get_mutiple_current_proof_from_contract(&self, channel: &Channel) {
        warn!("initialze all proof for miners");
        self.register_mutiple().await;
        let mut map: HashMap<Pubkey, Hash> = HashMap::new();
        // let signer = &self.signer();
        for (pubkey, _keypair) in self.client_inf.iter() {
            let new_proof = get_proof(&self.rpc_client, *pubkey).await;
            let new_treasury = get_treasury(&self.rpc_client).await;
            warn!(
                "Initialze miner {} Total Hash: {} Reward rate: {}",
                // new_proof.hash,
                pubkey,
                new_proof.total_hashes,
                (new_treasury.reward_rate as f64) / (10f64.powf(ore::TOKEN_DECIMALS as f64))
            );
            // let type_name = INITIAL_TASK.to_string();
            let payload = NewTask {
                msg_type: INITIAL_TASK.to_string(),
                pubkey: pubkey.to_bytes(),
                hash: new_proof.hash.0,
                difficulty: new_treasury.difficulty.0,
            };
            // println!("{:?}", &new_treasury.difficulty.0);
            let task_json = serde_json::to_string(&payload).unwrap();
            let _confirm = channel
                .basic_publish(
                    "",          // 默认交换机
                    PROOF_QUEUE, // 队列名称
                    BasicPublishOptions::default(),
                    task_json.as_bytes().to_vec(),
                    BasicProperties::default(),
                )
                .await
                .unwrap()
                .await
                .unwrap();
            map.insert(*pubkey, new_proof.hash);
            info!("miner {}'s proof work is {}", &pubkey, &new_proof.hash);
        }
        info!("initialze all account for challenge for moniter, sleep for 15s ");
        sleep(Duration::from_secs(15)).await;
        loop {
            let mut updates = Vec::new();
            for (miner_pubkey, last_proof_hash) in map.iter_mut() {
                let current_proof = get_proof(&self.rpc_client, *miner_pubkey).await;
                // info!(
                //     "check miner {} current_proof: {} last_proof:{}",
                //     &miner_pubkey, current_proof.hash, last_proof_hash
                // );

                if current_proof.hash != *last_proof_hash {
                    let treasury = get_treasury(&self.rpc_client).await;
                    warn!(
                        "New proof for {} Total Hash: {} Reward rate: {}",
                        &miner_pubkey,
                        current_proof.total_hashes,
                        (treasury.reward_rate as f64) / (10f64.powf(ore::TOKEN_DECIMALS as f64))
                    );

                    let payload = NewTask {
                        msg_type: UPDATE_TASK.to_string(),
                        pubkey: miner_pubkey.to_bytes(),
                        hash: current_proof.hash.0,
                        difficulty: treasury.difficulty.0,
                    };
                    let task_json = serde_json::to_string(&payload).unwrap();
                    let _confirm = channel
                        .basic_publish(
                            "",          // 默认交换机
                            PROOF_QUEUE, // 队列名称
                            BasicPublishOptions::default(),
                            task_json.as_bytes().to_vec(),
                            BasicProperties::default(),
                        )
                        .await
                        .unwrap()
                        .await
                        .unwrap();
                    updates.push((miner_pubkey.clone(), current_proof.hash));
                }
            }
            if updates.len() != 0 {
                for (key, new_hash) in updates {
                    map.insert(key, new_hash);
                    info!(
                        "Push new proof: {} to the map for miner {}",
                        &new_hash, &key
                    );
                }
            } else {
                info!("Didnt have any new proof, keep monitering");
            }
            // sleep(Duration::from_secs(60)).await;
            sleep(Duration::from_secs(15)).await;
        }
    }
    pub async fn find_bus_id(&self) -> u64 {
        let mut max_reward_bus: Option<Bus> = None;
        for &address in BUS_ADDRESSES.iter() {
            match self.get_bus(&address).await {
                Ok(bus) => {
                    // println!("{:?}", &bus);
                    if let Some(max_bus) = &max_reward_bus {
                        if bus.rewards > max_bus.rewards {
                            max_reward_bus = Some(bus);
                        }
                    } else {
                        max_reward_bus = Some(bus);
                    }
                }
                Err(e) => {
                    error!("Error getting bus at address {}: {}", address, e);
                    continue;
                }
            }
        }
        if let Some(bus) = max_reward_bus {
            return bus.id;
        } else {
            return 7;
        }
    }
    pub async fn get_bus(&self, address: &Pubkey) -> Result<Bus, anyhow::Error> {
        let _data = match self.rpc_client.get_account_data(&address).await {
            Ok(data) => {
                let _ = Bus::try_from_bytes(&data).map_err(|e| println!("{:?}", e));
                return Ok(*Bus::try_from_bytes(&data).unwrap());
            }
            Err(e) => {
                return Err(anyhow!(
                    "Failed to get bus account for {}: {:?}",
                    address,
                    e
                ));
            }
        };
    }
    pub fn validate_hash(
        &self,
        hash: KeccakHash,
        current_hash: KeccakHash,
        signer: Pubkey,
        nonce: u64,
        difficulty: KeccakHash,
    ) -> bool {
        // Validate hash correctness
        let hash_ = hashv(&[
            current_hash.as_ref(),
            signer.as_ref(),
            nonce.to_le_bytes().as_slice(),
        ]);
        if sol_memcmp(hash.as_ref(), hash_.as_ref(), HASH_BYTES) != 0 {
            return false;
        }

        // Validate hash difficulty
        if hash.gt(&difficulty) {
            return false;
        }

        true
    }
}
