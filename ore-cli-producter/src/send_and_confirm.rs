use std::time::Duration;

use crate::utils::get_treasury;
use log::*;
use solana_client::{
    client_error::{ClientError, ClientErrorKind, Result as ClientResult},
    rpc_config::RpcSendTransactionConfig,
};
use solana_program::instruction::Instruction;
use solana_sdk::{
    commitment_config::CommitmentLevel,
    signature::{Keypair, Signature, Signer},
    transaction::Transaction,
};
use solana_transaction_status::{TransactionConfirmationStatus, UiTransactionEncoding};

use crate::utils::get_clock_account;
use crate::Miner;
use tokio::time::sleep;

const RPC_RETRIES: usize = 0;
// const SIMULATION_RETRIES: usize = 3;
const GATEWAY_RETRIES: usize = 4;
const CONFIRM_RETRIES: usize = 4;

const CONFIRM_DELAY: u64 = 5000;
const GATEWAY_DELAY: u64 = 2000;
const TIME_POCH: u64 = 30;

impl Miner {
    pub async fn send_and_confirm_mutiple(
        &self,
        ixs: &[Instruction],
        signers: &[&Keypair],
        skip_confirm: bool,
    ) -> ClientResult<Signature> {
        let send_client = self.rpc_client_send.clone();
        let info_client = self.rpc_client.clone();
        // Build tx
        let (hash, slot) = info_client
            .get_latest_blockhash_with_commitment(self.rpc_client.commitment())
            .await
            .unwrap();
        let send_cfg = RpcSendTransactionConfig {
            skip_preflight: true,
            preflight_commitment: Some(CommitmentLevel::Finalized),
            encoding: Some(UiTransactionEncoding::Base64),
            max_retries: Some(RPC_RETRIES),
            min_context_slot: Some(slot),
        };
        let payer_pubkey = &signers[0].pubkey();
        let mut tx = Transaction::new_with_payer(ixs, Some(payer_pubkey));

        match tx.try_sign(signers, hash) {
            Ok(_txn) => {}
            Err(e) => {
                error!(
                    "send tx err the err is {:?} \n got {} signer \n {:?}",
                    e,
                    signers.len(),
                    signers,
                );
                return Err(e.into());
            }
        }
        // tx.sign(signers, hash);
        let fee = &info_client.get_fee_for_message(&tx.message).await;
        info!("Transaction's fee: {:?} payer: {:?}", fee, payer_pubkey);
        loop {
            let last_treasury = get_treasury(&self.rpc_client).await;
            let clock = get_clock_account(&self.rpc_client).await;

            let during_time = clock.unix_timestamp - last_treasury.last_reset_at;
            if during_time >= TIME_POCH.try_into().unwrap() {
                info!("didnt rest the proof reward time sleep 3s");
                sleep(Duration::from_secs(3)).await;
            } else {
                break;
            }
        }

        let mut attempts = 0;
        loop {
            // println!("Attempt: {:?}", attempts);
            match send_client
                .send_transaction_with_config(&tx, send_cfg)
                .await
            {
                Ok(sig) => {
                    // Confirm tx
                    if skip_confirm {
                        return Ok(sig);
                    }
                    for _ in 0..CONFIRM_RETRIES {
                        std::thread::sleep(Duration::from_millis(CONFIRM_DELAY));
                        match info_client.get_signature_statuses(&[sig]).await {
                            Ok(signature_statuses) => {
                                info!("Confirmation: {:?}", signature_statuses.value[0]);
                                for signature_status in signature_statuses.value {
                                    if let Some(signature_status) = signature_status.as_ref() {
                                        if signature_status.confirmation_status.is_some() {
                                            let current_commitment = signature_status
                                                .confirmation_status
                                                .as_ref()
                                                .unwrap();
                                            match current_commitment {
                                                TransactionConfirmationStatus::Processed => {}
                                                TransactionConfirmationStatus::Confirmed
                                                | TransactionConfirmationStatus::Finalized => {
                                                    println!("Transaction landed!");
                                                    std::thread::sleep(Duration::from_millis(
                                                        GATEWAY_DELAY,
                                                    ));
                                                    return Ok(sig);
                                                }
                                            }
                                        } else {
                                            info!("No status");
                                        }
                                    }
                                }
                            }

                            // Handle confirmation errors
                            Err(err) => {
                                error!("{:?}", err.kind().to_string());
                            }
                        }
                    }
                    info!("Transaction did not land");
                }

                // Handle submit errors
                Err(err) => {
                    println!("{:?}", err.kind().to_string());
                }
            }

            // Retry
            // stdout.flush().ok();
            std::thread::sleep(Duration::from_millis(GATEWAY_DELAY));
            attempts += 1;
            if attempts > GATEWAY_RETRIES {
                return Err(ClientError {
                    request: None,
                    kind: ClientErrorKind::Custom("Max retries".into()),
                });
            }
        }
    }
}
