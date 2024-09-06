use log::warn;

use crate::{utils::proof_pubkey, Miner};

impl Miner {
    // pub async fn register(&self) {
    //     // Return early if miner is already registered
    //     let signer = self.signer();
    //     let proof_address = proof_pubkey(signer.pubkey());
    //     let client = self.rpc_client.clone();
    //     if client.get_account(&proof_address).await.is_ok() {
    //         println!("already start");
    //         return;
    //     }

    //     // Sign and send transaction.
    //     println!("Generating challenge...");
    //     'send: loop {
    //         let ix = ore::instruction::register(signer.pubkey());
    //         if self.send_and_confirm(&[ix], true, false).await.is_ok() {
    //             break 'send;
    //         }
    //     }
    // }
    pub async fn register_mutiple(&self) {
        for (key, _keypair) in self.client_inf.iter() {
            let signer = self.get_signer(&key);
            let proof_address = proof_pubkey(*key);
            let client = self.rpc_client.clone();
            if client.get_account(&proof_address).await.is_ok() {
                println!("{} already registed", key);
                return;
            }

            warn!("Account {} Need register", key);

            'send: loop {
                let ix = ore::instruction::register(*key);
                if self
                    .send_and_confirm_mutiple(&[ix], &[&signer], false)
                    .await
                    .is_ok()
                {
                    break 'send;
                }
            }
        }
    }
}
