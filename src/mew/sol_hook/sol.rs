#![allow(deprecated)]
use { 
    crate::mew::{sol_hook::compute_budget::{ix_set_compute_unit_limit, ix_set_compute_unit_price}, 
    swqos::{blox::{Bloxroute, SubmitOpts, SubmitProtection}, 
    helius::HeliusSender, jito::JitoClient, nextblock::NextBlock, temporal::TemporalSender, zero_slot::ZeroSlot}}, 
    base64::{engine::general_purpose::STANDARD as B64, Engine as _}, 
    borsh::BorshDeserialize, 
    mpl_token_metadata::accounts::Metadata as MplMetadata, 
    serde::{Deserialize, Serialize}, 
    solana_client::{nonblocking::rpc_client::RpcClient, 
    rpc_config::{RpcSendTransactionConfig, RpcTransactionConfig}, rpc_response::SlotInfo}, 
    solana_commitment_config::CommitmentConfig, solana_keypair::Keypair, solana_message::{v0::Message as V0Message, VersionedMessage}, 
    solana_program::{hash::Hash, instruction::Instruction, nonce::{state::Versions as NonceVersions, State}, pubkey::Pubkey, system_instruction}, 
    solana_pubsub_client::nonblocking::pubsub_client::PubsubClient, solana_rpc_client_types::{config::{RpcTransactionLogsConfig, RpcTransactionLogsFilter}, response::RpcLogsResponse}, solana_signature::Signature, 
    solana_signer::Signer, solana_transaction::versioned::VersionedTransaction, solana_transaction_status::{EncodedConfirmedTransactionWithStatusMeta, UiTransactionEncoding}, std::{collections::{HashMap, HashSet}, str::FromStr, sync::Arc, time::{Duration, Instant}}, tokio::sync::mpsc::{self, Receiver}, tokio_stream::{Stream, StreamExt}, tonic::Status, yellowstone_grpc_client::{GeyserGrpcBuilder, GeyserGrpcClient, Interceptor, InterceptorXToken}, yellowstone_grpc_proto::geyser::{
        CommitmentLevel, SubscribeRequest, SubscribeRequestFilterAccounts, SubscribeRequestFilterSlots, SubscribeRequestFilterTransactions, SubscribeUpdate
    }, 
    spl_token::state::Mint as SplMint,
    spl_token_2022::state::Mint as SplMint2022,
    solana_program::program_pack::Pack,
    std::pin::Pin,
    crate::{log, warn},
    solana_rpc_client_types::config::RpcAccountInfoConfig,
    solana_account_decoder_client_types::UiAccountEncoding,
    solana_account::Account,
    crate::mew::writing::cc
};

pub type YellowstoneClient = GeyserGrpcClient<InterceptorXToken>;

pub const TOKEN_PROGRAM_ID: Pubkey = Pubkey::from_str_const("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA");
pub const TOKEN_2022_PROGRAM_ID: Pubkey = Pubkey::from_str_const("TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb");
pub const ATA_PROGRAM_ID: Pubkey = Pubkey::from_str_const("ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL");
pub const WSOL_MINT: Pubkey = Pubkey::from_str_const("So11111111111111111111111111111111111111112");
pub const USDC_MINT: Pubkey = Pubkey::from_str_const("EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v");
pub const METADATA_PROGRAM_ID: Pubkey = Pubkey::from_str_const("metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s");
pub const SYSTEM_PROGRAM: Pubkey = Pubkey::from_str_const("11111111111111111111111111111111");

fn trim_nul(s: &str) -> &str {
    s.trim_matches('\0')
}

pub type SubStream =
    Pin<Box<dyn Stream<Item = Result<SubscribeUpdate, Status>> + Send>>;

#[derive(Debug)]
pub enum PriorityFeeLevel {
    Low,
    Medium,
    High,
    Turbo,
    Max,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TokenInfo {
    pub mint: Pubkey,
    pub name: String,
    pub symbol: String,
    pub uri: String,
    pub creator: Option<Pubkey>,
    pub authority: Pubkey,
}

#[derive(Clone)]
pub struct SolHook {
    pub rpc_client: Arc<RpcClient>,
}

impl SolHook {
    pub fn new(rpc_client: String) -> Self {
        Self { rpc_client: Arc::new(RpcClient::new(rpc_client)) }
    }

    pub async fn get_nonce_data(
        &self,
        nonce_account: &Pubkey,
    ) -> anyhow::Result<(Hash, Pubkey)> {
        let acc = match self.rpc_client.get_account(nonce_account).await {
            Ok(acc) => acc,
            Err(_) => anyhow::bail!("nonce account does not exist"),
        };
        let versions: NonceVersions = bincode::deserialize(&acc.data)?;
        match versions.state() {
            State::Initialized(d) => Ok((d.durable_nonce.as_hash().clone(), d.authority)),
            _ => anyhow::bail!("nonce account is not initialized"),
        }
    }

    pub async fn create_nonce_account(
        &self,
        payer: &Keypair,
        nonce_account: &Keypair,
        authority: &Pubkey,
    ) -> anyhow::Result<Signature> {
        let space = State::size();
        let lamports = self
            .rpc_client
            .get_minimum_balance_for_rent_exemption(space)
            .await?;
    
        let ixs = system_instruction::create_nonce_account(
            &payer.pubkey(),
            &nonce_account.pubkey(),
            authority,
            lamports,
        );
    
        let (blockhash, _) = self
            .rpc_client
            .get_latest_blockhash_with_commitment(CommitmentConfig::processed())
            .await?;
    
        let msg = V0Message::try_compile(&payer.pubkey(), &ixs, &[], blockhash)?;
        let tx  = VersionedTransaction::try_new(VersionedMessage::V0(msg), &[payer, nonce_account])?;
    
        let sig = self.rpc_client
            .send_transaction_with_config(
                &tx,
                RpcSendTransactionConfig {
                    skip_preflight: true,
                    preflight_commitment: None,
                    encoding: None,
                    max_retries: Some(0),
                    min_context_slot: None,
                },
            )
            .await?;
    
        Ok(sig)
    }

    pub async fn subscribe_logs_channel(
        &self,
        ws_url: &str,
        filter: RpcTransactionLogsFilter,
        commitment: CommitmentConfig,
    ) -> anyhow::Result<(Receiver<RpcLogsResponse>, tokio::task::JoinHandle<()>)> {
        let ws = ws_url.to_string();
        let (tx, rx) = mpsc::channel::<RpcLogsResponse>(1024);

        let handle = tokio::spawn(async move {
            let client = match PubsubClient::new(&ws).await {
                Ok(c) => c,
                Err(e) => { warn!("ws connect failed: {e}"); return; }
            };
            let cfg = RpcTransactionLogsConfig {
                commitment: Some(commitment),
            };
            let (mut stream, _unsub) = match client.logs_subscribe(filter, cfg).await {
                Ok(p) => p,
                Err(e) => { warn!("subscribe failed: {e}"); return; }
            };

            while let Some(msg) = stream.next().await {
                if let Err(e) = tx.send(msg.value).await {
                    warn!("send failed: {e}");
                    return;
                }
            }
        });

        Ok((rx, handle))
    }   

    pub async fn subscribe_slot_channel(
        &self,
        ws_url: &str,
    ) -> anyhow::Result<(Receiver<SlotInfo>, tokio::task::JoinHandle<()>)> {
        let ws = ws_url.to_string();
        let (tx, rx) = mpsc::channel::<SlotInfo>(1024);

        let handle = tokio::spawn(async move {
            let client = match PubsubClient::new(&ws).await {
                Ok(c) => c,
                Err(e) => { warn!("ws connect failed: {e}"); return; }
            };
            let (mut stream, _unsub) = match client.slot_subscribe().await {
                Ok(p) => p,
                Err(e) => { warn!("subscribe failed: {e}"); return; }
            };
            while let Some(msg) = stream.next().await {
                let _ = tx.send(msg).await;
            }
        });
        Ok((rx, handle))
    }
    
    pub async fn grpc_connect(
        url: impl Into<String>,
        token: Option<impl Into<String>>,
    ) -> anyhow::Result<GeyserGrpcClient<impl Interceptor>> {
        let token = token.map(Into::into);
        let mut b = GeyserGrpcBuilder::from_shared(url.into())?;
        if let Some(t) = token {
            b = b.x_token(Some(t))?;
        }
        let grpc = b.connect().await?;
        Ok(grpc)
    }

    pub async fn grpc_subscribe_slot_channel(
        grpc_client: &mut GeyserGrpcClient<impl Interceptor>,
    ) -> anyhow::Result<impl Stream<Item = Result<SubscribeUpdate, Status>>> {
        let request = SubscribeRequest {
            slots: HashMap::from([("all".into(), SubscribeRequestFilterSlots {
                filter_by_commitment: Some(true),
                interslot_updates: Some(true),
            })]),
            ..Default::default()
        };
    
        let (_, stream) = grpc_client.subscribe_with_request(Some(request)).await?;
        Ok(stream)
    }

    pub async fn grpc_subscribe_accounts(grpc_client: &mut GeyserGrpcClient<impl Interceptor>, account: &Pubkey) -> anyhow::Result<impl Stream<Item = Result<SubscribeUpdate, Status>>> {
        let request = SubscribeRequest {
            accounts: HashMap::from([("all".into(), SubscribeRequestFilterAccounts {
                account: vec![account.to_string()],
                owner: vec![],
                filters: vec![],
                nonempty_txn_signature: Some(false),
            })]),
            ..Default::default()
        };
        let (_, stream) = grpc_client.subscribe_with_request(Some(request)).await?;
        Ok(stream)
    }

    pub async fn grpc_subscribe_transactions(grpc_client: &mut GeyserGrpcClient<impl Interceptor>, accounts: &[Pubkey]) -> anyhow::Result<impl Stream<Item = Result<SubscribeUpdate, Status>>> {
        let request = SubscribeRequest {
            transactions: HashMap::from([("all".into(), SubscribeRequestFilterTransactions {
                vote: Some(false),
                failed: Some(false),
                signature: None,
                account_include: vec![],
                account_exclude: vec![],
                account_required: accounts.iter().map(|a| a.to_string()).collect(),
            })]),
            ..Default::default()
        };
        let (_, stream) = grpc_client.subscribe_with_request(Some(request)).await?;
        Ok(stream)
    }

    pub async fn grpc_latest_blockhash(&self, client: &mut GeyserGrpcClient<impl Interceptor>) -> anyhow::Result<Hash> {
        let resp = client.get_latest_blockhash(Some(CommitmentLevel::Processed)).await?;
        Ok(Hash::from_str(&resp.blockhash)?)
    }   

    pub fn parse_signature(signature: &[u8]) -> anyhow::Result<Signature> {
        if signature.len() != 64 {
            anyhow::bail!("Signature must be exactly 64 bytes");
        }
        let raw_signature_array: [u8; 64] = signature.try_into()?;
        Ok(Signature::from(raw_signature_array))
    }

    pub async fn exists(&self, address: &Pubkey) -> anyhow::Result<bool> {
        Ok(self.rpc_client.get_account(address).await.is_ok())
    }

    pub async fn get_token_decimals(&self, mint: &Pubkey) -> anyhow::Result<u8> {
        if *mint == WSOL_MINT {
            return Ok(9);
        }
    
        let acc = match self.rpc_client.get_account_with_commitment(mint, CommitmentConfig::processed()).await {
            Ok(acc) => acc.value.unwrap_or_default(),
            Err(e) => {
                anyhow::bail!("Error getting account: {:?}", e);
            }
        };
        if acc.owner == TOKEN_PROGRAM_ID {
            let mint_state = SplMint::unpack(&acc.data)
                .map_err(|e| anyhow::anyhow!("not an SPL Token mint: {e}"))?;
            Ok(mint_state.decimals)
        } else if acc.owner == TOKEN_2022_PROGRAM_ID {
            let mint_state = SplMint2022::unpack(&acc.data)
                .map_err(|e| anyhow::anyhow!("not a Token-2022 mint: {e}"))?;
            Ok(mint_state.decimals)
        } else {
            anyhow::bail!("account {} not owned by SPL Token program(s): {}", mint, acc.owner);
        }
    }

    pub async fn get_token_metadata(&self, mint: &Pubkey) -> anyhow::Result<(MplMetadata, Pubkey)> {
        let (pda, _) = Pubkey::find_program_address(
            &[b"metadata", METADATA_PROGRAM_ID.as_ref(), mint.as_ref()],
            &METADATA_PROGRAM_ID,
        );
        let acc = self.rpc_client.get_account(&pda).await?;
        let mut data: &[u8] = &acc.data;
        let md = MplMetadata::deserialize(&mut data)?;
        Ok((md, pda))
    }

    pub async fn get_token_info(&self, mint: &Pubkey) -> anyhow::Result<TokenInfo> {
        let (md, _) = self.get_token_metadata(mint).await?;

        let creator = md
            .creators
            .as_ref()
            .and_then(
                |v| v.first().map(|c| c.address)
            );

        Ok(TokenInfo {
            mint: *mint,
            name: trim_nul(&md.name).to_string(),
            symbol: trim_nul(&md.symbol).to_string(),
            uri: trim_nul(&md.uri).to_string(),
            creator: creator,
            authority: md.update_authority,
        })
    }

    pub async fn get_token_program_id(&self, token_address: &Pubkey) -> anyhow::Result<Pubkey> {
        let info = self.rpc_client.get_account_with_commitment(token_address, CommitmentConfig::processed()).await?;
        Ok(info.value.ok_or(anyhow::anyhow!("account not found"))?.owner)
    }

    pub async fn get_ata_auto(&self, owner: &Pubkey, mint: &Pubkey) -> anyhow::Result<Pubkey> {
        let token_program_id = self.get_token_program_id(&mint).await?;
        let ata = match token_program_id {
            TOKEN_PROGRAM_ID => self.get_ata_for_token(owner, mint),
            TOKEN_2022_PROGRAM_ID => self.get_ata_for_token2022(owner, mint),
            _ => {
                return Err(anyhow::anyhow!("Invalid token program id"))
            }
        };
        Ok(ata)
    }

    pub fn get_ata_for_token(&self, owner: &Pubkey, mint: &Pubkey) -> Pubkey {
        let (ata, _) = Pubkey::find_program_address(
            &[owner.as_ref(), TOKEN_PROGRAM_ID.as_ref(), mint.as_ref()],
            &ATA_PROGRAM_ID,
        );
        ata
    }

    pub fn get_ata_for_token2022(&self, owner: &Pubkey, mint: &Pubkey) -> Pubkey {
        let (ata, _) = Pubkey::find_program_address(
            &[owner.as_ref(), TOKEN_2022_PROGRAM_ID.as_ref(), mint.as_ref()],
            &ATA_PROGRAM_ID,
        );
        ata
    }

    pub async fn get_account(&self, address: &Pubkey) -> anyhow::Result<Account> {
        let account = self.rpc_client.get_account_with_config(address, RpcAccountInfoConfig {
            encoding: Some(UiAccountEncoding::JsonParsed),
            commitment: Some(CommitmentConfig::finalized()),
            min_context_slot: None,
            data_slice: None,
        }).await?;
        Ok(account.value.unwrap())
    }

    pub async fn get_balance(&self, address: &Pubkey) -> anyhow::Result<f64> {
        let balance = self.rpc_client.get_balance(address).await?;
        Ok(balance as f64 / 1e9)
    }

    /// Returns the balance of a token in the ui amount format
    /// 
    /// # Arguments
    /// 
    /// * `address` - The address of the account to get the balance of
    /// * `token_address` - The address of the token to get the balance of
    /// 
    /// # Returns
    /// 
    /// The balance of the token in the ui_amount format
    /// 
    /// # Errors
    /// 
    pub async fn get_token_balance(&self, address: &Pubkey, token_address: &Pubkey) -> anyhow::Result<f64> {
        let token_ata = match self.get_ata_auto(address, token_address).await {
            Ok(ata) => ata,
            Err(e) => {
                return Err(anyhow::anyhow!(format!("Error getting token ata: {:?}", e)))
            }
        };
        let token_balance = self.rpc_client.get_token_account_balance_with_commitment(&token_ata, CommitmentConfig::confirmed()).await?;
        Ok(token_balance.value.ui_amount.unwrap_or(0.0))
    }

    pub async fn get_token_balance_from_ata(&self, ata: &Pubkey) -> anyhow::Result<f64> {
        let token_balance = self.rpc_client.get_token_account_balance_with_commitment(ata, CommitmentConfig::confirmed()).await?;
        Ok(token_balance.value.ui_amount.unwrap_or(0.0))
    }

    pub async fn get_transaction(&self, signature: &Signature) -> anyhow::Result<EncodedConfirmedTransactionWithStatusMeta> {
        let tx = self.rpc_client.get_transaction_with_config(signature, RpcTransactionConfig {
            commitment: Some(CommitmentConfig::confirmed()),
            max_supported_transaction_version: Some(0),
            ..Default::default()
        }).await?;
        Ok(tx)
    }

    pub async fn get_transaction_parsed(
        &self,
        sig: &Signature,
    ) -> anyhow::Result<EncodedConfirmedTransactionWithStatusMeta> {
        let cfg = RpcTransactionConfig {
            encoding: Some(UiTransactionEncoding::JsonParsed),
            commitment: Some(CommitmentConfig::confirmed()),
            max_supported_transaction_version: Some(0),
        };
        Ok(self.rpc_client.get_transaction_with_config(sig, cfg).await?)
    }

    pub async fn fetch_priority_fee(
        &self,
        level: &PriorityFeeLevel,
        addresses: &[Pubkey],
    ) -> anyhow::Result<u64> {
        let mut set = HashSet::with_capacity(addresses.len());
        let mut addrs: Vec<Pubkey> = addresses
            .iter()
            .copied()
            .filter(|k| set.insert(*k))
            .take(128)
            .collect();

        if addrs.is_empty() {
            addrs = vec![SYSTEM_PROGRAM, TOKEN_PROGRAM_ID, TOKEN_2022_PROGRAM_ID];
        }

        let mut samples = self.rpc_client
            .get_recent_prioritization_fees(&addrs)
            .await?
            .into_iter()
            .map(|r| r.prioritization_fee)
            .filter(|&v| v > 0)
            .collect::<Vec<u64>>();

        if samples.is_empty() {
            return Ok(match level {
                PriorityFeeLevel::Low    => 50_000,
                PriorityFeeLevel::Medium => 100_000,
                PriorityFeeLevel::High   => 500_000, 
                PriorityFeeLevel::Turbo  => 1_000_000,
                PriorityFeeLevel::Max    => 30_000_000,
            });
        }

        samples.sort_unstable();
        let p = match level {
            PriorityFeeLevel::Low    => 0.50,
            PriorityFeeLevel::Medium => 0.75,
            PriorityFeeLevel::High   => 0.90,
            PriorityFeeLevel::Turbo  => 0.95,
            PriorityFeeLevel::Max    => 0.99,
        };

        let idx = ((samples.len() - 1) as f64 * p).ceil() as usize;
        let mut price = samples[idx.min(samples.len() - 1)];

        if price < 10_000 { price = 10_000; }
        if price > 30_000_000  { price = 30_000_000;  }

        Ok(price)
    }

    pub async fn fetch_sol_price(&self) -> anyhow::Result<f64> {
        let resp = reqwest::get("https://api.coingecko.com/api/v3/simple/price?ids=solana&vs_currencies=usd");
        match resp.await {
            Ok(data) => {
                let data: serde_json::Value = data.json().await?;
                let sol_price = data["solana"]["usd"].as_f64().unwrap();
                Ok(sol_price)
            }
            Err(e) => {
                anyhow::bail!("Error fetching sol price: {:?}", e);
            }
        }
    }

    pub async fn close_ata(&self, mint: &Pubkey, owner: &Keypair, return_ix: bool) -> anyhow::Result<(Instruction, Signature)> {
        let blockhash = self.rpc_client.get_latest_blockhash_with_commitment(CommitmentConfig::processed()).await?.0;
        let ata = self.get_ata_auto(&owner.pubkey(), mint).await?;
        let ix = spl_token::instruction::close_account(
            &TOKEN_PROGRAM_ID,
            &ata,
            &owner.pubkey(),
            &owner.pubkey(),
            &[&owner.pubkey()],
        ).unwrap();
        if return_ix {
            return Ok((ix, Signature::default()));
        }
        let msg = VersionedMessage::V0(
            V0Message::try_compile(
                &owner.pubkey(),
                &[ix.clone()],
                &[],
                blockhash,
            )?
        );
        let tx = VersionedTransaction::try_new(msg, &[&owner])?;
        let sig = self.rpc_client.send_transaction_with_config(&tx, RpcSendTransactionConfig{ 
            skip_preflight: true,
            preflight_commitment: None,
            encoding: None,
            max_retries: Some(0),
            min_context_slot: None,
        }).await?;
        let mut retry_count = 0;
        while self.rpc_client.confirm_transaction_with_commitment(&sig, CommitmentConfig::confirmed()).await?.value == false {
            tokio::time::sleep(std::time::Duration::from_secs_f32(0.1)).await;
            retry_count += 1;
            if retry_count > 50 {
                anyhow::bail!("Transaction failed to confirm {}", sig.to_string());
            }
        }
        Ok((ix, sig))
    }

    pub async fn send(&self, mut ixs: Vec<Instruction>, payer: &Keypair, fee: u64, compute_budget: Option<u32>) -> anyhow::Result<Signature> {
        let time_start = Instant::now();
        ixs.insert(0, ix_set_compute_unit_price(fee));
        if let Some(compute_budget) = compute_budget {
            ixs.insert(1, ix_set_compute_unit_limit(compute_budget));
        }
        let blockhash = self.rpc_client.get_latest_blockhash_with_commitment(CommitmentConfig::processed()).await?.0;
        let block_1 = self.rpc_client.get_slot_with_commitment(CommitmentConfig::processed()).await?;
        let msg = VersionedMessage::V0(
            V0Message::try_compile(
                &payer.pubkey(),
                &ixs,
                &[],
                blockhash,
            )?
        );
        let tx = VersionedTransaction::try_new(msg, &[payer])?;
        let sig = self.rpc_client.send_transaction_with_config(&tx, RpcSendTransactionConfig{ 
            skip_preflight: true,
            preflight_commitment: None,
            encoding: None,
            max_retries: Some(0),
            min_context_slot: None,
        }).await?;
        log!("Time to send: {:?}", time_start.elapsed());
        let mut retry_count = 0;
        while self.rpc_client.confirm_transaction_with_commitment(&sig, CommitmentConfig::confirmed()).await?.value == false {
            tokio::time::sleep(std::time::Duration::from_secs_f32(0.1)).await;
            retry_count += 1;
            if retry_count > 50 {
                anyhow::bail!("Transaction failed to confirm {}", sig.to_string());
            }
        }
        let block_2 = self.rpc_client.get_slot_with_commitment(CommitmentConfig::processed()).await?;
        log!("Blocks diff: n+{:?}", block_2 - block_1);
        log!("Time taken: {:?}", time_start.elapsed());
        Ok(sig)
    }

    pub async fn send_with_jito(&self,
        jito_client: &JitoClient,
        mut ixs: Vec<Instruction>,
        payer: &Keypair,
        fee: u64,
        compute_budget: u32,
    ) -> anyhow::Result<Signature> {
        let time_start = Instant::now();
        ixs.insert(0, ix_set_compute_unit_price(fee));
        ixs.insert(1, ix_set_compute_unit_limit(compute_budget));

        let rta = jito_client.get_tip_account();
        let jito_tip_ix = solana_program::system_instruction::transfer(
            &payer.pubkey(),
            &rta,
            1_000_000,
        );

        ixs.insert(2, jito_tip_ix);
        let blockhash = self.rpc_client.get_latest_blockhash_with_commitment(CommitmentConfig::processed()).await?.0;
        let block_1 = self.rpc_client.get_slot_with_commitment(CommitmentConfig::processed()).await?;
        println!("Block 0: {:?}", block_1);
        let msg = VersionedMessage::V0(
            V0Message::try_compile(
                &payer.pubkey(),
                &ixs,
                &[],
                blockhash,
            )?
        );
        
        let tx = VersionedTransaction::try_new(msg, &[payer])?;

        let wire = bincode::serialize(&tx)?;
        let b64  = B64.encode(wire.clone());

        let resp = jito_client.send_transaction(&b64, true).send().await?;
        let v: serde_json::Value = resp.json().await?;
        let sig_str = v.get("result")
            .and_then(|r| r.as_str())
            .ok_or_else(|| anyhow::anyhow!(format!("no result in response: {}", v)))?;
        let sig = Signature::from_str(sig_str)?;
        let mut retry_count = 0;
        while self.rpc_client.confirm_transaction_with_commitment(&sig, CommitmentConfig::confirmed()).await?.value == false {
            tokio::time::sleep(std::time::Duration::from_secs_f32(0.1)).await;
            retry_count += 1;
            if retry_count > 50 {
                anyhow::bail!("Transaction failed to confirm https://solscan.io/tx/{}", sig.to_string());
            }
        }
        let block_2 = self.rpc_client.get_slot_with_commitment(CommitmentConfig::processed()).await?;
        println!("Blocks diff: n+{:?}", block_2 - block_1);
        println!("Time taken: {:?}", time_start.elapsed());
        Ok(sig)
    }

    pub async fn send_with_jito_bundle(
        &self,
        jito_client: &JitoClient,
        mut ixs: Vec<Instruction>,
        payer: &Keypair,
        fee: u64,
        compute_budget: u32,
        tip_lamports: u64,
    ) -> anyhow::Result<Signature> {
        ixs.insert(0, ix_set_compute_unit_price(fee));
        ixs.insert(1, ix_set_compute_unit_limit(compute_budget));

        let tip_acc = jito_client.get_tip_account();
        ixs.insert(2, solana_program::system_instruction::transfer(
            &payer.pubkey(), &tip_acc, tip_lamports,
        ));

        let (blockhash, _) = self
            .rpc_client
            .get_latest_blockhash_with_commitment(CommitmentConfig::processed())
            .await?;

        let msg = V0Message::try_compile(&payer.pubkey(), &ixs, &[], blockhash)?;
        let tx  = VersionedTransaction::try_new(VersionedMessage::V0(msg), &[payer])?;
        let sig = tx.signatures[0];

        let wire = bincode::serialize(&tx)?;
        let b64  = B64.encode(wire);

        let resp = jito_client.send_bundle(&[b64]).send().await?;
        let v: serde_json::Value = resp.json().await?;
        let bundle_id = v.get("result")
            .and_then(|r| r.as_str())
            .unwrap_or("<no-bundle-id>");
        eprintln!("Jito bundle_id: {}", bundle_id);

        let mut tries = 0u32;
        while !self.rpc_client
            .confirm_transaction_with_commitment(&sig, CommitmentConfig::confirmed())
            .await?
            .value
        {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            tries += 1;
            if tries > 300 {
                anyhow::bail!("Jito bundle timed out https://solscan.io/tx/{}", sig);
            }
        }
        Ok(sig)
    }

    pub async fn spray_with_jito(
        &self,
        mut ixs: Vec<Instruction>,
        payer: &Keypair,
        fee: u64,
        compute_budget: u32,
        clients: &Vec<JitoClient>,
    ) -> anyhow::Result<Signature> {
        let time_start = Instant::now();
        ixs.insert(0, ix_set_compute_unit_price(fee));
        ixs.insert(1, ix_set_compute_unit_limit(compute_budget));

        let rta = clients[0].get_tip_account();
        let jito_tip_ix = solana_program::system_instruction::transfer(
            &payer.pubkey(),
            &rta,
            1_000_000,
        );

        ixs.insert(2, jito_tip_ix);
        let blockhash = self.rpc_client.get_latest_blockhash_with_commitment(CommitmentConfig::processed()).await?.0;
        let block_1 = self.rpc_client.get_slot_with_commitment(CommitmentConfig::processed()).await?;
        println!("Block 0: {:?} | Elapsed: {:?}", block_1, time_start.elapsed());
        let msg = VersionedMessage::V0(
            V0Message::try_compile(
                &payer.pubkey(),
                &ixs,
                &[],
                blockhash,
            )?
        );
        
        let tx = VersionedTransaction::try_new(msg, &[payer])?;
        let sig = tx.signatures[0];
        let wire = bincode::serialize(&tx)?;
        let b64  = B64.encode(wire.clone());


        let be_fut = async {
            let reqs: Vec<_> = clients.iter().map(|c| c.send_transaction(&b64, true)).collect();
            self.spawn_forget(reqs).await;
        };

        let _ = tokio::join!(be_fut);
        let mut retry_count = 0;
        while self.rpc_client.confirm_transaction_with_commitment(&sig, CommitmentConfig::confirmed()).await?.value == false {
            tokio::time::sleep(std::time::Duration::from_secs_f32(0.1)).await;
            retry_count += 1;
            if retry_count > 50 {
                anyhow::bail!("Transaction failed to confirm https://solscan.io/tx/{}", sig.to_string());
            }
        }
        let block_2 = self.rpc_client.get_slot_with_commitment(CommitmentConfig::processed()).await?;
        println!("Blocks diff: n+{:?}", block_2 - block_1);
        println!("Time taken: {:?}", time_start.elapsed());
        Ok(sig)
    }

    pub async fn spray_with_jito_bundle(
        &self,
        mut ixs: Vec<Instruction>,
        payer: &Keypair,
        fee: u64,
        compute_budget: u32,
        clients: &[JitoClient],
        tip_lamports: u64,
    ) -> anyhow::Result<Signature> {
        let start = std::time::Instant::now();

        ixs.insert(0, ix_set_compute_unit_price(fee));
        ixs.insert(1, ix_set_compute_unit_limit(compute_budget));

        let tip_acc = clients[0].get_tip_account();
        ixs.insert(2, solana_program::system_instruction::transfer(
            &payer.pubkey(), &tip_acc, tip_lamports,
        ));

        let (blockhash, _) = self
            .rpc_client
            .get_latest_blockhash_with_commitment(CommitmentConfig::processed())
            .await?;
        let slot0 = self.rpc_client.get_slot_with_commitment(CommitmentConfig::processed()).await?;
        let msg = V0Message::try_compile(&payer.pubkey(), &ixs, &[], blockhash)?;
        let tx  = VersionedTransaction::try_new(VersionedMessage::V0(msg), &[payer])?;
        let sig = tx.signatures[0];

        let b64 = {
            let wire = bincode::serialize(&tx)?;
            B64.encode(wire)
        };

        // Fire-and-forget to all regions.
        let reqs: Vec<_> = clients.iter().map(|c| c.send_bundle(&[&b64])).collect();
        self.spawn_forget(reqs).await;

        // Confirm fast.
        let mut tries = 0u32;
        while !self.rpc_client
            .confirm_transaction_with_commitment(&sig, CommitmentConfig::confirmed())
            .await?
            .value
        {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            tries += 1;
            if tries > 300 {
                anyhow::bail!("Jito bundle spray: timeout https://solscan.io/tx/{}", sig);
            }
        }
        let slot1 = self.rpc_client.get_slot_with_commitment(CommitmentConfig::processed()).await?;
        println!("Blocks diff: n+{}", slot1.saturating_sub(slot0));
        let end = std::time::Instant::now();
        println!("Time taken: {:?}", end.duration_since(start));
        Ok(sig)
    }

    /// NextBlock.io
    /// Accuracy: n+1/n+2
    pub async fn spray_with_nextblock(
        &self,
        mut ixs: Vec<Instruction>,
        payer: &Keypair,
        fee: u64,
        compute_budget: u32,
        clients: &Vec<NextBlock>,
    ) -> anyhow::Result<Signature> {
        let start = std::time::Instant::now();
    
        // CU budget + price
        ixs.insert(0, ix_set_compute_unit_price(fee));
        ixs.insert(1, ix_set_compute_unit_limit(compute_budget));
    
        let tip_acc = clients[0].get_tip_account();
        let tip_ix = solana_program::system_instruction::transfer(
            &payer.pubkey(), 
            &tip_acc, 
            1_000_000
        );
        ixs.insert(2, tip_ix);
    
        let blockhash = self.rpc_client.get_latest_blockhash_with_commitment(CommitmentConfig::processed()).await?.0;
        let slot0 = self.rpc_client.get_slot_with_commitment(CommitmentConfig::processed()).await?;
        let msg = V0Message::try_compile(&payer.pubkey(), &ixs, &[], blockhash)?;
        let tx  = VersionedTransaction::try_new(VersionedMessage::V0(msg), &[payer])?;
        let sig = tx.signatures[0];
    
        let wire = bincode::serialize(&tx)?;
        let b64  = B64.encode(wire);
    
        let per_call   = std::time::Duration::from_millis(250);
        let overall_nb = std::time::Duration::from_millis(350);
    
        let reqs: Vec<_> = clients
            .iter()
            .map(|c| c.send_transaction(&b64)) 
            .collect();
    
        let nb_fut = async {
            let futures = reqs.into_iter().map(|rb| tokio::time::timeout(per_call, rb.send()));
            let all = futures::future::join_all(futures);
            let _ = tokio::time::timeout(overall_nb, all).await;
        };
    
        let rpc_fut = tokio::time::timeout(
            std::time::Duration::from_millis(200),
            self.rpc_client.send_transaction_with_config(
                &tx,
                RpcSendTransactionConfig {
                    skip_preflight: true,
                    preflight_commitment: None,
                    encoding: None,
                    max_retries: Some(0),
                    min_context_slot: None,
                },
            ),
        );
    
        let _ = tokio::join!(nb_fut, rpc_fut);
    
        // Fast confirm loop
        let mut tries = 0u32;
        while !self.rpc_client
            .confirm_transaction_with_commitment(&sig, CommitmentConfig::confirmed())
            .await?
            .value
        {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            tries += 1;
            if tries > 300 {
                anyhow::bail!("NB spray: transaction timed out https://solscan.io/tx/{}", sig);
            }
        }
    
        let slot1 = self.rpc_client.get_slot_with_commitment(CommitmentConfig::processed()).await?;
        println!("Blocks diff: n+{}", slot1.saturating_sub(slot0));
        println!("Time taken: {:?}", start.elapsed());
        Ok(sig)
    }

    pub async fn spray_with_helius(
        &self,
        mut ixs: Vec<Instruction>,
        payer: &Keypair,
        fee: u64,
        compute_budget: u32,
        clients: &Vec<HeliusSender>,
    ) -> anyhow::Result<Signature> {
        let start = std::time::Instant::now();
    
        ixs.insert(0, ix_set_compute_unit_price(fee));
        ixs.insert(1, ix_set_compute_unit_limit(compute_budget));
        
        let tip_acc = clients[0].get_tip_account();
        let tip_ix = solana_program::system_instruction::transfer(
            &payer.pubkey(), 
            &tip_acc, 
            500_000
        );
        ixs.insert(2, tip_ix);
        let blockhash = self.rpc_client.get_latest_blockhash_with_commitment(CommitmentConfig::processed()).await?.0;
        let slot0 = self.rpc_client.get_slot_with_commitment(CommitmentConfig::processed()).await?;
        let msg = V0Message::try_compile(&payer.pubkey(), &ixs, &[], blockhash)?;
        let tx  = VersionedTransaction::try_new(VersionedMessage::V0(msg), &[payer])?;
        let sig = tx.signatures[0];
    
        let wire = bincode::serialize(&tx)?;
        let b64  = B64.encode(wire);
    
        let per_call   = std::time::Duration::from_millis(250);
        let overall_nb = std::time::Duration::from_millis(350);
    
        let reqs: Vec<_> = clients
            .iter()
            .map(|c| c.send_transaction(&b64, true)) 
            .collect();
    
        let hl_fut = async {
            let futures = reqs.into_iter().map(|rb| tokio::time::timeout(per_call, rb.send()));
            let all = futures::future::join_all(futures);
            let _ = tokio::time::timeout(overall_nb, all).await;
        };
    
        let rpc_fut = tokio::time::timeout(
            std::time::Duration::from_millis(200),
            self.rpc_client.send_transaction_with_config(
                &tx,
                RpcSendTransactionConfig {
                    skip_preflight: true,
                    preflight_commitment: None,
                    encoding: None,
                    max_retries: Some(0),
                    min_context_slot: None,
                },
            ),
        );

        let _ = tokio::join!(hl_fut, rpc_fut);
    
        let mut tries = 0u32;
        while !self.rpc_client
            .confirm_transaction_with_commitment(&sig, CommitmentConfig::confirmed())
            .await?
            .value
        {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            tries += 1;
            if tries > 300 {
                anyhow::bail!("NB spray: transaction timed out https://solscan.io/tx/{}", sig);
            }
        }
    
        let slot1 = self.rpc_client.get_slot_with_commitment(CommitmentConfig::processed()).await?;
        println!("Blocks diff: n+{}", slot1.saturating_sub(slot0));
        println!("Time taken: {:?}", start.elapsed());
        Ok(sig)
    }

    pub async fn spray_with_zero_slot(
        &self,
        mut ixs: Vec<Instruction>,
        payer: &Keypair,
        fee: u64,
        compute_budget: u32,
        clients: &Vec<ZeroSlot>,
    ) -> anyhow::Result<Signature> {
        let start = std::time::Instant::now();

        let tip_acc = clients[0].get_tip_account();
        let tip_ix = solana_program::system_instruction::transfer(
            &payer.pubkey(), 
            &tip_acc, 
            3_200_000
        );
        ixs.insert(0, tip_ix);

        ixs.insert(1, ix_set_compute_unit_price(fee));
        ixs.insert(2, ix_set_compute_unit_limit(compute_budget));
        
        let blockhash = self.rpc_client.get_latest_blockhash_with_commitment(CommitmentConfig::processed()).await?.0;
        let slot0 = self.rpc_client.get_slot_with_commitment(CommitmentConfig::processed()).await?;
        let msg = V0Message::try_compile(&payer.pubkey(), &ixs, &[], blockhash)?;
        let tx  = VersionedTransaction::try_new(VersionedMessage::V0(msg), &[payer])?;
        let sig = tx.signatures[0];
    
        let wire = bincode::serialize(&tx)?;
        let b64  = B64.encode(wire);
    
        let per_call   = std::time::Duration::from_millis(250);
        let overall_nb = std::time::Duration::from_millis(350);
    
        let reqs: Vec<_> = clients
            .iter()
            .map(|c| c.send_transaction(&b64)) 
            .collect();
    
        let zs_fut = async {
            let futures = reqs.into_iter().map(|rb| tokio::time::timeout(per_call, rb.send()));
            let all = futures::future::join_all(futures);
            let _ = tokio::time::timeout(overall_nb, all).await;
        };
    
        let _ = tokio::join!(zs_fut);
    
        let mut tries = 0u32;
        while !self.rpc_client
            .confirm_transaction_with_commitment(&sig, CommitmentConfig::confirmed())
            .await?
            .value
        {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            tries += 1;
            if tries > 300 {
                anyhow::bail!("ZeroSlot spray: transaction timed out https://solscan.io/tx/{}", sig);
            }
        }
    
        let slot1 = self.rpc_client.get_slot_with_commitment(CommitmentConfig::processed()).await?;
        println!("Blocks diff: n+{}", slot1.saturating_sub(slot0));
        println!("Time taken: {:?}", start.elapsed());
        Ok(sig)
    }

    pub async fn spray_with_temporal(
        &self,
        mut ixs: Vec<Instruction>,
        payer: &Keypair,
        fee: u64,
        compute_budget: u32,
        clients: &Vec<TemporalSender>,
        tip_lamports: u64,
    ) -> anyhow::Result<Signature> {
        let start = std::time::Instant::now();
        ixs.insert(1, ix_set_compute_unit_price(fee));
        ixs.insert(2, ix_set_compute_unit_limit(compute_budget));

        let tip_acc = clients[0].get_tip_account();
        let tip_ix = solana_program::system_instruction::transfer(
            &payer.pubkey(),
            &tip_acc,
            tip_lamports,
        );
        ixs.insert(0, tip_ix);

        let blockhash = self
            .rpc_client
            .get_latest_blockhash_with_commitment(CommitmentConfig::processed())
            .await?
            .0;

        let slot0 = self
            .rpc_client
            .get_slot_with_commitment(CommitmentConfig::processed())
            .await?;

        let msg = V0Message::try_compile(&payer.pubkey(), &ixs, &[], blockhash)?;
        let tx  = VersionedTransaction::try_new(VersionedMessage::V0(msg), &[payer])?;
        let sig = tx.signatures[0];

        let wire = bincode::serialize(&tx)?;
        let b64  = B64.encode(wire);
        let per_call   = std::time::Duration::from_millis(250);
        let overall_to = std::time::Duration::from_millis(350);

        let reqs: Vec<_> = clients.iter().map(|c| c.send_transaction(&b64)).collect();
        let tmp_fut = async {
            let futures = reqs.into_iter().map(|rb| tokio::time::timeout(per_call, rb.send()));
            let all = futures::future::join_all(futures);
            let _ = tokio::time::timeout(overall_to, all).await;
        };

        let _ = tokio::join!(tmp_fut);

        let mut tries = 0u32;
        while !self
            .rpc_client
            .confirm_transaction_with_commitment(&sig, CommitmentConfig::confirmed())
            .await?
            .value
        {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            tries += 1;
            if tries > 300 {
                anyhow::bail!("Temporal spray: transaction timed out https://solscan.io/tx/{}", sig);
            }
        }

        let slot1 = self
            .rpc_client
            .get_slot_with_commitment(CommitmentConfig::processed())
            .await?;
        println!("Blocks diff: n+{}", slot1.saturating_sub(slot0));
        println!("Time taken: {:?}", start.elapsed());
        Ok(sig)
    }

    pub async fn spray_with_blox(
        &self,
        mut ixs: Vec<Instruction>,
        payer: &Keypair,
        fee: u64,
        compute_budget: u32,
        clients: &Vec<Bloxroute>,
        tip_lamports: u64,
    ) -> anyhow::Result<Signature> {
        let start = std::time::Instant::now();
        ixs.insert(1, ix_set_compute_unit_price(fee));
        ixs.insert(2, ix_set_compute_unit_limit(compute_budget));

        let tip_acc = clients[0].get_tip_acc();
        let tip_ix = solana_program::system_instruction::transfer(
            &payer.pubkey(),
            &tip_acc,
            tip_lamports,
        );
        ixs.insert(0, tip_ix);

        let blockhash = self
            .rpc_client
            .get_latest_blockhash_with_commitment(CommitmentConfig::processed())
            .await?
            .0;

        let slot0 = self
            .rpc_client
            .get_slot_with_commitment(CommitmentConfig::processed())
            .await?;

        let msg = V0Message::try_compile(&payer.pubkey(), &ixs, &[], blockhash)?;
        let tx  = VersionedTransaction::try_new(VersionedMessage::V0(msg), &[payer])?;
        let sig = tx.signatures[0];

        let wire = bincode::serialize(&tx)?;
        let b64  = B64.encode(wire);

        let reqs: Vec<_> = clients.iter().map(|c| c.submit(&b64, &SubmitOpts {
            skip_preflight: Some(true),
            front_running_protection: Some(false),
            submit_protection: Some(SubmitProtection::Low),
            fast_best_effort: Some(false),
            use_staked_rpcs: Some(true),
            allow_back_run: Some(false),
            revenue_address: Some(String::new()),
        })).collect();
        self.spawn_forget(reqs).await;

        let mut tries = 0u32;
        while !self
            .rpc_client
            .confirm_transaction_with_commitment(&sig, CommitmentConfig::confirmed())
            .await?
            .value
        {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            tries += 1;
            if tries > 300 {
                anyhow::bail!("Blox spray: transaction timed out https://solscan.io/tx/{}", sig);
            }
        }

        let slot1 = self
            .rpc_client
            .get_slot_with_commitment(CommitmentConfig::processed())
            .await?;
        println!("Blocks diff: n+{}", slot1.saturating_sub(slot0));
        println!("Time taken: {:?}", start.elapsed());
        Ok(sig)
    }

    pub async fn spray_with_all(
        &self,
        mut ixs: Vec<Instruction>,
        payer: &Keypair,
        fee: u64,
        nb_clients: &[NextBlock],
        jito_clients: &[JitoClient],
        helius_clients: &[HeliusSender],
        zs_clients: &[ZeroSlot],
        temporal_clients: &[TemporalSender],
        blox_clients: &[Bloxroute],
        tip_lamports: u64,
        slot0: Option<u64>,
        nonce_account: Option<Pubkey>,
    ) -> anyhow::Result<Signature> {
        let start = std::time::Instant::now();

        let (nonce_hash, _) = match nonce_account {
            Some(nonce_account) => self.get_nonce_data(&nonce_account).await?,
            None => (
                self.rpc_client
                    .get_latest_blockhash_with_commitment(CommitmentConfig::processed())
                    .await?
                    .0,
                Pubkey::default(),
            ),
        };
        if nonce_account.is_some() {
            ixs.insert(
                0,
                system_instruction::advance_nonce_account(&nonce_account.unwrap(), &payer.pubkey()),
            );
        }

        let blockhash = nonce_hash;
        let slot0 = match slot0 {
            Some(slot) => slot,
            None => {
                let slot = self
                    .rpc_client
                    .get_slot_with_commitment(CommitmentConfig::processed())
                    .await?;
                slot
            }
        };

        let mut ixs_hl = ixs.clone();
        ixs_hl.insert(1, ix_set_compute_unit_price(fee * rand::random_range(0.7..1.3) as u64));
        let hl_tip_acc = helius_clients[rand::random_range(0..helius_clients.len())].get_tip_account();
        ixs_hl.push(solana_program::system_instruction::transfer(&payer.pubkey(), &hl_tip_acc, tip_lamports));
        let hl_msg = V0Message::try_compile(&payer.pubkey(), &ixs_hl, &[], blockhash)?;
        let hl_tx  = VersionedTransaction::try_new(VersionedMessage::V0(hl_msg), &[payer])?;
        let hl_sig = hl_tx.signatures[0];
        let hl_b64 = {
            let wire = bincode::serialize(&hl_tx)?;
            B64.encode(wire)
        };

        let mut ixs_zs = ixs.clone();
        ixs_zs.insert(1, ix_set_compute_unit_price(fee * rand::random_range(0.7..1.3) as u64));
        let zs_tip_acc = zs_clients[rand::random_range(0..zs_clients.len())].get_tip_account();
        ixs_zs.insert(2, solana_program::system_instruction::transfer(&payer.pubkey(), &zs_tip_acc, tip_lamports));
        let zs_msg = V0Message::try_compile(&payer.pubkey(), &ixs_zs, &[], blockhash)?;
        let zs_tx  = VersionedTransaction::try_new(VersionedMessage::V0(zs_msg), &[payer])?;
        let zs_sig = zs_tx.signatures[0];
        let zs_b64 = { let wire = bincode::serialize(&zs_tx)?; B64.encode(wire) };
    
        let mut ixs_jito = ixs.clone();
        ixs_jito.insert(1, ix_set_compute_unit_price(fee * rand::random_range(0.7..1.3) as u64));
        let jito_rta = jito_clients[0].get_tip_account();
        ixs_jito.push(solana_program::system_instruction::transfer(&payer.pubkey(), &jito_rta, tip_lamports));
        let jito_msg = V0Message::try_compile(&payer.pubkey(), &ixs_jito, &[], blockhash)?;
        let jito_tx  = VersionedTransaction::try_new(VersionedMessage::V0(jito_msg), &[payer])?;
        let jito_sig = jito_tx.signatures[0];
        let jito_b64 = { let wire = bincode::serialize(&jito_tx)?; B64.encode(wire) };
    
        let mut ixs_nb = ixs.clone();
        ixs_nb.insert(1, ix_set_compute_unit_price(fee * rand::random_range(0.7..1.3) as u64));
        let nb_tip_acc = nb_clients[rand::random_range(0..nb_clients.len())].get_tip_account();
        ixs_nb.push(solana_program::system_instruction::transfer(&payer.pubkey(), &nb_tip_acc, tip_lamports));
        let nb_msg = V0Message::try_compile(&payer.pubkey(), &ixs_nb, &[], blockhash)?;
        let nb_tx  = VersionedTransaction::try_new(VersionedMessage::V0(nb_msg), &[payer])?;
        let nb_sig = nb_tx.signatures[0];
        let nb_b64 = { let wire = bincode::serialize(&nb_tx)?; B64.encode(wire) };
    
        let mut ixs_tmp = ixs.clone();
        ixs_tmp.insert(1, ix_set_compute_unit_price(fee * rand::random_range(0.7..1.3) as u64));
        let tmp_tip_acc = temporal_clients[rand::random_range(0..temporal_clients.len())].get_tip_account();
        ixs_tmp.push(solana_program::system_instruction::transfer(&payer.pubkey(), &tmp_tip_acc, tip_lamports)); // 0.001 SOL
        let tmp_msg = V0Message::try_compile(&payer.pubkey(), &ixs_tmp, &[], blockhash)?;
        let tmp_tx  = VersionedTransaction::try_new(VersionedMessage::V0(tmp_msg), &[payer])?;
        let tmp_sig = tmp_tx.signatures[0];
        let tmp_b64 = { let wire = bincode::serialize(&tmp_tx)?; B64.encode(wire) };

        let mut ixs_bx = ixs.clone();
        ixs_bx.insert(1, ix_set_compute_unit_price(fee * rand::random_range(0.7..1.3) as u64));
        let bx_tip_acc = blox_clients[rand::random_range(0..blox_clients.len())].get_tip_acc();
        ixs_bx.push(solana_program::system_instruction::transfer(&payer.pubkey(), &bx_tip_acc, tip_lamports));
        let bx_msg = V0Message::try_compile(&payer.pubkey(), &ixs_bx, &[], blockhash)?;
        let bx_tx  = VersionedTransaction::try_new(VersionedMessage::V0(bx_msg), &[payer])?;
        let bx_sig = bx_tx.signatures[0];
        let bx_b64 = { let wire = bincode::serialize(&bx_tx)?; B64.encode(wire) };

        let bx_fut = async {
            let reqs: Vec<_> = blox_clients.iter().map(|c| c.submit(&bx_b64, &SubmitOpts {
                skip_preflight: Some(true),
                front_running_protection: Some(false),
                submit_protection: Some(SubmitProtection::Low),
                fast_best_effort: Some(false),
                use_staked_rpcs: Some(true),
                allow_back_run: Some(false),
                revenue_address: Some(String::new()),
            })).collect();
            self.spawn_forget(reqs).await;
        };


        let hl_fut = async { 
            let reqs: Vec<_> = helius_clients.iter().map(|c| c.send_transaction(&hl_b64, false)).collect(); 
            self.spawn_forget(reqs).await;
        };

        let zs_fut = async {
            let reqs: Vec<_> = zs_clients.iter().map(|c| c.send_transaction(&zs_b64)).collect();
            self.spawn_forget(reqs).await;
        };

        let be_fut = async {
            let reqs: Vec<_> = jito_clients.iter().map(|c| c.send_transaction(&jito_b64, true)).collect();
            self.spawn_forget(reqs).await;
        };

        let nb_fut = async {
            let reqs: Vec<_> = nb_clients.iter().map(|c| c.send_transaction(&nb_b64)).collect();
            self.spawn_forget(reqs).await;
        };
    
        let tmp_fut = async {
            let reqs: Vec<_> = temporal_clients.iter().map(|c| c.send_transaction(&tmp_b64)).collect();
            self.spawn_forget(reqs).await;
        };

        let _ = tokio::join!(zs_fut, tmp_fut, hl_fut, nb_fut, be_fut, bx_fut);
        log!(cc::LIGHT_WHITE, "Time to send: {:?}", start.elapsed());
        log!(cc::LIGHT_WHITE, "ZeroSlot sig:  https://solscan.io/tx/{}", zs_sig);
        log!(cc::LIGHT_WHITE, "Jito sig:      https://solscan.io/tx/{}", jito_sig);
        log!(cc::LIGHT_WHITE, "NextBlock sig: https://solscan.io/tx/{}", nb_sig);
        log!(cc::LIGHT_WHITE, "Helius sig:    https://solscan.io/tx/{}", hl_sig);
        log!(cc::LIGHT_WHITE, "Temporal sig:  https://solscan.io/tx/{}", tmp_sig);
        log!(cc::LIGHT_WHITE, "Blox sig:      https://solscan.io/tx/{}", bx_sig);
        let mut tries = 0u32;
        let winner = loop {
            if self.rpc_client
                .confirm_transaction_with_commitment(&jito_sig, CommitmentConfig::confirmed())
                .await?
                .value
            {
                break jito_sig;
            }
            if self.rpc_client
                .confirm_transaction_with_commitment(&nb_sig, CommitmentConfig::confirmed())
                .await?
                .value
            {
                break nb_sig;
            }
            if self.rpc_client
                .confirm_transaction_with_commitment(&zs_sig, CommitmentConfig::confirmed())
                .await?
                .value
            {
                break zs_sig;
            }
            if self.rpc_client
                .confirm_transaction_with_commitment(&hl_sig, CommitmentConfig::confirmed())
                .await?
                .value
            {
                break hl_sig;
            }
            if self.rpc_client
                .confirm_transaction_with_commitment(&tmp_sig, CommitmentConfig::confirmed())
                .await?
                .value
            {
                break tmp_sig;
            }
            if self.rpc_client
                .confirm_transaction_with_commitment(&bx_sig, CommitmentConfig::confirmed())
                .await?
                .value
            {
                break bx_sig;
            }
            tries += 1;
            if tries > 300 {
                anyhow::bail!(
                    "spray_with_all timeout; jito={}, nb={}, zs={}",
                    jito_sig, nb_sig, zs_sig
                );
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        };
    
        let slot1 = self
            .rpc_client
            .get_slot_with_commitment(CommitmentConfig::processed())
            .await?;
        log!("Blocks diff: n+{}", slot1.saturating_sub(slot0));
        log!("Time taken: {:?}", start.elapsed());
    
        Ok(winner)
    }

    pub async fn spawn_forget(&self, reqs: Vec<reqwest::RequestBuilder>) {
        for rb in reqs {
            tokio::spawn(async move {
                let _ = rb.send().await;
            });
        }
        tokio::task::yield_now().await;
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*, crate::mew::{config::config, swqos::{blox::BLOX_ENDPOINTS, helius::{HeliusSender, HELIUS_SENDER_ENDPOINTS}, jito::get_jito_clients, nextblock::{NextBlock, NB_ENDPOINTS}, temporal::{TemporalSender, TEMPORAL_HTTP_ENDPOINTS}, zero_slot::{ZeroSlot, ZERO_SLOT_ENDPOINTS}}, writing::{cc, Colors}}, std::{io::{self}, str::FromStr, time::Instant}, 
        crate::mew::sol_hook::{pump_fun::{PumpFunEvent, PumpFun}, pump_swap::{PumpSwapEvent, PumpSwap}, vacation::Vacation},
        yellowstone_grpc_proto::prelude::{TransactionStatusMeta, SubscribeUpdateTransactionInfo},
        yellowstone_grpc_proto::prelude::subscribe_update::UpdateOneof,
        tokio, urlencoding::decode as url_decode
    };

    #[tokio::test]
    async fn test_exists() {
        let rpc = "https://api.mainnet-beta.solana.com".to_string();
        let sol = SolHook::new(rpc);
        let exists = sol.exists(&Pubkey::try_from("FL4CKfetEWBMXFs15ZAz4peyGbCuzVaoszRKcoVt3WfC").unwrap()).await.unwrap_or(false);
        println!("Exists: {}", exists);
    }

    #[tokio::test]
    async fn test_get_balance() {
        let rpc = "https://api.mainnet-beta.solana.com".to_string();
        let sol = SolHook::new(rpc);
        let balance = sol.get_balance(&Pubkey::try_from("FL4CKfetEWBMXFs15ZAz4peyGbCuzVaoszRKcoVt3WfC").unwrap()).await;
        println!("Balance: {}", balance.unwrap());
    }

    #[tokio::test]
    async fn test_get_token_program_id() {
        let rpc = "https://api.mainnet-beta.solana.com".to_string();
        let sol = SolHook::new(rpc);
        let token_program_id = sol.get_token_program_id(&WSOL_MINT).await.unwrap();
        println!("Token program id: {:?}", token_program_id);
    }

    #[tokio::test]
    async fn test_get_token_balance() {
        let rpc = "https://api.mainnet-beta.solana.com".to_string();
        let sol = SolHook::new(rpc);
        let start = Instant::now();

        let token_balance = match sol.get_token_balance(&Pubkey::try_from("Esj1iA4Gbrt5rRCQVv5x7wAtr1NXZ4smWBp7FYmHasa7").unwrap(), &USDC_MINT).await {
            Ok(balance) => balance,
            Err(e) => {
                println!("Error getting token balance: {:?}", e);
                return;
            }
        };
        println!("Token balance: {}", token_balance);
        let duration = start.elapsed();
        println!("Time taken: {:?}", duration);
    }

    #[tokio::test]
    async fn test_get_token_metadata() {
        let rpc = "https://api.mainnet-beta.solana.com".to_string();
        let sol = SolHook::new(rpc);
        let (metadata, pda) = sol.get_token_metadata(&Pubkey::from_str("FKugW5fK2g3b2rm79TiL7GTKMJKE25xG9LYbUPnpump").unwrap()).await.unwrap();
        println!("Metadata: {:?}", metadata);
        println!("PDA: {:?}", pda);
    }

    #[tokio::test]
    async fn test_get_token_info() {
        let rpc = "https://api.mainnet-beta.solana.com".to_string();
        let sol = SolHook::new(rpc);
        let token_info = sol.get_token_info(&Pubkey::from_str("6entYcGxY1n2XkpZtTM62nreoS4rPByJSkoKHQjwruMg").unwrap()).await.unwrap();
        println!("Token info: {:?}", token_info);
    }

    #[tokio::test]
    async fn test_subscribe_logs() {
        let ws = String::from("wss://api.apewise.org/ws?api-key=");
        let rpc = String::from("https://api.mainnet-beta.solana.com");
        let sol = SolHook::new(rpc.clone());
        let (mut rx, handle) = sol.subscribe_logs_channel(&ws, RpcTransactionLogsFilter::Mentions(vec![Pubkey::from_str("6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P").unwrap().to_string()]), CommitmentConfig::processed()).await.unwrap();
        while let Some(msg) = rx.recv().await {
            println!("Message: {:?}", msg);
        }
        handle.abort();
        tokio::time::sleep(std::time::Duration::from_secs_f32(1.0)).await;
    }

    #[tokio::test]
    // Pump.fun Test
    async fn test_pump_fun() {
        let ws = String::from("wss://api.apewise.org/ws?api-key=");
        let rpc = String::from("https://api.mainnet-beta.solana.com");
        let sol = SolHook::new(rpc.clone());
        let (mut rx, handle) = sol.subscribe_logs_channel(&ws, RpcTransactionLogsFilter::Mentions(vec![Pubkey::from_str("6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P").unwrap().to_string()]), CommitmentConfig::processed()).await.unwrap();
        while let Some(msg) = rx.recv().await {
            if msg.err != None {
                println!("Error: {:?}", msg.err);
                continue;
            }
            // println!("Message: {:?}", msg);
            let lines: std::slice::Iter<'_, String> = msg.logs
             .iter();

            let events = PumpFun::parse_logs(lines, None);
            for event in events {
                match event {
                    PumpFunEvent::Trade(Some(trade)) => {
                        println!("Trade: {:?}", trade);
                        println!("{}Price: {:#.10}", cc::BOLD, PumpFun::get_price(&trade));
                    }
                    PumpFunEvent::Create(Some(create)) => {
                        println!("Create: {:?}", create);
                    }
                    _ => {}
                }
            }
            tokio::time::sleep(std::time::Duration::from_secs_f32(1.0)).await;
        }
        handle.abort();
        tokio::time::sleep(std::time::Duration::from_secs_f32(1.0)).await;
    }

    #[tokio::test]
    async fn test_grpc_pump() {
        let config = config();
        let grpc_url   = config.grpc_url.clone().unwrap();
        let grpc_token = config.grpc_token.clone().unwrap();
    
        let mut grpc_client = SolHook::grpc_connect(&grpc_url, Some(&grpc_token)).await.unwrap();
        let pump_address = Pubkey::from_str("6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P").unwrap();
        let accounts = vec![pump_address];
        let mut stream = SolHook::grpc_subscribe_transactions(&mut grpc_client, &accounts).await.unwrap();
    
        while let Some(msg) = stream.next().await {
            let update = match msg {
                Ok(u) => u,
                Err(e) => { warn!("grpc error: {e}"); continue; }
            };
    
            let Some(UpdateOneof::Transaction(tx_update)) = update.update_oneof else { continue };
    
            let Some(SubscribeUpdateTransactionInfo {
                meta: Some(TransactionStatusMeta { log_messages, .. }),
                ..
            }) = tx_update.transaction else { continue };
    
            let events = PumpFun::parse_logs(log_messages.iter(), None);
            for event in events {
                match event {
                    PumpFunEvent::Trade(Some(trade)) => {
                        log!(cc::LIGHT_WHITE, "Trade: {:?}", trade);
                        log!(cc::LIGHT_WHITE, "Price: {:#.10}", PumpFun::get_price(&trade));
                    }
                    PumpFunEvent::Create(Some(create)) => {
                        log!(cc::LIGHT_WHITE, "Create: {:?}", create);
                    }
                    _ => {}
                }
            }
        }
    }

    #[tokio::test]
    async fn test_grpc_pump_swap() {
        let config = config();
        let grpc_url   = config.grpc_url.clone().unwrap();
        let grpc_token = config.grpc_token.clone().unwrap();
    
        let mut grpc_client = SolHook::grpc_connect(&grpc_url, Some(&grpc_token)).await.unwrap();
        let pump_address = Pubkey::from_str("pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA").unwrap();
        let global_config: Pubkey = Pubkey::from_str("ADyA8hdefvWN2dbGGWFotbzWxrAvLW83WG6QCVXvJKqw").unwrap();
        let accounts = vec![pump_address, global_config];
        let mut stream = SolHook::grpc_subscribe_transactions(&mut grpc_client, &accounts).await.unwrap();
    
        while let Some(msg) = stream.next().await {
            let update = match msg {
                Ok(u) => u,
                Err(e) => { warn!("grpc error: {e}"); continue; }
            };
    
            let Some(UpdateOneof::Transaction(tx_update)) = update.update_oneof else { continue };
            
            let Some(SubscribeUpdateTransactionInfo {
                meta: Some(TransactionStatusMeta { log_messages, .. }),
                ..
            }) = tx_update.transaction.as_ref() else { continue };
    
            let events = PumpSwap::parse_logs(log_messages.iter(), None);
            let sig_raw = tx_update.transaction.as_ref().unwrap().signature.clone();
            let sig = Signature::try_from(sig_raw).unwrap();
            for event in events {
                match event {
                    PumpSwapEvent::Buy(Some(buy)) => {
                        println!("Sig: {:?}, Buy: {:?}", sig.to_string(), buy);
                    }
                    PumpSwapEvent::Sell(Some(sell)) => {
                        println!("Sig: {:?}, Sell: {:?}", sig.to_string(), sell);
                    }
                    PumpSwapEvent::CreatePool(Some(create)) => {
                        println!("Sig: {:?}, Create pool: {:?}", sig.to_string(), create);
                    }
                    _ => {}
                }
            }
        }
    }

    #[tokio::test]
    async fn test_pump_fetch_state() {
        let rpc: String = String::from("https://api.apewise.org/rpc?api-key=");
        let sol = SolHook::new(rpc.clone());
        let keypair = Keypair::from_base58_string("");
        let pump = PumpFun::new(Arc::new(keypair.insecure_clone()), Arc::new(sol.clone()));
        let bonding_curve = Pubkey::from_str("usAdAyXTZHo8U5JH4ykNi2Cr6fbWyFYXkkhKqFeSXA6").unwrap();
        let (state, price) = pump.fetch_price(&bonding_curve).await.unwrap();
        println!("State: {:?}", state.complete);
        println!("Price: {}", price);
    }

    #[tokio::test]
    async fn test_pump_buy() {
        let config = config();
        let mint = Pubkey::from_str("AhMLyXcAzkpHbfH2AxMEdiUCzSmsdtbWSvh953Y9pump").unwrap();
        let bonding_curve = Pubkey::from_str("usAdAyXTZHo8U5JH4ykNi2Cr6fbWyFYXkkhKqFeSXA6").unwrap();
        let sol = SolHook::new(config.rpc_url.clone());

        let keypair = Keypair::from_base58_string("");
        let pump = PumpFun::new(Arc::new(keypair.insecure_clone()), Arc::new(sol.clone()));
        let (state, price) = pump.fetch_price(&bonding_curve).await.unwrap();

        let (ixs, recent_fees) = pump.buy(
            &mint,
            &bonding_curve,
            &state.creator,
            0.001,
            1.3,
            price,
            Some(true),
        ).await.unwrap();
        let sig = sol.send(ixs, &keypair, recent_fees, None).await.unwrap();
        println!("Sig: https://solscan.io/tx/{}", sig);
    }

    #[tokio::test]
    async fn test_pump_swap_buy() {
        let config = config();
        let mint = Pubkey::from_str("VF7cfdL5L8KKDKLd6QpRmFnMdq1Y3gJ1hAA6Zxspump").unwrap();
        let pool = Pubkey::from_str("Hiyw67D7pNxtpto2D5ofV8F2w2RZrMuMxkF2zBhoTkZp").unwrap();
        let sol = SolHook::new(config.rpc_url.clone());

        let keypair = Keypair::from_base58_string("");
        let pump = PumpSwap::new(Arc::new(keypair.insecure_clone()), Arc::new(sol.clone()));
        let (state, price) = pump.fetch_price(&pool).await.unwrap();
        log!("Creator: {:?} | Price: {}", state.coin_creator, price);

        let (ixs, recent_fees) = pump.buy(
            &mint,
            &pool,
            &state.coin_creator,
            0.001,
            1.3,
            price,
            Some(false),
        ).await.unwrap();
        let sig = sol.send(ixs, &keypair, recent_fees, Some(250_000)).await.unwrap();
        println!("Sig: https://solscan.io/tx/{}", sig);
    }

    #[tokio::test]
    async fn test_pump_swap_sell() {
        let config = config();
        let mint = Pubkey::from_str("VF7cfdL5L8KKDKLd6QpRmFnMdq1Y3gJ1hAA6Zxspump").unwrap();
        let pool = Pubkey::from_str("Hiyw67D7pNxtpto2D5ofV8F2w2RZrMuMxkF2zBhoTkZp").unwrap();
        let sol = SolHook::new(config.rpc_url.clone());
        let keypair = Keypair::from_base58_string("");
        let pump = PumpSwap::new(Arc::new(keypair.insecure_clone()), Arc::new(sol.clone()));
        let (state, price) = pump.fetch_price(&pool).await.unwrap();
        let (ixs, _recent_fees) = pump.sell(
            &mint,
            &pool,
            &state.coin_creator,
            100,
            1.3,
            price,
        ).await.unwrap();
        let sig = sol.send(ixs, &keypair, 5_000_0, Some(250_000)).await.unwrap();
        println!("Sig: https://solscan.io/tx/{}", sig);
    }

    #[tokio::test]
    async fn test_pump_sell() {
        let config = config();
        let mint = Pubkey::from_str("AhMLyXcAzkpHbfH2AxMEdiUCzSmsdtbWSvh953Y9pump").unwrap();
        let bonding_curve = Pubkey::from_str("usAdAyXTZHo8U5JH4ykNi2Cr6fbWyFYXkkhKqFeSXA6").unwrap();
        let sol = SolHook::new(config.rpc_url.clone());

        let keypair = Keypair::from_base58_string("");
        let pump = PumpFun::new(Arc::new(keypair.insecure_clone()), Arc::new(sol.clone()));
        let (state, price) = pump.fetch_price(&bonding_curve).await.unwrap();

        let (mut ixs, recent_fees) = pump.sell(
            &mint,
            &bonding_curve,
            &state.creator,
            100,
            1.5,
            price,
        ).await.unwrap();

        let (close_ix, _) = sol.close_ata(&mint, &keypair, true).await.unwrap();
        ixs.push(close_ix);

        let sig = sol.send(ixs, &keypair, recent_fees, None).await.unwrap();
        println!("Sig: https://solscan.io/tx/{}", sig);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_pump_buy_jito() {
        let config = config();
        let mint = Pubkey::from_str("AhMLyXcAzkpHbfH2AxMEdiUCzSmsdtbWSvh953Y9pump").unwrap();
        let bonding_curve = Pubkey::from_str("usAdAyXTZHo8U5JH4ykNi2Cr6fbWyFYXkkhKqFeSXA6").unwrap();
        let sol = SolHook::new(config.rpc_url.clone());
        let keypair = Keypair::from_base58_string("");
        let pump = PumpFun::new(Arc::new(keypair.insecure_clone()), Arc::new(sol.clone()));
        let (state, price) = pump.fetch_price(&bonding_curve).await.unwrap();
        let jito_client = &get_jito_clients()[0];

        // send via TPU; confirm via async RPC (confirmed)
        let (ixs, recent_fees) = pump.buy(
            &mint,
            &bonding_curve,
            &state.creator,
            0.001,
            1.1,
            price,
            Some(true),
        ).await.unwrap();

        let sig = sol.send_with_jito(&jito_client, ixs, &keypair, recent_fees, 70_000).await.unwrap();
        println!("Sig: https://solscan.io/tx/{}", sig);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_pump_buy_jito_bundle() {
        let config = config();
        let mint = Pubkey::from_str("AhMLyXcAzkpHbfH2AxMEdiUCzSmsdtbWSvh953Y9pump").unwrap();
        let bonding_curve = Pubkey::from_str("usAdAyXTZHo8U5JH4ykNi2Cr6fbWyFYXkkhKqFeSXA6").unwrap();
        let sol = SolHook::new(config.rpc_url.clone());
        let keypair = Keypair::from_base58_string("");
        let pump = PumpFun::new(Arc::new(keypair.insecure_clone()), Arc::new(sol.clone()));
        let (state, price) = pump.fetch_price(&bonding_curve).await.unwrap();
        let jito_clients = &get_jito_clients();

        let (ixs, recent_fees) = pump.buy(
            &mint,
            &bonding_curve,
            &state.creator,
            0.001,
            1.1,
            price,
            Some(true),
        ).await.unwrap();

        let sig = sol.spray_with_jito_bundle(ixs, &keypair, recent_fees, 70_000, &jito_clients, 1_000_000).await.unwrap();
        println!("Sig: https://solscan.io/tx/{}", sig);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_pump_buy_nextblock() {
        let config = config();
        let clients = &NextBlock::multiple_new(&NB_ENDPOINTS, url_decode("").unwrap().to_string());
        let mint = Pubkey::from_str("AhMLyXcAzkpHbfH2AxMEdiUCzSmsdtbWSvh953Y9pump").unwrap();
        let bonding_curve = Pubkey::from_str("usAdAyXTZHo8U5JH4ykNi2Cr6fbWyFYXkkhKqFeSXA6").unwrap();
        let sol = SolHook::new(config.rpc_url.clone());
        let keypair = Keypair::from_base58_string("");
        let pump = PumpFun::new(Arc::new(keypair.insecure_clone()), Arc::new(sol.clone()));
        let (state, price) = pump.fetch_price(&bonding_curve).await.unwrap();

        // send via TPU; confirm via async RPC (confirmed)
        let (ixs, recent_fees) = pump.buy(
            &mint,
            &bonding_curve,
            &state.creator,
            0.001,
            1.1,
            price,
            Some(true),
        ).await.unwrap();

        let sig = sol.spray_with_nextblock(ixs, &keypair, recent_fees, 70_000, &clients).await.unwrap();
        println!("Sig: https://solscan.io/tx/{}", sig);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_pump_buy_helius() {
        let config = config();
        let clients = &HeliusSender::multiple_new(&HELIUS_SENDER_ENDPOINTS);
        let mint = Pubkey::from_str("AhMLyXcAzkpHbfH2AxMEdiUCzSmsdtbWSvh953Y9pump").unwrap();
        let bonding_curve = Pubkey::from_str("usAdAyXTZHo8U5JH4ykNi2Cr6fbWyFYXkkhKqFeSXA6").unwrap();
        let sol = SolHook::new(config.rpc_url.clone());
        let keypair = Keypair::from_base58_string("");
        let pump = PumpFun::new(Arc::new(keypair.insecure_clone()), Arc::new(sol.clone()));
        let (state, price) = pump.fetch_price(&bonding_curve).await.unwrap();

        // send via TPU; confirm via async RPC (confirmed)
        let (ixs, recent_fees) = pump.buy(
            &mint,
            &bonding_curve,
            &state.creator,
            0.001,
            1.1,
            price,
            Some(true),
        ).await.unwrap();

        let sig = sol.spray_with_helius(ixs, &keypair, recent_fees, 70_000, &clients).await.unwrap();
        println!("Sig: https://solscan.io/tx/{}", sig);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_pump_buy_zero_slot() {
        let config = config();
        let clients = &ZeroSlot::multiple_new(&ZERO_SLOT_ENDPOINTS, url_decode("").unwrap().to_string());
        clients[0].warmup().await;
        let mint = Pubkey::from_str("AhMLyXcAzkpHbfH2AxMEdiUCzSmsdtbWSvh953Y9pump").unwrap();
        let bonding_curve = Pubkey::from_str("usAdAyXTZHo8U5JH4ykNi2Cr6fbWyFYXkkhKqFeSXA6").unwrap();
        let sol = SolHook::new(config.rpc_url.clone());
        let keypair = Keypair::from_base58_string("");
        let pump = PumpFun::new(Arc::new(keypair.insecure_clone()), Arc::new(sol.clone()));
        let (state, price) = pump.fetch_price(&bonding_curve).await.unwrap();

        let (ixs, recent_fees) = pump.buy(
            &mint,
            &bonding_curve,
            &state.creator,
            0.001,
            1.1,
            price,
            Some(true),
        ).await.unwrap();

        let sig = sol.spray_with_zero_slot(ixs, &keypair, recent_fees, 100_000, &clients).await.unwrap();
        println!("Sig: https://solscan.io/tx/{}", sig);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_pump_buy_temporal() {
        let config = config();
        let clients = &TemporalSender::multiple_new(&TEMPORAL_HTTP_ENDPOINTS, url_decode("").unwrap().to_string());
        let mint = Pubkey::from_str("AhMLyXcAzkpHbfH2AxMEdiUCzSmsdtbWSvh953Y9pump").unwrap();
        let bonding_curve = Pubkey::from_str("usAdAyXTZHo8U5JH4ykNi2Cr6fbWyFYXkkhKqFeSXA6").unwrap();
        let sol = SolHook::new(config.rpc_url.clone());
        let keypair = Keypair::from_base58_string("");
        let pump = PumpFun::new(Arc::new(keypair.insecure_clone()), Arc::new(sol.clone()));
        let (state, price) = pump.fetch_price(&bonding_curve).await.unwrap();

        let (ixs, recent_fees) = pump.buy(
            &mint,
            &bonding_curve,
            &state.creator,
            0.001,
            1.1,
            price,
            Some(true),
        ).await.unwrap();

        let sig = sol.spray_with_temporal(ixs, &keypair, recent_fees, 70_000, clients, 1_000_000).await.unwrap();
        println!("Sig: https://solscan.io/tx/{}", sig);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_pump_buy_blox() {
        let config = config();
        let clients = &Bloxroute::multiple_new(&BLOX_ENDPOINTS, url_decode("").unwrap().to_string());
        let mint = Pubkey::from_str("AhMLyXcAzkpHbfH2AxMEdiUCzSmsdtbWSvh953Y9pump").unwrap();
        let bonding_curve = Pubkey::from_str("usAdAyXTZHo8U5JH4ykNi2Cr6fbWyFYXkkhKqFeSXA6").unwrap();
        let sol = SolHook::new(config.rpc_url.clone());
        let keypair = Keypair::from_base58_string("");
        let pump = PumpFun::new(Arc::new(keypair.insecure_clone()), Arc::new(sol.clone()));
        let (state, price) = pump.fetch_price(&bonding_curve).await.unwrap();

        let (ixs, recent_fees) = pump.buy(
            &mint,
            &bonding_curve,
            &state.creator,
            0.001,
            1.1,
            price,
            Some(true),
        ).await.unwrap();

        let sig = sol.spray_with_blox(ixs, &keypair, recent_fees, 90_000, clients, 1_000_000).await.unwrap();
        println!("Sig: https://solscan.io/tx/{}", sig);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_pump_buy_all() {
        let config = config();
        let clients = &NextBlock::multiple_new(&NB_ENDPOINTS, url_decode("").unwrap().to_string());
        let nonce_account = config.nonce_account.clone().unwrap();
        let jito_clients = &get_jito_clients();
        let helius_clients = &HeliusSender::multiple_new(&HELIUS_SENDER_ENDPOINTS);
        for c in helius_clients {
            c.ping().send().await.unwrap();
        }

        let zero_slot_clients = &ZeroSlot::multiple_new(&ZERO_SLOT_ENDPOINTS, url_decode("").unwrap().to_string());
        zero_slot_clients[0].warmup().await;
        let temporal_clients = &TemporalSender::multiple_new(&TEMPORAL_HTTP_ENDPOINTS, url_decode("").unwrap().to_string());
        for c in temporal_clients {
            c.ping().send().await.unwrap();
        }

        let blox_clients = &Bloxroute::multiple_new(&BLOX_ENDPOINTS, url_decode("").unwrap().to_string());

        let mint = Pubkey::from_str("AhMLyXcAzkpHbfH2AxMEdiUCzSmsdtbWSvh953Y9pump").unwrap();
        let bonding_curve = Pubkey::from_str("usAdAyXTZHo8U5JH4ykNi2Cr6fbWyFYXkkhKqFeSXA6").unwrap();
        let sol = SolHook::new(config.rpc_url.clone());
        let keypair = Keypair::from_base58_string("");
        let pump = PumpFun::new(Arc::new(keypair.insecure_clone()), Arc::new(sol.clone()));
        let (state, price) = pump.fetch_price(&bonding_curve).await.unwrap();

        // send via TPU; confirm via async RPC (confirmed)
        let (ixs, recent_fees) = pump.buy(
            &mint,
            &bonding_curve,
            &state.creator,
            0.001,
            1.1,
            price,
            Some(false),
        ).await.unwrap();

        let sig = sol.spray_with_all(ixs, &keypair, recent_fees, &clients, &jito_clients, &helius_clients, &zero_slot_clients, &temporal_clients, &blox_clients, 1_100_000, None, Some(Pubkey::from_str(&nonce_account).unwrap())).await.unwrap();    
        println!("Sig: https://solscan.io/tx/{}", sig);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_tx_single() {
        let config = config();
        let mint = Pubkey::from_str("AhMLyXcAzkpHbfH2AxMEdiUCzSmsdtbWSvh953Y9pump").unwrap();
        let jito_clients = &get_jito_clients();
        let zero_slot_clients = &ZeroSlot::multiple_new(&ZERO_SLOT_ENDPOINTS, url_decode("").unwrap().to_string());
        zero_slot_clients[0].warmup().await;
        let sol = SolHook::new(config.rpc_url.clone());
        let keypair = Keypair::from_base58_string("");
        let recent_fees = 1_000_000;
        let mut ixs = vec![];
        
        ixs.push(
            spl_associated_token_account::instruction::create_associated_token_account(
                &keypair.pubkey(),
                &keypair.pubkey(),
                &mint,
                &TOKEN_PROGRAM_ID,
            )
        );

        let sig = sol.spray_with_jito(ixs, &keypair, recent_fees, 25_000, &jito_clients).await.unwrap();
        println!("Sig: https://solscan.io/tx/{}", sig);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_tx_all() {
        let config = config();
        let clients = &NextBlock::multiple_new(&NB_ENDPOINTS, url_decode("").unwrap().to_string());
        let nonce_account = Pubkey::from_str(&config.nonce_account.clone().unwrap()).unwrap();
        let mint = Pubkey::from_str("AhMLyXcAzkpHbfH2AxMEdiUCzSmsdtbWSvh953Y9pump").unwrap();
        let jito_clients = &get_jito_clients();
        let helius_clients = &HeliusSender::multiple_new(&HELIUS_SENDER_ENDPOINTS);
        let zero_slot_clients = &ZeroSlot::multiple_new(&ZERO_SLOT_ENDPOINTS, url_decode("").unwrap().to_string());
        zero_slot_clients[0].warmup().await;
        let temporal_clients = &TemporalSender::multiple_new(&TEMPORAL_HTTP_ENDPOINTS, url_decode("").unwrap().to_string());
        for c in temporal_clients {
            c.ping().send().await.unwrap();
        }
        for c in helius_clients {
            c.ping().send().await.unwrap();
        }
        let blox_clients = &Bloxroute::multiple_new(&BLOX_ENDPOINTS, url_decode("").unwrap().to_string());
        let sol = SolHook::new(config.rpc_url.clone());
        let keypair = Keypair::from_base58_string("");
        let recent_fees = 2_000_000;
        let mut ixs = vec![];
        
        ixs.push(
            spl_associated_token_account::instruction::create_associated_token_account(
                &keypair.pubkey(),
                &keypair.pubkey(),
                &mint,
                &TOKEN_PROGRAM_ID,
            )
        );

        let sig = sol.spray_with_all(ixs, &keypair, recent_fees, &clients, &jito_clients, &helius_clients, &zero_slot_clients, &temporal_clients, &blox_clients, 1_100_000, None, Some(nonce_account)).await.unwrap();
        println!("Sig: https://solscan.io/tx/{}", sig);
    }

    #[tokio::test]
    async fn test_subscribe_slot() {
        let ws = String::from("wss://api.apewise.org/ws?api-key=");
        let rpc = String::from("https://api.mainnet-beta.solana.com");
        let sol = SolHook::new(rpc.clone());
        let (mut rx, _) = sol.subscribe_slot_channel(&ws).await.unwrap();
        while let Some(msg) = rx.recv().await {
            println!("Message: {:?}", msg);
        }
    }

    #[tokio::test]
    async fn test_fees() {
        let config = config();
        let rpc = String::from(config.rpc_url.clone());
        let sol = SolHook::new(rpc.clone());
        let addresses = vec![
        ];
        sol.fetch_priority_fee(&PriorityFeeLevel::Low, &addresses).await.unwrap();
    }

    #[tokio::test]
    async fn test_compare_rpc_grpc() {
        let config = config();
        let grpc_url = config.grpc_url.clone().unwrap();
        let grpc_token = config.grpc_token.clone().unwrap();
        let rpc_url = config.rpc_url.clone();

        let sol = SolHook::new(rpc_url);
        let mut grpc_reqs = SolHook::grpc_connect(&grpc_url, Some(&grpc_token)).await.unwrap();
        let mut grpc_subs = SolHook::grpc_connect(&grpc_url, Some(&grpc_token)).await.unwrap();
        let mut stream = SolHook::grpc_subscribe_slot_channel(&mut grpc_subs).await.unwrap();

        let mut last_slot = 0;
        let mut last_time = Instant::now();
        while let Some(msg) = stream.next().await {
            let slot: yellowstone_grpc_proto::prelude::subscribe_update::UpdateOneof = msg.unwrap().update_oneof.unwrap();
            match slot {
                yellowstone_grpc_proto::prelude::subscribe_update::UpdateOneof::Slot(slot_update) => {
                    let slot = slot_update.slot;
                    if slot == last_slot {
                        continue;
                    }
                    last_slot = slot;
                    let time = last_time.elapsed();
                    println!("Slot: {:?}, Time: {:?}", slot, time);
                    last_time = Instant::now();
                    let rpc_blockhash = sol.rpc_client.get_latest_blockhash_with_commitment(CommitmentConfig::processed()).await.unwrap();
                    let grpc_blockhash = sol.grpc_latest_blockhash(&mut grpc_reqs).await.unwrap();
                    println!("RPC blockhash: {:?}", rpc_blockhash);
                    println!("GRPC blockhash: {:?}", grpc_blockhash);
                }
                _ => {}
            }
        }
    }

    #[tokio::test]
    async fn test_close_ata() {
        let config = config();
        let sol = SolHook::new(config.rpc_url.clone());
        let keypair = Keypair::from_base58_string("");
        let mint = Pubkey::from_str("AhMLyXcAzkpHbfH2AxMEdiUCzSmsdtbWSvh953Y9pump").unwrap();
        let (_, sig) = sol.close_ata(&mint, &keypair, false).await.unwrap();
        println!("Sig: https://solscan.io/tx/{}", sig);
    }

    #[tokio::test]
    async fn test_vacation() {
        let config = config();
        let sol = SolHook::new(config.rpc_url.clone());
        let mut vacation = Vacation::new(sol);
        vacation = vacation.initialize("postgres://").await;
        let leaders = vacation.get_leaders(8).await;
        println!("Leaders: {:?}", leaders);
        let db_url = "sqlite://vacation.db";
        vacation.connect_db(&db_url).await.unwrap();
        vacation.ensure_table().await.unwrap();
        println!("Table created");
    }

    #[tokio::test]
    async fn test_vacation_fill() {
        let mut colors = Colors::new(io::stdout().lock());
        let config = config();
        let sol = SolHook::new(config.rpc_url.clone());
        let mut vacation = Vacation::new(sol);
        vacation = vacation.initialize("postgres://").await;
        vacation.fill(Some(500)).await.unwrap();
        colors.cprint("Vacation filled", cc::GREEN);
    }

    #[tokio::test]
    async fn test_buy_with_vac() {
        const FAV_PLACES: &[&str] = &[
            "Frankfurt",
        ];
        let config = config();

        let nonce_account = Pubkey::from_str(&config.nonce_account.clone().unwrap()).unwrap();
        let mut colors = Colors::new(io::stdout().lock());
        let grpc_token = config.grpc_token.clone().unwrap();
        let grpc_url = config.grpc_url.clone().unwrap();
        let mut grpc_client = SolHook::grpc_connect(&grpc_url, Some(&grpc_token)).await.unwrap();

        let clients = &NextBlock::multiple_new(&NB_ENDPOINTS, url_decode("").unwrap().to_string());
        let jito_clients = &get_jito_clients();
        let helius_clients = &HeliusSender::multiple_new(&HELIUS_SENDER_ENDPOINTS);
        for c in helius_clients {
            c.ping().send().await.unwrap();
        }

        let zero_slot_clients = &ZeroSlot::multiple_new(&ZERO_SLOT_ENDPOINTS, url_decode("").unwrap().to_string());
        zero_slot_clients[0].warmup().await;
        let temporal_clients = &TemporalSender::multiple_new(&TEMPORAL_HTTP_ENDPOINTS, url_decode("").unwrap().to_string());
        for c in temporal_clients {
            c.ping().send().await.unwrap();
        }

        let blox_clients = &Bloxroute::multiple_new(&BLOX_ENDPOINTS, url_decode("").unwrap().to_string());

        let mint = Pubkey::from_str("AhMLyXcAzkpHbfH2AxMEdiUCzSmsdtbWSvh953Y9pump").unwrap();
        let bonding_curve = Pubkey::from_str("usAdAyXTZHo8U5JH4ykNi2Cr6fbWyFYXkkhKqFeSXA6").unwrap();
        let sol = SolHook::new(config.rpc_url.clone());
        let keypair = Keypair::from_base58_string("");
        let pump = PumpFun::new(Arc::new(keypair.insecure_clone()), Arc::new(sol.clone()));
        let (state, price) = pump.fetch_price(&bonding_curve).await.unwrap();

        let mut vacation = Vacation::new(sol.clone());
        vacation = vacation.initialize("postgres://").await;
        vacation.fill(Some(500)).await.unwrap();
        let leaders = vacation.get_upcoming_leaders_vacation(4).await.unwrap(); // 4 slots ahead
        let xslot = sol.rpc_client.get_slot_with_commitment(CommitmentConfig::processed()).await.unwrap();
        let cur_vac = leaders.iter()
            .filter(|(slot, _, _)| *slot == xslot)
            .collect::<Vec<_>>();
        colors.cprint(&format!("Current vacation: {:?}", cur_vac), cc::GREEN);

        let mut stream = SolHook::grpc_subscribe_slot_channel(&mut grpc_client).await.unwrap();
        let mut irv = vec![];
        let mut last_slot = 0;
        let mut is_active = false;
        while let Some(msg) = stream.next().await {
            let slot: UpdateOneof = msg.unwrap().update_oneof.unwrap();
            match slot {
                UpdateOneof::Slot(slot_update) => {
                    let slot: u64 = slot_update.slot;
                    if slot == last_slot {
                        continue;
                    }
                    last_slot = slot;
                    let leaders = vacation.get_upcoming_leaders_vacation(4).await.unwrap(); // 4 slots ahead
                    let quad = leaders.iter()
                        .filter(|(s, _, _)| *s == slot || *s == slot + 1 || *s == slot + 2 || *s == slot + 3) // slot, city, country
                        .collect::<Vec<_>>();
                    colors.cprint(&format!("Current quad: {:?}", quad), cc::GREEN);

                    if FAV_PLACES.contains(&quad[3].1.as_str()) && !is_active {
                        is_active = true;
                        colors.cprint(&format!("Preparing the buy ahead.."), cc::GREEN);

                        irv.push(pump.buy(
                            &mint,
                            &bonding_curve,
                            &state.creator,
                            0.001,
                            1.1,
                            price,
                            Some(false),
                        ).await.unwrap());
                        
                    } else if FAV_PLACES.contains(&quad[1].1.as_str()) {
                        if !irv.is_empty() {
                            let sig = sol.spray_with_all(irv[0].0.clone(), &keypair, irv[0].1, &clients, &jito_clients, &helius_clients, &zero_slot_clients, &temporal_clients, &blox_clients, 1_100_000, Some(slot), Some(nonce_account)).await.unwrap();
                            println!("Sig: https://solscan.io/tx/{}", sig);
                        }
                        break;
                    }
                }

                _ => {}
            }
        }
    }

    #[tokio::test]
    async fn test_nonce_account() {
        let config = config();
        let sol = SolHook::new(config.rpc_url.clone());
        let keypair = Keypair::from_base58_string("");
        let nonce_account = Keypair::new();
        let sig = sol.create_nonce_account(&keypair, &nonce_account, &keypair.pubkey()).await.unwrap();
        let nonce_secret = nonce_account.to_base58_string();
        let nonce_pubkey = nonce_account.pubkey();
        println!("Nonce secret: {:?}", nonce_secret);
        println!("Nonce pubkey: {:?}", nonce_pubkey);
        println!("Sig: https://solscan.io/tx/{}", sig);
    }

    #[tokio::test]
    async fn test_nonce_account_data() {
        let config = config();
        let sol = SolHook::new(config.rpc_url.clone());
        let nonce_account = Pubkey::from_str("").unwrap();
        let (blockhash, authority) = sol.get_nonce_data(&nonce_account).await.unwrap();
        println!("Blockhash: {:?}", blockhash);
        println!("Authority: {:?}", authority);
    }
}