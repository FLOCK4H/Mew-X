use crate::mew::config::get_priority_fee_lvl;
use crate::mew::writing::cc;
use {crate::log, pump_swap_types::state::Pool, spl_token_2022::instruction::sync_native};

#[allow(unused_imports)]
use {
    crate::mew::sol_hook::{
        sol::{PriorityFeeLevel, SYSTEM_PROGRAM, SolHook, TOKEN_PROGRAM_ID, WSOL_MINT},
        utils::decode_b64,
    },
    borsh::BorshDeserialize,
    pump_swap_types::{
        BondingCurve,
        events::{BuyEvent, CreatePoolEvent, SellEvent},
    },
    solana_commitment_config::CommitmentConfig,
    solana_keypair::Keypair,
    solana_program::{
        instruction::{AccountMeta, Instruction},
        pubkey::Pubkey,
    },
    solana_signer::Signer,
    spl_associated_token_account::instruction::{
        create_associated_token_account, create_associated_token_account_idempotent,
    },
    std::{io::Cursor, sync::Arc},
    tokio,
};

pub const CREATE_POOL_DISCRIM: [u8; 8] = [233, 146, 209, 142, 207, 104, 64, 188];
pub const BUY_INSTR_DISCRIM: [u8; 8] = [102, 6, 61, 18, 1, 218, 235, 234];
pub const SELL_INSTR_DISCRIM: [u8; 8] = [51, 230, 133, 164, 1, 127, 131, 173];
pub const COLLECT_FEE_DISCRIM: [u8; 8] = [122, 2, 127, 1, 14, 191, 12, 175];
pub const WITHDRAW_DISCRIM: [u8; 8] = [22, 9, 133, 26, 160, 44, 71, 192];

pub const PUMP_SWAP_ID: Pubkey =
    Pubkey::from_str_const("pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA");
pub const EVENT_AUTHORITY: Pubkey =
    Pubkey::from_str_const("GS4CU59F31iL7aR2Q8zVS8DRrcRnXX1yjQ66TqNVQnaR");
pub const GLOBAL_CONFIG_PUB: Pubkey =
    Pubkey::from_str_const("ADyA8hdefvWN2dbGGWFotbzWxrAvLW83WG6QCVXvJKqw");
pub const PROTOCOL_FEE_RECIP: Pubkey =
    Pubkey::from_str_const("62qc2CNXwrYqQScmEdiZFFAnJR262PxWEuNQtxfafNgV");
pub const PROTOCOL_FEE_RECIP_ATA: Pubkey =
    Pubkey::from_str_const("94qWNrtmfn42h3ZjUZwWvK1MEo9uVmmrBPd2hpNjYDjb");
pub const ASSOCIATED_TOKEN_PROGRAM: Pubkey =
    Pubkey::from_str_const("ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL");
pub const GLOBAL_VOLUME_ACCUMULATOR: Pubkey =
    Pubkey::from_str_const("C2aFPdENg4A2HQsmrd5rTw5TaYBX5Ku887cWjbFKtZpw");
pub const FEE_CONFIG: Pubkey =
    Pubkey::from_str_const("5PHirr8joyTMp9JMm6nW7hNDVyEYdkzDqazxPD7RaTjx");
pub const FEE_PROGRAM: Pubkey =
    Pubkey::from_str_const("pfeeUxB6jkeY1Hxd7CsFCAjcbHA9rWtchMGdZ6VojVZ");

pub const SEARCH_FOR: &str = "Program data: ";

// Offsets for base/quote mints inside PumpSwap Pool account data
pub const BASE_MINT_OFFSET: usize = 43;
pub const QUOTE_MINT_OFFSET: usize = 75;

#[derive(Debug)]
pub enum PumpSwapEvent {
    CreatePool(Option<CreatePoolEvent>),
    Buy(Option<BuyEvent>),
    Sell(Option<SellEvent>),
    Unknown,
}

#[derive(Clone)]
pub struct PumpSwap {
    pub keypair: Arc<Keypair>,
    pub sol: Arc<SolHook>,
}

impl PumpSwap {
    pub fn new(keypair: Arc<Keypair>, sol: Arc<SolHook>) -> Self {
        Self { keypair, sol }
    }

    fn normalize_slippage(slippage: f64) -> f64 {
        let normalized = if slippage > 1.0 {
            slippage / 100.0
        } else {
            slippage
        };
        normalized.max(0.0)
    }

    pub async fn derive_creator_vault(&self, creator: &Pubkey) -> anyhow::Result<(Pubkey, Pubkey)> {
        let (pda, _) =
            Pubkey::find_program_address(&[b"creator_vault", creator.as_ref()], &PUMP_SWAP_ID);
        let vault_ata = self.sol.get_ata_for_token(&pda, &WSOL_MINT);
        Ok((pda, vault_ata))
    }

    pub async fn derive_uv_accu(&self, user: &Pubkey) -> anyhow::Result<Pubkey> {
        let (pda, _) = Pubkey::find_program_address(
            &[b"user_volume_accumulator", user.as_ref()],
            &PUMP_SWAP_ID,
        );
        Ok(pda)
    }

    pub async fn fetch_state(&self, pool: &Pubkey) -> Result<Pool, anyhow::Error> {
        let state = self
            .sol
            .rpc_client
            .get_account_with_commitment(pool, CommitmentConfig::processed())
            .await?
            .value
            .ok_or(anyhow::anyhow!("account not found"))?
            .data;
        let mut cursor = Cursor::new(&state[8..]);
        let pool = Pool::deserialize_reader(&mut cursor)?;
        Ok(pool)
    }

    /// Find PumpSwap pools by base mint (and optional quote mint) via on-chain memcmp filters.
    /// Returns pool addresses that match the given mint(s).
    pub async fn find_pools_by_mint(
        &self,
        base_mint: &Pubkey,
        quote_mint: Option<&Pubkey>,
    ) -> anyhow::Result<Vec<Pubkey>> {
        use solana_account_decoder_client_types::UiAccountEncoding;
        use solana_client::rpc_config::{RpcAccountInfoConfig, RpcProgramAccountsConfig};
        use solana_client::rpc_filter::{Memcmp, RpcFilterType};

        let mut filters = vec![RpcFilterType::Memcmp(Memcmp::new_base58_encoded(
            BASE_MINT_OFFSET,
            base_mint.as_ref(),
        ))];

        if let Some(q) = quote_mint {
            filters.push(RpcFilterType::Memcmp(Memcmp::new_base58_encoded(
                QUOTE_MINT_OFFSET,
                q.as_ref(),
            )));
        }

        let cfg = RpcProgramAccountsConfig {
            filters: Some(filters),
            account_config: RpcAccountInfoConfig {
                encoding: Some(UiAccountEncoding::Base64),
                commitment: Some(CommitmentConfig::confirmed()),
                ..Default::default()
            },
            with_context: None,
            sort_results: None,
        };

        let acct_list = self
            .sol
            .rpc_client
            .get_program_accounts_with_config(&PUMP_SWAP_ID, cfg)
            .await?;

        Ok(acct_list.into_iter().map(|(k, _)| k).collect())
    }

    pub async fn fetch_price(&self, pool: &Pubkey) -> anyhow::Result<(Pool, f64)> {
        let state = self.fetch_state(pool).await?;
        let vsr = self
            .sol
            .get_token_balance_from_ata(&state.pool_quote_token_account)
            .await?;
        let vtr = self
            .sol
            .get_token_balance_from_ata(&state.pool_base_token_account)
            .await?;
        let price = vsr as f64 / vtr as f64;
        Ok((state, price))
    }

    pub async fn get_mint_from_pool(&self, pool: &Pubkey) -> anyhow::Result<Pubkey> {
        let pool = self.fetch_state(pool).await?;
        Ok(pool.base_mint)
    }

    pub fn parse_logs(
        logs: std::slice::Iter<'_, String>,
        sig: Option<&String>,
    ) -> Vec<PumpSwapEvent> {
        let mut events: Vec<PumpSwapEvent> = Vec::new();
        for log in logs {
            let is_program_data = log.contains(SEARCH_FOR);
            if is_program_data {
                let program_data = log.split_at(SEARCH_FOR.len());
                let (_, program_data) = program_data;
                let b64 = match decode_b64(program_data) {
                    Ok(b64) => b64,
                    Err(_) => {
                        continue;
                    }
                };
                if b64[..8] == CREATE_POOL_DISCRIM {
                    let mut cur = Cursor::new(&b64[8..]);
                    let dtx = CreatePoolEvent::deserialize_reader(&mut cur);
                    match dtx {
                        Ok(dtx) => {
                            events.push(PumpSwapEvent::CreatePool(Some(dtx)));
                        }
                        Err(e) => {
                            eprintln!(
                                "Error deserializing create pool event {:?}: {e}",
                                sig.unwrap_or(&"".to_string())
                            );
                        }
                    }
                } else if b64[..8] == BUY_INSTR_DISCRIM {
                    let mut cur = Cursor::new(&b64[8..]);
                    let dtx = BuyEvent::deserialize_reader(&mut cur);
                    match dtx {
                        Ok(dtx) => {
                            events.push(PumpSwapEvent::Buy(Some(dtx)));
                        }
                        Err(e) => {
                            eprintln!(
                                "Error deserializing buy event {:?}: {e}",
                                sig.unwrap_or(&"".to_string())
                            );
                        }
                    }
                } else if b64[..8] == SELL_INSTR_DISCRIM {
                    let mut cur = Cursor::new(&b64[8..]);
                    let dtx = SellEvent::deserialize_reader(&mut cur);
                    match dtx {
                        Ok(dtx) => {
                            events.push(PumpSwapEvent::Sell(Some(dtx)));
                        }
                        Err(e) => {
                            eprintln!(
                                "Error deserializing sell event {:?}: {e}",
                                sig.unwrap_or(&"".to_string())
                            );
                        }
                    }
                } else {
                    events.push(PumpSwapEvent::Unknown);
                }
            }
        }
        events
    }

    pub fn price_from_create(create: &CreatePoolEvent) -> f64 {
        let vtr = create.pool_base_amount as f64 / 1e6; // token
        let vsr = create.pool_quote_amount as f64 / 1e9; // sol
        vsr / vtr
    }

    pub fn price_from_buy(buy: &BuyEvent) -> f64 {
        let vtr = buy.pool_base_token_reserves as f64 / 1e6;
        let vsr = buy.pool_quote_token_reserves as f64 / 1e9;
        vsr / vtr
    }

    pub fn price_from_sell(sell: &SellEvent) -> f64 {
        let vtr = sell.pool_base_token_reserves as f64 / 1e6;
        let vsr = sell.pool_quote_token_reserves as f64 / 1e9;
        vsr / vtr
    }

    pub fn price_from_reserves(vtr: u64, vsr: u64) -> f64 {
        let vtr = vtr as f64 / 1e6;
        let vsr = vsr as f64 / 1e9;
        vsr / vtr
    }

    pub async fn get_market_cap(vtr: u64, vsr: u64, sol_price_usd: f64) -> anyhow::Result<f64> {
        let price = Self::price_from_reserves(vtr, vsr);
        let price_in_usd = price * sol_price_usd;
        Ok(1000000000.0 * price_in_usd)
    }

    pub async fn buy(
        &self,
        mint: &Pubkey,
        pool: &Pubkey,
        creator: &Pubkey,
        sol_amount_in: f64,
        slippage: f64,
        price: f64,
        use_idempotent: Option<bool>,
    ) -> anyhow::Result<(Vec<Instruction>, u64)> {
        let buyer = self.keypair.pubkey();

        // Base token program (may be TOKEN_PROGRAM_ID or Token2022)
        let base_program = self.sol.get_token_program_id(mint).await.unwrap();
        let quote_program = self.sol.get_token_program_id(&WSOL_MINT).await.unwrap();

        let slippage_pct = Self::normalize_slippage(slippage);
        let sol_amount_lamports = (sol_amount_in * 1e9) as u64;
        let max_sol_cost = ((sol_amount_lamports as f64) * (1.0 + slippage_pct)).ceil() as u64;
        let token_amount_out = ((sol_amount_in / price) * 1e6) as u64;

        let (creator_vault, creator_vault_ata) = self.derive_creator_vault(creator).await.unwrap();
        let user_volume_accu = self.derive_uv_accu(&buyer).await.unwrap();
        let state = self.fetch_state(pool).await.unwrap();

        let mut ixs = vec![];

        if use_idempotent.unwrap_or(false) {
            // Base mint ATA
            ixs.push(create_associated_token_account_idempotent(
                &buyer,
                &buyer,
                mint,
                &base_program,
            ));
            // WSOL ATA
            ixs.push(create_associated_token_account_idempotent(
                &buyer,
                &buyer,
                &WSOL_MINT,
                &quote_program,
            ));
        } else {
            ixs.push(create_associated_token_account(
                &buyer,
                &buyer,
                mint,
                &base_program,
            ));
            ixs.push(create_associated_token_account(
                &buyer,
                &buyer,
                &WSOL_MINT,
                &quote_program,
            ));
        }

        let wsol_ata = self.sol.get_ata_for_token(&buyer, &WSOL_MINT);

        let associated_user = if base_program == TOKEN_PROGRAM_ID {
            self.sol.get_ata_for_token(&buyer, mint)
        } else {
            self.sol.get_ata_for_token2022(&buyer, mint)
        };

        ixs.push(solana_program::system_instruction::transfer(
            &buyer,
            &wsol_ata,
            max_sol_cost,
        ));

        ixs.push(sync_native(&quote_program, &wsol_ata)?);

        let accs = vec![
            AccountMeta::new(*pool, false), // pool must be mutable now
            AccountMeta::new(buyer, true),
            AccountMeta::new_readonly(GLOBAL_CONFIG_PUB, false),
            AccountMeta::new_readonly(*mint, false),
            AccountMeta::new_readonly(WSOL_MINT, false),
            AccountMeta::new(associated_user, false),
            AccountMeta::new(wsol_ata, false),
            AccountMeta::new(state.pool_base_token_account, false),
            AccountMeta::new(state.pool_quote_token_account, false),
            AccountMeta::new_readonly(PROTOCOL_FEE_RECIP, false),
            AccountMeta::new(PROTOCOL_FEE_RECIP_ATA, false),
            // Base token program (may be Token2022)
            AccountMeta::new_readonly(base_program, false),
            // Quote token program (WSOL – legacy)
            AccountMeta::new_readonly(quote_program, false),
            AccountMeta::new_readonly(SYSTEM_PROGRAM, false),
            AccountMeta::new_readonly(ASSOCIATED_TOKEN_PROGRAM, false),
            AccountMeta::new_readonly(EVENT_AUTHORITY, false),
            AccountMeta::new_readonly(PUMP_SWAP_ID, false),
            AccountMeta::new(creator_vault_ata, false),
            AccountMeta::new_readonly(creator_vault, false),
            AccountMeta::new(GLOBAL_VOLUME_ACCUMULATOR, false),
            AccountMeta::new(user_volume_accu, false),
            AccountMeta::new_readonly(FEE_CONFIG, false),
            AccountMeta::new_readonly(FEE_PROGRAM, false),
        ];

        let recent_fees = self
            .sol
            .fetch_priority_fee(
                &get_priority_fee_lvl(),
                &accs.iter().map(|acc| acc.pubkey).collect::<Vec<Pubkey>>(),
            )
            .await
            .unwrap();

        log!(cc::LIGHT_CYAN, "Fee: {:?}", recent_fees);

        let mut data = [0u8; 8 + 8 + 8];
        data[..8].copy_from_slice(&BUY_INSTR_DISCRIM);
        data[8..16].copy_from_slice(&token_amount_out.to_le_bytes());
        data[16..24].copy_from_slice(&max_sol_cost.to_le_bytes());

        ixs.push(Instruction {
            program_id: PUMP_SWAP_ID,
            accounts: accs,
            data: data.to_vec(),
        });

        Ok((ixs, recent_fees))
    }

    pub async fn sell(
        &self,
        mint: &Pubkey,
        pool: &Pubkey,
        creator: &Pubkey,
        sell_pct: u64,
        slippage: f64,
        price: f64,
    ) -> anyhow::Result<(Vec<Instruction>, u64)> {
        let buyer = self.keypair.pubkey();
        let program = self.sol.get_token_program_id(mint).await.unwrap();
        let quote_program = self.sol.get_token_program_id(&WSOL_MINT).await.unwrap();
        let state = self.fetch_state(pool).await.unwrap();
        let (creator_vault, creator_vault_ata) = self.derive_creator_vault(creator).await.unwrap();

        let slippage_pct = Self::normalize_slippage(slippage);
        let token_balance = self.sol.get_token_balance(&buyer, &mint).await.unwrap();
        let token_balance_raw = (token_balance * 1e6) as u64;
        let token_amount_out = (token_balance_raw * sell_pct.max(1) / 100) as u64;
        let min_sol_output = token_balance * price;
        let min_sol_output = min_sol_output * (1.0 - slippage_pct);
        let min_sol_output = (min_sol_output.max(0.0) * 1e9) as u64;

        let mut ixs = vec![];
        let wsol_ata = self.sol.get_ata_for_token(&buyer, &WSOL_MINT);

        let associated_user: Pubkey;
        if program == TOKEN_PROGRAM_ID {
            associated_user = self.sol.get_ata_for_token(&buyer, mint);
        } else {
            associated_user = self.sol.get_ata_for_token2022(&buyer, mint);
        }
        let accs = vec![
            AccountMeta::new(*pool, false), // pool must be mutable now
            AccountMeta::new(buyer, true),
            AccountMeta::new_readonly(GLOBAL_CONFIG_PUB, false),
            AccountMeta::new_readonly(*mint, false),
            AccountMeta::new_readonly(WSOL_MINT, false),
            AccountMeta::new(associated_user, false),
            AccountMeta::new(wsol_ata, false),
            AccountMeta::new(state.pool_base_token_account, false),
            AccountMeta::new(state.pool_quote_token_account, false),
            AccountMeta::new_readonly(PROTOCOL_FEE_RECIP, false),
            AccountMeta::new(PROTOCOL_FEE_RECIP_ATA, false),
            AccountMeta::new_readonly(program, false),
            AccountMeta::new_readonly(quote_program, false),
            AccountMeta::new_readonly(SYSTEM_PROGRAM, false),
            AccountMeta::new_readonly(ASSOCIATED_TOKEN_PROGRAM, false),
            AccountMeta::new_readonly(EVENT_AUTHORITY, false),
            AccountMeta::new_readonly(PUMP_SWAP_ID, false),
            AccountMeta::new(creator_vault_ata, false),
            AccountMeta::new_readonly(creator_vault, false),
            AccountMeta::new_readonly(FEE_CONFIG, false),
            AccountMeta::new_readonly(FEE_PROGRAM, false),
        ];
        let recent_fees = self
            .sol
            .fetch_priority_fee(
                &get_priority_fee_lvl(),
                &accs.iter().map(|acc| acc.pubkey).collect::<Vec<Pubkey>>(),
            )
            .await
            .unwrap();
        log!(cc::LIGHT_CYAN, "Fee: {:?}", recent_fees);
        let mut data = [0u8; 8 + 8 + 8];
        data[..8].copy_from_slice(&SELL_INSTR_DISCRIM);
        data[8..16].copy_from_slice(&token_amount_out.to_le_bytes());
        data[16..24].copy_from_slice(&min_sol_output.to_le_bytes());

        ixs.push(Instruction {
            program_id: PUMP_SWAP_ID,
            accounts: accs,
            data: data.to_vec(),
        });
        Ok((ixs, recent_fees))
    }
}
