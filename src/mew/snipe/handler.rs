use std::time::{Instant};

use crate::{log, warn};

#[allow(unused_imports)]
use {
    crate::mew::{config::{config, Config, get_snipe_config, StratsConfig, get_strats_config, get_mode, get_use_regions, get_tx_settings, get_nonce_account, get_max_tokens_at_once}, deagle::deagle::{AlgoConfig, Deagle, Source}, sol_hook::{pump_fun::{PumpFun, PumpFunEvent, TOTAL_SUPPLY, BondingCurveAccount}, pump_swap::{PumpSwap, PumpSwapEvent}, goldmine::{Goldmine, Dupe, Sim}, sol::{SolHook, SYSTEM_PROGRAM, WSOL_MINT}}, writing::{cc, Colors}, sol_hook::vacation::Vacation},
    pump_fun_types::events::{CreateEvent, TradeEvent},
    pump_swap_types::events::{BuyEvent, CreatePoolEvent, SellEvent},
    solana_keypair::Keypair,
    solana_program::pubkey::{self, Pubkey}, tokio::stream,
    std::{collections::HashMap, str::FromStr},
    yellowstone_grpc_proto::prelude::{TransactionStatusMeta, SubscribeUpdateTransactionInfo},
    yellowstone_grpc_proto::prelude::subscribe_update::UpdateOneof,
    tokio_stream::StreamExt,
    std::io::{self, Write},
    sqlx::types::Json,
    std::{sync::Arc},
    tokio::sync::RwLock,
    ringbuffer::{AllocRingBuffer, RingBuffer},
    chrono::Utc,
    dashmap::DashMap,
    dashmap::mapref::entry::Entry,
    pump_swap_types::state::Pool,
    solana_program::instruction::Instruction,
    crate::mew::swqos::{blox::{Bloxroute, BLOX_ENDPOINTS}, jito::{JitoClient, get_jito_clients}, nextblock::{NextBlock, NB_ENDPOINTS}, temporal::{TemporalSender, TEMPORAL_HTTP_ENDPOINTS}, zero_slot::{ZeroSlot, ZERO_SLOT_ENDPOINTS}, helius::{HeliusSender, HELIUS_SENDER_ENDPOINTS}},
    solana_signature::Signature,
    solana_rpc_client_types::config::RpcTransactionLogsFilter,
    solana_commitment_config::CommitmentConfig,
};
use once_cell::sync::Lazy;
use regex::Regex;
use reqwest::{header, Client};

#[derive(Debug, Clone)]
pub enum Market {
    PumpSwap,
    PumpFun,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Holdings {
    pub pool: Pubkey,
    pub buy_price: Option<f64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MewMint {
    pub mint: Pubkey,
    pub bonding_curve: Pubkey,
    pub price: f64,
    pub highest_price: f64,
    pub name: String,
    pub symbol: String,
    pub uri: String,
    pub creator: Pubkey,
    pub creator_sold: bool,
    pub creator_token_amount: f64,
    pub buys: i64,
    pub sells: i64,
    pub volume: f64,
    pub liquidity: f64,
    pub txns_in_zero: i64,
    pub txns_in_n: i64,
    pub volume_in_n: f64,
    pub first_seen_slot: u64,
    pub last_seen_slot: u64,
    pub is_migrated: bool,
    pub created_time: f64,
}

#[derive(Clone)]
pub struct MewSnipe {
    pub sol_hook: Arc<SolHook>,
    pub goldmine: Arc<Goldmine>,
    pub vacation: Arc<Vacation>,
    pub deagle: Arc<Deagle>,
    pub pump_fun: Arc<PumpFun>,
    pub pump_swap: Arc<PumpSwap>,
    pub grpc_url: String,
    pub grpc_token: String,
    pub creators: Arc<RwLock<Vec<(String, Source)>>>,
    pub mints: Arc<RwLock<HashMap<Pubkey, MewMint>>>,
    pub algo_config: AlgoConfig,
    holdings: Arc<DashMap<Pubkey, Holdings>>,
}

pub struct Pools {
    pub pump_fun: Option<BondingCurveAccount>,
    pub pump_swap: Option<Pool>,
}

pub fn pct_change(entry: f64, now: f64) -> f64 {
    if !entry.is_finite() || !now.is_finite() {
        return f64::NAN;
    }
    if entry == 0.0 {
        return if now > 0.0 {
            f64::INFINITY
        } else if now < 0.0 {
            f64::NEG_INFINITY
        } else {
            0.0
        };
    }
    (now / entry - 1.0) * 100.0
}

static TWITTER_SCORE_RE: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"Twitter Score is (\d+)").unwrap());

pub fn sanitize_twitter_input(raw: &str) -> Option<String> {
    static PURE_HANDLE_RE: Lazy<Regex> =
        Lazy::new(|| Regex::new(r"^[A-Za-z0-9_]{1,15}$").unwrap());

    static HANDLE_FROM_URL: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r"(?i)^(?:https?://)?(?:www\.)?(?:x\.com|twitter\.com)/([A-Za-z0-9_]{1,15})(?:/.*)?$")
            .unwrap()
    });

    let s = raw.trim();

    if s.to_ascii_lowercase().contains("/i/communities") || s.to_ascii_lowercase().contains("/i/spaces") 
    || s.to_ascii_lowercase().contains("/i/topics") || s.to_ascii_lowercase().contains("/status") 
    || s.to_ascii_lowercase().contains("/intent") {
        return None; // drop communities entirely
    }
    if let Some(cap) = HANDLE_FROM_URL.captures(s) {
        return Some(cap[1].to_ascii_lowercase());
    }
    if PURE_HANDLE_RE.is_match(s) {
        return Some(s.to_ascii_lowercase());
    }
    None
}

pub async fn fetch_twitter_score(client: &Client, twitter: &str) -> i32 {
    let handle = twitter.trim().trim_start_matches('@');
    let url = format!("https://twitterscore.io/twitter/{}/overview/", handle);

    match client
        .get(&url)
        .header(
            header::USER_AGENT,
            "Mozilla/5.0 (compatible; TwitterScoreBot/1.0; +https://example.com)",
        )
        .send()
        .await
    {
        Ok(resp) if resp.status().is_success() => match resp.text().await {
            Ok(body) => TWITTER_SCORE_RE
                .captures(&body)
                .and_then(|c| c.get(1))
                .and_then(|m| m.as_str().parse::<i32>().ok())
                .unwrap_or(0),
            _ => {
                0
            },
        },
        _ => 0,
    }
}

pub fn timestamp_now() -> f64 {
    let time = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("Time error.");
    let ts: f64 = time.as_secs() as f64 + f64::from(time.subsec_nanos()) * 1e-9;
    ts
}

impl MewSnipe {
    pub fn new(
        sol_hook: SolHook,
        goldmine: Goldmine,
        vacation: Vacation,
        deagle: Arc<Deagle>,
        pump_fun: PumpFun,
        pump_swap: PumpSwap,
        grpc_url: String,
        grpc_token: String,
        creators: Vec<(String, Source)>,
        algo_config: AlgoConfig,
    ) -> Self {
        Self {
            sol_hook: Arc::new(sol_hook),
            goldmine: Arc::new(goldmine),
            vacation: Arc::new(vacation),
            deagle: deagle,
            pump_fun: Arc::new(pump_fun),
            pump_swap: Arc::new(pump_swap),
            grpc_url,
            grpc_token,
            creators: Arc::new(RwLock::new(creators)),
            mints: Arc::new(RwLock::new(HashMap::new())),
            algo_config: algo_config,
            holdings: Arc::new(DashMap::new()),
        }
    }

    pub async fn refresh_creators_loop(&self) -> anyhow::Result<()> {
        let mut last_creators = self.creators.read().await.len();
        loop {
            let algo_creators = match self.deagle.algo_choose_creators(self.algo_config.clone()).await {
                Ok(creators) => creators,
                Err(e) => {
                    warn!("refresh_creators_loop: {e}");
                    return Err(anyhow::anyhow!("refresh_creators_loop: {e}"));
                }
            };
            {
                let c_len = algo_creators.len();
                let diff = c_len as i64 - last_creators as i64;

                let mut w = self.creators.write().await;
                *w = algo_creators;

                log!(cc::LIGHT_WHITE, "Refreshed creators: {:?} | Diff: {:?}", c_len, diff);
                last_creators = c_len;
            }
            tokio::time::sleep(std::time::Duration::from_secs(60)).await;
        }
    }

    async fn start_session(&self, mint: Pubkey, pool: Pubkey, source: Source, disable_checks: bool, is_mtd: bool) {
        if get_use_regions() {
            let is_region = match self.vacation.is_leader_active_region().await {
                Ok(is_region) => is_region,
                Err(e) => {
                    warn!("start_session_once: {e}");
                    return;
                }
            };
            if !is_region.0 {
                log!("Skipping session because region is not active: {:?}", is_region.1);
                return;
            }
        }
        let max_tokens_at_once = get_max_tokens_at_once();
        if self.holdings.len() >= max_tokens_at_once as usize {
            log!(cc::LIGHT_WHITE, "Skipping session because max tokens at once is reached: {:?}", max_tokens_at_once);
            return;
        }
        match self.holdings.entry(mint) {
            Entry::Occupied(_) => {
                log!(cc::LIGHT_WHITE, "Mint already being sessioned: {:?}", mint);
                return;
            }
            Entry::Vacant(v) => {
                v.insert(Holdings { pool, buy_price: None });
            }
        }

        let me = self.clone();
        tokio::spawn(async move {
            let mint_str = mint.to_string();
            let mode = get_mode();
            if mode == "sim" {
                if let Err(e) = me.sim_session(&mint_str, &source, disable_checks, is_mtd).await {
                    warn!("session error: {e}");
                }
            } else if mode == "trade" {
                if let Err(e) = me.trade_session(&mint_str, &source, disable_checks, is_mtd).await {
                    warn!("session error: {e}");
                }
            }
            me.holdings.remove(&mint);
        });
    }

    pub async fn sim_session(&self, mint: &String, source: &Source, disable_checks: bool, is_mtd: bool) -> anyhow::Result<()> {
        let source = source.clone();
        log!(cc::LIGHT_WHITE, "Sim session: {:?}", source);
        let mut last_price = 0.0;
        let mut last_update = std::time::Instant::now();

        let snipe_config = get_snipe_config();
        let buy_amount = snipe_config.buy_amount; // in SOL
        let chp_lower = snipe_config.chp_lower;
        let chp_upper = snipe_config.chp_upper;
        let tiz_lower = snipe_config.tiz_lower;
        let tiz_upper = snipe_config.tiz_upper;
        let max_loss = snipe_config.max_loss;
        let take_profit = snipe_config.take_profit;
        let max_no_activity_ms = snipe_config.max_no_activity_ms;
        let max_na_on_start_ms = snipe_config.max_na_on_start_ms;
        let min_dev_sold = snipe_config.min_dev_sold;
        let wait_time_creator_buy = 2;
        let strats_config = get_strats_config();
        let mtd_max_loss = strats_config.mtd_max_loss;
        let mtd_take_profit = strats_config.mtd_take_profit;

        let max_loss = if is_mtd {mtd_max_loss} else {max_loss};
        let take_profit = if is_mtd {mtd_take_profit} else {take_profit};
        tokio::time::sleep(std::time::Duration::from_millis(wait_time_creator_buy)).await;

        // check creator hold percent (aka 'chp')
        if snipe_config.use_chp && !disable_checks {
            log!(cc::LIGHT_WHITE, "Reading mint: {:?}", mint);
            let mint = self.mints.read().await.get(&Pubkey::from_str(mint.clone().as_str()).unwrap()).unwrap().clone();
            log!(cc::LIGHT_WHITE, "Creator token amount: {:?}", mint.creator_token_amount);
            let creator_hold_percent = mint.creator_token_amount / TOTAL_SUPPLY as f64;

            if mint.txns_in_zero >= tiz_lower && mint.txns_in_zero <= tiz_upper {
                warn!("Skipping sim session because txns in slot zero aren't in range: {:?} | Possibly bundled: {}", mint.mint, mint.txns_in_zero);
                return Ok(());
            }

            if creator_hold_percent > chp_lower && creator_hold_percent < chp_upper {
                warn!("Skipping sim session because creator hold percent is out of range: {:?}", creator_hold_percent);
                return Ok(());
            }
        }

        
        let snipe_time_ms = 300;
        tokio::time::sleep(std::time::Duration::from_millis(snipe_time_ms)).await;

        // buy
        let mut mint = self.mints.read().await.get(&Pubkey::from_str(mint.clone().as_str()).unwrap()).unwrap().clone();
        mint.buys += 1;
        mint.volume += buy_amount;
        let price = mint.price;
        let tok_amount = self.pump_fun.lamports_to_tokens(buy_amount * 1e9, price);
        log!(cc::LIGHT_WHITE, "Buying {} with {} in return for {} tokens", mint.mint, buy_amount, tok_amount as f64 / 1e6);

        let bc = match mint.is_migrated {
            false => Pools { pump_fun: Some(self.pump_fun.fetch_state(&mint.bonding_curve).await.unwrap()), pump_swap: None },
            true => Pools { pump_fun: None, pump_swap: Some(self.pump_swap.fetch_state(&mint.bonding_curve).await.unwrap()) },
        };

        let mut buy_price = price;
        if let Some(bc) = bc.pump_fun {
            let (vsr, vtr) = (bc.virtual_sol_reserves as f64 / 1e9, bc.virtual_token_reserves as f64 / 1e6);
            let (vsr, vtr) = (vsr + buy_amount, vtr + tok_amount as f64 / 1e6);
            mint.price = vsr / vtr;
            buy_price = vsr / vtr;
            log!(cc::LIGHT_WHITE, "After buying, price: {}", mint.price);
        }

        // throw everything above tiz_lower and below tiz_upper txns in zero
        if snipe_config.use_tiz && !disable_checks {
            if mint.txns_in_zero >= tiz_lower && mint.txns_in_zero <= tiz_upper {
                log!(cc::LIGHT_RED, "Selling Mint because txns in slot zero aren't in range: {:?} | Possibly bundled: {}", mint.mint, mint.txns_in_zero);
                return Ok(());
            }
        }

        {
            let mut map = self.mints.write().await;
            map.insert(mint.mint, mint.clone());
        }

        let mut should_sell = false;
        let mut profit_lapse: Option<Instant> = None;
        let mut recent_profits_diffs: AllocRingBuffer<f64> = AllocRingBuffer::new(10);
        let mut last_profit = 0.0;
        let mut highest_profit = 0.0;
        let mut first_creator_balance = 0.0;

        loop {
            if should_sell {
                break;
            }

            let snapshot = {
                let map = self.mints.read().await;
                match map.get(&mint.mint) {
                    Some(m) => m.clone(),
                    None => {
                        warn!("sim_session: Mint removed: {:?}", mint.mint);
                        break;
                    }
                }
            };

            if first_creator_balance == 0.0 && snapshot.creator_token_amount > 0.0 {
                first_creator_balance = snapshot.creator_token_amount;
            }

            // profit check
            let price = snapshot.price;
            let profit = pct_change(buy_price, price);
            if profit <= -max_loss {
                if profit_lapse.is_some() {
                    if profit_lapse.unwrap().elapsed().as_millis() > 200 {
                        log!(cc::LIGHT_RED, "Mint sold due to loss: {:?} | Profit: {:.6}%", snapshot.mint, profit);
                        should_sell = true;
                        profit_lapse = None;
                    }
                } else {
                    profit_lapse = Some(Instant::now());
                }
            } else if (take_profit > 0.0) && (profit >= take_profit) {
                log!(cc::LIGHT_GREEN, "Mint sold due to profit: {:?} | Profit: {:.6}%", snapshot.mint, profit);
                should_sell = true;
            }

            // activity check
            let token_stage_activity_ms = { if (snapshot.buys < 50 && profit < 10.0) || (snapshot.buys < 20) {max_na_on_start_ms} else {max_no_activity_ms} };
            let elapsed = last_update.elapsed().as_millis();
            if elapsed > token_stage_activity_ms as u128 {
                log!(cc::LIGHT_YELLOW, "Selling Mint because of inactivity (MAX_NA_ON_START_MS: {:?}ms, MAX_NO_ACTIVITY_MS: {:?}ms) | Mint: {:?} | Last Update: {:?} | Last profit recorded: {:.6}%", max_na_on_start_ms, max_no_activity_ms, snapshot.mint, elapsed, last_profit);
                should_sell = true;
            }

            if price == last_price {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                continue;
            };
            // updates 
            last_update = std::time::Instant::now();
            last_price = price;

            recent_profits_diffs.enqueue(profit - last_profit);
            last_profit = profit;

            {
                if recent_profits_diffs.is_full() {
                    let recent_profits = recent_profits_diffs.to_vec()
                        .iter().sum::<f64>();
                    if (recent_profits < -20.0 && profit < 100.0)
                    || (profit >= 200.0 && highest_profit / 2.0 > profit) {
                        log!(cc::LIGHT_RED, "Selling Mint because recent profits is too low: {:?} | Check recent profits: {:.6}%", snapshot.mint, recent_profits);
                        should_sell = true;
                    }
                }
            }


            if profit > highest_profit {
                highest_profit = profit;
            }

            // log!(cc::LIGHT_GRAY, "Recent profits diffs: {:?}", recent_profits_diffs.to_vec());

            // dev sold check
            if snapshot.creator_sold && first_creator_balance > min_dev_sold as f64 {
                log!(cc::LIGHT_WHITE, "Mint: {:?} | Dev Sold: {} | Profit: {:.6}%", snapshot.mint, snapshot.creator_sold, profit);
                should_sell = true;
            }

            log!(
                cc::ORANGE,
                "Mint: {:?}\n Price: {:#.10}\n Dev Sold: {}\n Dev Token Amount: {}\n Buys: {}\n Sells: {}\n Volume: {}\n Liquidity: {}\n Txns in n: {}\n Txns in zero: {}\n First Seen: {}\n Slot: {}\n Source: {:?}\n Profit: {:.6}%\n",
                snapshot.mint,
                snapshot.price,
                snapshot.creator_sold,
                snapshot.creator_token_amount,
                snapshot.buys,
                snapshot.sells,
                snapshot.volume,
                snapshot.liquidity,
                snapshot.txns_in_n,
                snapshot.txns_in_zero,
                snapshot.first_seen_slot,
                snapshot.last_seen_slot,
                source,
                profit
            );

            let sim = Sim {
                mint: snapshot.mint.to_string(),
                bonding_curve: snapshot.bonding_curve.to_string(),
                price: snapshot.price,
                name: snapshot.name,
                symbol: snapshot.symbol,
                uri: snapshot.uri,
                creator: snapshot.creator.to_string(),
                creator_sold: snapshot.creator_sold,
                creator_token_amount: snapshot.creator_token_amount,
                buys: snapshot.buys,
                sells: snapshot.sells,
                volume: snapshot.volume,
                liquidity: snapshot.liquidity,
                txns_in_zero: snapshot.txns_in_zero,
                first_seen_slot: snapshot.first_seen_slot as i64,
                last_seen_slot: snapshot.last_seen_slot as i64,
                profit: profit,
                highest_profit: highest_profit,
            };

            self.goldmine.upsert_sim(&sim).await.unwrap();
        }
        Ok(())
    }

    pub async fn buy(
        &self,
        mint: &String,
        current_pool: &String,
        creator: &String,
        sol_amount_in: f64,
        slippage: f64,
        price: f64,
        use_idempotent: Option<bool>,
        market: Market,
    ) -> anyhow::Result<(bool, Signature)> {
        let mint = Pubkey::from_str(mint.clone().as_str()).unwrap();
        let current_pool = Pubkey::from_str(current_pool.clone().as_str()).unwrap();
        let creator = Pubkey::from_str(creator.clone().as_str()).unwrap();
        let tx_settings = get_tx_settings();

        let (ixs, fee) = match market {
            Market::PumpSwap => self.pump_swap.buy(
                &mint,
                &current_pool,
                &creator,
                sol_amount_in,
                slippage,
                price,
                use_idempotent,
            ).await?,
            Market::PumpFun => self.pump_fun.buy(
                &mint,
                &current_pool,
                &creator,
                sol_amount_in,
                slippage,
                price,
                use_idempotent,
            ).await?,
        };
        
        let mut tx: Result<solana_signature::Signature, anyhow::Error> = Ok(Signature::default());
        if tx_settings.tx_strat == "swqos" {
            let nb_clients = NextBlock::multiple_new(&NB_ENDPOINTS, tx_settings.nextblock_key);
            let jito_clients = &get_jito_clients();
            let helius_clients = &HeliusSender::multiple_new(&HELIUS_SENDER_ENDPOINTS);
            let zs_clients = ZeroSlot::multiple_new(&ZERO_SLOT_ENDPOINTS, tx_settings.zero_slot_key);
            let temporal_clients = &TemporalSender::multiple_new(&TEMPORAL_HTTP_ENDPOINTS, tx_settings.temporal_key);
            let blox_clients = Bloxroute::multiple_new(&BLOX_ENDPOINTS, tx_settings.blox_key);
            let tip_lamports = tx_settings.tip_lamports;
            
            let nonce_account = if get_nonce_account().is_some() && get_nonce_account().unwrap().len() > 0 {
                Some(Pubkey::from_str(&get_nonce_account().unwrap()).unwrap())
            } else {
                None
            };

            tx = self.sol_hook.spray_with_all(
                ixs, 
                &self.pump_fun.keypair, 
                fee, 
                &nb_clients, 
                jito_clients, 
                helius_clients, 
                &zs_clients, 
                temporal_clients, 
                &blox_clients, 
                tip_lamports, 
                None, 
                nonce_account
            ).await;
        } 
        else if tx_settings.tx_strat == "rpc" {
            tx = self.sol_hook.send(ixs, &self.pump_fun.keypair, fee, None).await;
        }
        match tx {
            Ok(sig) => {
                log!(cc::LIGHT_WHITE, "Sig: https://solscan.io/tx/{:?}", sig);
                return Ok((true, sig));
            },
            Err(e) => {
                warn!("Error sending tx: {e}");
                return Ok((false, Signature::default()));
            }
        }
    }

    pub async fn sell(
        &self,
        mint: &String,
        current_pool: &String,
        creator: &String,
        sell_pct: u64,
        slippage: f64,
        price: f64,
        market: Market,
        retries: u32,
    ) -> anyhow::Result<(bool, Signature)> {
        let mint = Pubkey::from_str(mint.clone().as_str()).unwrap();
        let current_pool = Pubkey::from_str(current_pool.clone().as_str()).unwrap();
        let creator = Pubkey::from_str(creator.clone().as_str()).unwrap();
        let sell_pct = sell_pct;
        let slippage = slippage;
        let price = price;

        let (ixs, fee) = match market {
            Market::PumpSwap => self.pump_swap.sell(
                &mint,
                &current_pool,
                &creator,
                sell_pct,
                slippage,
                price,
            ).await?,
            Market::PumpFun => self.pump_fun.sell(
                &mint,
                &current_pool,
                &creator,
                sell_pct,
                slippage,
                price,
            ).await?,
        };
        let tx = self.sol_hook.send(ixs.clone(), &self.pump_fun.keypair, fee, None).await;
        let mut tries = 0;
        match tx {
            Ok(sig) => {
                log!(cc::LIGHT_WHITE, "Sig: https://solscan.io/tx/{:?}", sig);
                return Ok((true, sig));
            },
            Err(e) => {
                warn!("Error sending tx: {e}");
                loop {
                    if tries < retries {
                        let tx = self.sol_hook.send(ixs.clone(), &self.pump_fun.keypair, fee, None).await;
                        if tx.is_ok() {
                            return Ok((true, tx.unwrap()));
                        }
                        tries += 1;
                    } else {
                        break;
                    }
                }
                return Ok((false, Signature::default()));
            }
        }
    }

    pub async fn trade_session(&self, mint: &String, source: &Source, disable_checks: bool, is_mtd: bool) -> anyhow::Result<()> {
        let source = source.clone();
        log!(cc::LIGHT_WHITE, "Trade session: {:?}", source);
        let mut last_price = 0.0;
        let mut last_update = std::time::Instant::now();

        let snipe_config = get_snipe_config();
        let buy_amount = snipe_config.buy_amount; // in SOL
        let chp_lower = snipe_config.chp_lower;
        let chp_upper = snipe_config.chp_upper;
        let tiz_lower = snipe_config.tiz_lower;
        let tiz_upper = snipe_config.tiz_upper;
        let max_loss = snipe_config.max_loss;
        let take_profit = snipe_config.take_profit;
        let max_no_activity_ms = snipe_config.max_no_activity_ms;
        let max_na_on_start_ms = snipe_config.max_na_on_start_ms;
        let min_dev_sold = snipe_config.min_dev_sold;
        let wait_time_creator_buy = 2;
        let strats_config = get_strats_config();
        let mtd_max_loss = strats_config.mtd_max_loss;
        let mtd_take_profit = strats_config.mtd_take_profit;
        let slippage = 1.0 + (snipe_config.slippage / 100.0);

        let max_loss = if is_mtd {mtd_max_loss} else {max_loss};
        let take_profit = if is_mtd {mtd_take_profit} else {take_profit};
        tokio::time::sleep(std::time::Duration::from_millis(wait_time_creator_buy)).await;

        // check creator hold percent (aka 'chp')
        if snipe_config.use_chp && !disable_checks {
            log!(cc::LIGHT_WHITE, "Reading mint: {:?}", mint);
            let mint = self.mints.read().await.get(&Pubkey::from_str(mint.clone().as_str()).unwrap()).unwrap().clone();
            log!(cc::LIGHT_WHITE, "Creator token amount: {:?}", mint.creator_token_amount);
            let creator_hold_percent = mint.creator_token_amount / TOTAL_SUPPLY as f64;

            if mint.txns_in_zero >= tiz_lower && mint.txns_in_zero <= tiz_upper {
                warn!("Skipping sim session because txns in slot zero aren't in range: {:?} | Possibly bundled: {}", mint.mint, mint.txns_in_zero);
                return Ok(());
            }

            if creator_hold_percent > chp_lower && creator_hold_percent < chp_upper {
                warn!("Skipping sim session because creator hold percent is out of range: {:?}", creator_hold_percent);
                return Ok(());
            }
        }

        // buy
        let _mint = self.mints.read().await.get(&Pubkey::from_str(mint.clone().as_str()).unwrap()).unwrap().clone();
        let price = _mint.price;
        let tok_amount = self.pump_fun.lamports_to_tokens(buy_amount * 1e9, price);
        log!(cc::LIGHT_WHITE, "Buying {} with {} in return for {} tokens | Is migrated: {}", _mint.mint, buy_amount, tok_amount as f64 / 1e6, _mint.is_migrated);

        let market = match _mint.is_migrated {
            false => Some(Market::PumpFun),
            true => Some(Market::PumpSwap),
        };

        if !self.buy(&_mint.mint.to_string(), &_mint.bonding_curve.to_string(), &_mint.creator.to_string(), buy_amount, slippage, price, Some(false), Market::PumpFun).await?.0 {
            return Ok(());
        }

        let _mint = self.mints.read().await.get(&Pubkey::from_str(mint.clone().as_str()).unwrap()).unwrap().clone();
        let price = _mint.price;
        let buy_price = price;

        // throw everything above tiz_lower and below tiz_upper txns in zero
        if snipe_config.use_tiz && !disable_checks {
            if _mint.txns_in_zero >= tiz_lower && _mint.txns_in_zero <= tiz_upper {
                log!(cc::LIGHT_RED, "Selling Mint because txns in slot zero aren't in range: {:?} | Possibly bundled: {}", _mint.mint, _mint.txns_in_zero);
                self.sell(&_mint.mint.to_string(), &_mint.bonding_curve.to_string(), &_mint.creator.to_string(), 100, slippage, price, market.unwrap(), 3).await?;
                return Ok(());
            }
        }

        let mut should_sell = false;
        let mut profit_lapse: Option<Instant> = None;
        let mut recent_profits_diffs: AllocRingBuffer<f64> = AllocRingBuffer::new(10);
        let mut last_profit = 0.0;
        let mut highest_profit = 0.0;
        let mut first_creator_balance = 0.0;

        loop {
            if should_sell {
                self.sell(&_mint.mint.to_string(), &_mint.bonding_curve.to_string(), &_mint.creator.to_string(), 100, slippage, price, market.unwrap(), 3).await?;
                break;
            }

            let snapshot = {
                let map = self.mints.read().await;
                match map.get(&_mint.mint) {
                    Some(m) => m.clone(),
                    None => {
                        warn!("sim_session: Mint removed: {:?}", _mint.mint);
                        break;
                    }
                }
            };

            if first_creator_balance == 0.0 && snapshot.creator_token_amount > 0.0 {
                first_creator_balance = snapshot.creator_token_amount;
            }

            // profit check
            let price = snapshot.price;
            let profit = pct_change(buy_price, price);
            if profit <= -max_loss {
                if profit_lapse.is_some() {
                    if profit_lapse.unwrap().elapsed().as_millis() > 200 {
                        log!(cc::LIGHT_RED, "Mint sold due to loss: {:?} | Profit: {:.6}%", snapshot.mint, profit);
                        should_sell = true;
                        profit_lapse = None;
                    }
                } else {
                    profit_lapse = Some(Instant::now());
                }
            } else if (take_profit > 0.0) && (profit >= take_profit) {
                log!(cc::LIGHT_GREEN, "Mint sold due to profit: {:?} | Profit: {:.6}%", snapshot.mint, profit);
                should_sell = true;
            }

            // activity check
            let token_stage_activity_ms = { if (snapshot.buys < 50 && profit < 10.0) || (snapshot.buys < 20) {max_na_on_start_ms} else {max_no_activity_ms} };
            let elapsed = last_update.elapsed().as_millis();
            if elapsed > token_stage_activity_ms as u128 {
                log!(cc::LIGHT_YELLOW, "Selling Mint because of inactivity (MAX_NA_ON_START_MS: {:?}ms, MAX_NO_ACTIVITY_MS: {:?}ms) | Mint: {:?} | Last Update: {:?} | Last profit recorded: {:.6}%", max_na_on_start_ms, max_no_activity_ms, snapshot.mint, elapsed, last_profit);
                should_sell = true;
            }

            if price == last_price {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                continue;
            };
            // updates 
            last_update = std::time::Instant::now();
            last_price = price;

            recent_profits_diffs.enqueue(profit - last_profit);
            last_profit = profit;

            {
                if recent_profits_diffs.is_full() {
                    let recent_profits = recent_profits_diffs.to_vec()
                        .iter().sum::<f64>();
                    if (recent_profits < -20.0 && profit < 100.0)
                    || (profit >= 200.0 && highest_profit / 2.0 > profit) {
                        log!(cc::LIGHT_RED, "Selling Mint because recent profits is too low: {:?} | Check recent profits: {:.6}%", snapshot.mint, recent_profits);
                        should_sell = true;
                    }
                }
            }


            if profit > highest_profit {
                highest_profit = profit;
            }

            // log!(cc::LIGHT_GRAY, "Recent profits diffs: {:?}", recent_profits_diffs.to_vec());

            // dev sold check
            if snapshot.creator_sold && first_creator_balance > min_dev_sold as f64 {
                log!(cc::LIGHT_WHITE, "Mint: {:?} | Dev Sold: {} | Profit: {:.6}%", snapshot.mint, snapshot.creator_sold, profit);
                should_sell = true;
            }

            log!(
                cc::ORANGE,
                "Mint: {:?}\n Price: {:#.10}\n Dev Sold: {}\n Dev Token Amount: {}\n Buys: {}\n Sells: {}\n Volume: {}\n Liquidity: {}\n Txns in n: {}\n Txns in zero: {}\n First Seen: {}\n Slot: {}\n Source: {:?}\n Profit: {:.6}%\n",
                snapshot.mint,
                snapshot.price,
                snapshot.creator_sold,
                snapshot.creator_token_amount,
                snapshot.buys,
                snapshot.sells,
                snapshot.volume,
                snapshot.liquidity,
                snapshot.txns_in_n,
                snapshot.txns_in_zero,
                snapshot.first_seen_slot,
                snapshot.last_seen_slot,
                source,
                profit
            );

            let sim = Sim {
                mint: snapshot.mint.to_string(),
                bonding_curve: snapshot.bonding_curve.to_string(),
                price: snapshot.price,
                name: snapshot.name,
                symbol: snapshot.symbol,
                uri: snapshot.uri,
                creator: snapshot.creator.to_string(),
                creator_sold: snapshot.creator_sold,
                creator_token_amount: snapshot.creator_token_amount,
                buys: snapshot.buys,
                sells: snapshot.sells,
                volume: snapshot.volume,
                liquidity: snapshot.liquidity,
                txns_in_zero: snapshot.txns_in_zero,
                first_seen_slot: snapshot.first_seen_slot as i64,
                last_seen_slot: snapshot.last_seen_slot as i64,
                profit: profit,
                highest_profit: highest_profit,
            };

            self.goldmine.upsert_sim(&sim).await.unwrap();
        }
        Ok(())
    }
    
    pub async fn dip_session(&self, mint: &String) -> anyhow::Result<()> {
        let mut stable_time = None;

        let strats_config = get_strats_config();
        let mtd_pct = strats_config.mtd_pct;
        let mtd_stable_time = strats_config.mtd_stable_time;
        let mut recent_profits_diffs = AllocRingBuffer::new(5);
        let mut last_profit = 0.0;
        let mut first_price = 0.0;
        let mut dip_lowest_price = 0.0;
        let mtd_min_mc = strats_config.mtd_min_mc;

        loop {
            let snapshot: MewMint = {
                let map = self.mints.read().await;
                match map.get(&Pubkey::from_str(mint.as_str()).unwrap()) {
                    Some(m) => m.clone(),
                    None => {
                        warn!("dip_session: Mint removed: {:?}", mint);
                        break;
                    }
                }
            };

            let price = snapshot.price;
            if first_price == 0.0 {
                first_price = price;
            }
            let highest_price = snapshot.highest_price;
            let dip_lvl = highest_price / (1.0 + mtd_pct as f64 / 100.0);
            if price < dip_lvl && stable_time.is_none() {
                dip_lowest_price = price;
                stable_time = Some(Instant::now());
            }
            if recent_profits_diffs.len() == 10 && stable_time.is_some() && stable_time.unwrap().elapsed().as_secs() > mtd_stable_time as u64 {
                let recent_profits = recent_profits_diffs.to_vec().iter().sum::<f64>();
                if price < dip_lvl && recent_profits > 0.0 && snapshot.liquidity > mtd_min_mc {
                    log!(cc::LIGHT_WHITE, "Mint: {:?} | Stable time: {:?} | Price: {:.10} | Dip level: {:.10} | Highest price: {:.10} | Recent profits: {:?}", mint, stable_time.unwrap().elapsed(), price, dip_lvl, highest_price, recent_profits_diffs.to_vec());
                    let me = self.clone();
                    let mint = mint.clone();
                    tokio::spawn(async move {
                        let mode = get_mode();
                        if mode == "sim" {
                            if let Err(e) = me.sim_session(&mint, &Source::Dip, true, true).await {
                                warn!("dip_session error: {e}");
                            }
                        } else if mode == "trade" {
                            if let Err(e) = me.trade_session(&mint, &Source::Dip, true, true).await {
                                warn!("dip_session error: {e}");
                            }
                        }
                    });
                    break;
                }
                else if price < dip_lvl && price < dip_lowest_price {
                    dip_lowest_price = price;
                    stable_time = None;
                }
            }
            let profit = pct_change(price, first_price);
            if profit != last_profit {
                let diff = profit - last_profit;
                recent_profits_diffs.enqueue(diff);
                last_profit = profit;
            }

            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }
        
        Ok(())
    }

    pub async fn subscribe_grpc_pump_fun(&self) -> anyhow::Result<()> {
        // let buy_amount = config().buy_amount;
        let grpc_url = self.grpc_url.clone();
        let grpc_token = self.grpc_token.clone();

        let mut grpc_client = SolHook::grpc_connect(&grpc_url, Some(&grpc_token)).await.unwrap();
        let pump_address = Pubkey::from_str("6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P").unwrap();
        let accounts = vec![pump_address];
        let mut stream = SolHook::grpc_subscribe_transactions(&mut grpc_client, &accounts).await.unwrap();

        log!(cc::LIGHT_WHITE, "Subscribed to Pump.fun using gRPC");

        let strats_config = get_strats_config();
        let enable_abs = strats_config.enable_abs;
        let abs_min_buys = strats_config.abs_min_buys;
        let abs_min_volume = strats_config.abs_min_volume;
        let abs_n = strats_config.abs_n;
        let enable_dbf = strats_config.enable_dbf;
        let dbf_max_chp = strats_config.dbf_max_chp;

        while let Some(msg) = stream.next().await {
            let update = match msg {
                Ok(u) => u,
                Err(e) => { warn!("grpc error: {e}"); continue; }
            };

            let Some(UpdateOneof::Transaction(tx_update)) = update.update_oneof else { continue };

            let Some(SubscribeUpdateTransactionInfo {
                meta: Some(TransactionStatusMeta { log_messages, .. }),
                ..
            }) = tx_update.transaction.clone() else { continue };

            let sig: Vec<u8> = tx_update.transaction.clone().unwrap().signature;
            let sig_str = SolHook::parse_signature(&sig).unwrap().to_string();

            let events = PumpFun::parse_logs(log_messages.iter(), Some(&sig_str));

            for event in events {
                match event {
                    PumpFunEvent::Trade(Some(trade)) => {
                        let mints = Arc::clone(&self.mints);
                        let me = self.clone();

                        tokio::spawn(async move {
                            // wait 2ms so create arm can index the trade
                            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
                            
                            let mut map = mints.write().await;
                            if let Some(mut mint) = map.get(&trade.mint).cloned() {
                                let user = trade.user;
                                let price = PumpFun::get_price(&trade);
                                if price > mint.highest_price {
                                    mint.highest_price = price;
                                }
                                let creator = mint.creator;

                                if trade.is_buy {
                                    mint.buys += 1;
                                    mint.volume += trade.sol_amount as f64 / 1e9;
                                } else { 
                                    mint.sells += 1;
                                    mint.volume += trade.sol_amount as f64 / 1e9;
                                }

                                let tok_amt: f64 = trade.token_amount as f64 / 1e6;
                                if user == creator {
                                    if trade.is_buy {
                                        mint.creator_token_amount += tok_amt;
                                    } else {
                                        let dev_sell_pct =  if mint.creator_token_amount >= tok_amt && mint.creator_token_amount > 0.0 { tok_amt / mint.creator_token_amount } else { 0.0 };
                                        if dev_sell_pct >= 0.5 {
                                            mint.creator_sold = true;
                                        }    
                                        mint.creator_token_amount -= tok_amt;
                                        if mint.creator_token_amount <= 0.0 {
                                            mint.creator_sold = true;
                                        }
                                    }
                                }
                                if tx_update.slot == mint.first_seen_slot {
                                    mint.txns_in_zero += 1;
                                }

                                if enable_dbf {
                                    let time = timestamp_now();
                                    if time < mint.created_time + 1.0 {
                                        let me = me.clone();
                                        tokio::spawn(async move {
                                            loop {
                                                let time = timestamp_now();
                                                let mint = me.mints.read().await.get(&trade.mint).unwrap().clone();
                                                if mint.created_time + 0.1 < time && time < mint.created_time + 1.0 {
                                                    if (mint.creator_token_amount / TOTAL_SUPPLY as f64) < dbf_max_chp {
                                                        if mint.txns_in_zero <= 1 && mint.buys <= 1 {
                                                            let me = me.clone();
                                                            let mint_pk = mint.mint;
                                                            let pool = mint.bonding_curve;
                                                            tokio::spawn(async move {
                                                                me.start_session(mint_pk, pool, Source::DevBestFriend, true, false).await;
                                                            });
                                                            break;
                                                        }
                                                    }
                                                }
                                                if time > mint.created_time + 1.0 {
                                                    break;
                                                }
                                                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                                            }
                                        });
                                    }
                                }

                                if enable_abs {
                                    let time = timestamp_now();
                                    if mint.created_time + 0.1 < time {
                                        if tx_update.slot < mint.first_seen_slot + abs_n as u64 {
                                            mint.txns_in_n += 1;
                                            mint.volume_in_n += trade.sol_amount as f64 / 1e9;
                                            //log!(cc::LIGHT_WHITE, "Mint: {:?} | User: {:?} | Txns in n: {:?}", mint.mint, user, mint.txns_in_n);
                                            if mint.txns_in_n >= abs_min_buys || mint.volume_in_n >= abs_min_volume {
                                                let me = me.clone();
                                                let mint_pk = mint.mint;
                                                let pool = mint.bonding_curve;
                                                tokio::spawn(async move {
                                                    me.start_session(mint_pk, pool, Source::Digged, true, false).await;
                                                });
                                            }
                                        }
                                    }
                                }
                                mint.price = price;
                                mint.last_seen_slot = tx_update.slot;
                                mint.liquidity = trade.real_sol_reserves as f64 / 1e9;
                                map.insert(trade.mint, mint);
                            }
                        })
                    },

                    PumpFunEvent::Create(Some(create)) => {
                        let mints = Arc::clone(&self.mints);
                        let goldmine = Arc::clone(&self.goldmine);
                        let me = self.clone();
                        let creators = self.creators.read().await.clone();

                        tokio::spawn(async move {
                            let name = create.name.clone();
                            let symbol = create.symbol.clone();
                            let uri = create.uri.clone();
                            let creator = create.creator;
                            let created_time = std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .expect("Time error.");
                            let ts: f64 = created_time.as_secs() as f64 + f64::from(created_time.subsec_nanos()) * 1e-9;

                            let price = PumpFun::get_open_price(&create);
                            log!(cc::LIGHT_GRAY, "Mint: {:?} at slot: {:?}", create.mint, tx_update.slot);
                            let new_mint = MewMint {
                                mint: create.mint,
                                bonding_curve: create.bonding_curve,
                                price,
                                highest_price: price,
                                name: name.clone(),
                                symbol: symbol.clone(),
                                uri: uri.clone(),
                                creator,
                                creator_sold: false,
                                creator_token_amount: 0.0,
                                buys: 0,
                                sells: 0,
                                volume: 0.0,
                                txns_in_zero: 0,
                                txns_in_n: 0,
                                volume_in_n: 0.0,
                                first_seen_slot: tx_update.slot,
                                last_seen_slot: tx_update.slot,
                                liquidity: 0.0,
                                is_migrated: false,
                                created_time: ts,
                            };

                            let mut map = mints.write().await;
                            map.insert(create.mint, new_mint.clone());

                            if goldmine.get_dupes(&name, &symbol, &uri).await.unwrap().is_some() {
                                return;
                            }
                            // TODO: Uncomment this when we have a way to blacklist creators
                            // if goldmine.blacklist_check(&creator.to_string()).await.unwrap() {
                            //     return;
                            // }

                            if let Some((_, creator_source)) = creators.iter().find(|(c, _)| c == &creator.to_string()) {
                                log!(
                                    cc::LIGHT_WHITE,
                                    "Mew mint {:?} from creator {:?}\n Source: {:?}",
                                    new_mint.mint,
                                    creator,
                                    creator_source
                                );
                                let src = creator_source.clone();

                                let me = me.clone();
                                tokio::spawn(async move {
                                    me.start_session(new_mint.mint, new_mint.bonding_curve, src, false, false).await;
                                });
                            }

                            // let socials = match me.pump_fun.check_socials(&create.uri, 5).await {
                            //     Ok(s) => s,
                            //     Err(e) => {
                            //         warn!("check_socials error: {e}");
                            //         return;
                            //     }
                            // };
                            // log!(cc::LIGHT_WHITE, "Socials: {:?}", socials);

                            // let mut twitter_score = 0;
                            // if let Some(twitter) = socials.twitter {
                            //     if let Some(handle) = sanitize_twitter_input(&twitter) {
                            //         match me.goldmine.get_twitter(&handle).await {
                            //             Ok(Some(twitter)) => {
                            //                 let mut creators = twitter.creators;
                            //                 creators.push(creator.to_string());
                            //                 me.goldmine.upsert_twitters(&handle, twitter.score, creators).await.unwrap();
                            //                 log!(cc::LIGHT_WHITE, "Existing twitter handle: {:?}", handle);
                            //                 twitter_score = twitter.score;
                            //             }
                            //             Ok(None) => {
                            //                 let score = fetch_twitter_score(&me.client, &handle).await;
                            //                 log!(cc::LIGHT_GRAY, "Twitter score: {:?}", score);
                            //                 twitter_score = score;
                            //                 me.goldmine.upsert_twitters(&handle, score, Json(vec![creator.to_string()])).await.unwrap();
                            //                 log!(cc::LIGHT_WHITE, "Upserted twitter handle: {:?}", handle);
                            //             }
                            //             Err(e) => {
                            //                 warn!("get_twitter error: {e}");
                            //             }
                            //         }

                            //     } else {
                            //         log!(cc::LIGHT_WHITE, "Skipping non-user Twitter link: {:?}", twitter);
                            //     }
                            // }
                            // if twitter_score > 1000 && twitter_score < 10000 {
                            //     log!(cc::LIGHT_CYAN, "Buying mint {:?} from creator {:?} because twitter score is {:?}", create.mint, creator, twitter_score);
                            //     let me = me.clone();
                            //     let mint = create.mint.clone().to_string();
                            //     tokio::spawn(async move {
                            //         me.holdings.insert(create.mint, Holdings {
                            //             pool: create.bonding_curve,
                            //             buy_price: None,
                            //         });
                            //         log!(cc::LIGHT_WHITE, "Holdings: {:?}", me.holdings.clone());
                            //         if let Err(e) = me.sim_session(&mint, &Source::Twitter).await {
                            //             warn!("sim_session error: {e}");
                            //         }
                            //         me.holdings.remove(&create.mint);
                            //         log!(cc::LIGHT_WHITE, "Holdings: {:?}", me.holdings.clone());
                            //     });
                            // }
                        })
                    },
                    _ => { tokio::spawn(async move { return; }) },
                };
            }
        }
        Ok(())
    }

    pub async fn subscribe_ws_pump_fun(&self) -> anyhow::Result<()> {
        // let buy_amount = config().buy_amount;
        let ws_url = config().ws_url.clone();

        let (mut rx, _handle) = self.sol_hook.subscribe_logs_channel(&ws_url, RpcTransactionLogsFilter::Mentions(vec![Pubkey::from_str("6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P").unwrap().to_string()]), CommitmentConfig::processed()).await.unwrap();
        log!(cc::LIGHT_WHITE, "Subscribed to Pump.fun using WS");

        let strats_config = get_strats_config();
        let enable_abs = strats_config.enable_abs;
        let abs_min_buys = strats_config.abs_min_buys;
        let abs_min_volume = strats_config.abs_min_volume;
        let abs_n = strats_config.abs_n;
        let enable_dbf = strats_config.enable_dbf;
        let dbf_max_chp = strats_config.dbf_max_chp;

        while let Some(msg) = rx.recv().await {
            let sig = msg.signature.clone();
            let slot = self.sol_hook.rpc_client.get_slot_with_commitment(CommitmentConfig::processed()).await.unwrap();
            let events = PumpFun::parse_logs(msg.logs.iter(), Some(&sig));

            for event in events {
                match event {
                    PumpFunEvent::Trade(Some(trade)) => {
                        let mints = Arc::clone(&self.mints);
                        let me = self.clone();

                        tokio::spawn(async move {
                            // wait 2ms so create arm can index the trade
                            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
                            
                            let mut map = mints.write().await;
                            if let Some(mut mint) = map.get(&trade.mint).cloned() {
                                let user = trade.user;
                                let price = PumpFun::get_price(&trade);
                                if price > mint.highest_price {
                                    mint.highest_price = price;
                                }
                                let creator = mint.creator;

                                if trade.is_buy {
                                    mint.buys += 1;
                                    mint.volume += trade.sol_amount as f64 / 1e9;
                                } else { 
                                    mint.sells += 1;
                                    mint.volume += trade.sol_amount as f64 / 1e9;
                                }

                                let tok_amt: f64 = trade.token_amount as f64 / 1e6;
                                if user == creator {
                                    if trade.is_buy {
                                        mint.creator_token_amount += tok_amt;
                                    } else {
                                        let dev_sell_pct =  if mint.creator_token_amount >= tok_amt && mint.creator_token_amount > 0.0 { tok_amt / mint.creator_token_amount } else { 0.0 };
                                        if dev_sell_pct >= 0.5 {
                                            mint.creator_sold = true;
                                        }    
                                        mint.creator_token_amount -= tok_amt;
                                        if mint.creator_token_amount <= 0.0 {
                                            mint.creator_sold = true;
                                        }
                                    }
                                }
                                if slot == mint.first_seen_slot {
                                    mint.txns_in_zero += 1;
                                }

                                if enable_dbf {
                                    let time = timestamp_now();
                                    if time < mint.created_time + 1.0 {
                                        let me = me.clone();
                                        tokio::spawn(async move {
                                            loop {
                                                let time = timestamp_now();
                                                let mint = me.mints.read().await.get(&trade.mint).unwrap().clone();
                                                if mint.created_time + 0.1 < time && time < mint.created_time + 1.0 {
                                                    if (mint.creator_token_amount / TOTAL_SUPPLY as f64) < dbf_max_chp {
                                                        if mint.txns_in_zero <= 1 && mint.buys <= 1 {
                                                            let me = me.clone();
                                                            let mint_pk = mint.mint;
                                                            let pool = mint.bonding_curve;
                                                            tokio::spawn(async move {
                                                                me.start_session(mint_pk, pool, Source::DevBestFriend, true, false).await;
                                                            });
                                                            break;
                                                        }
                                                    }
                                                }
                                                if time > mint.created_time + 1.0 {
                                                    break;
                                                }
                                                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                                            }
                                        });
                                    }
                                }

                                if enable_abs {
                                    let time = timestamp_now();
                                    if mint.created_time + 0.1 < time {
                                        if slot < mint.first_seen_slot + abs_n as u64 {
                                            mint.txns_in_n += 1;
                                            mint.volume_in_n += trade.sol_amount as f64 / 1e9;
                                            //log!(cc::LIGHT_WHITE, "Mint: {:?} | User: {:?} | Txns in n: {:?}", mint.mint, user, mint.txns_in_n);
                                            if mint.txns_in_n >= abs_min_buys || mint.volume_in_n >= abs_min_volume {
                                                let me = me.clone();
                                                let mint_pk = mint.mint;
                                                let pool = mint.bonding_curve;
                                                tokio::spawn(async move {
                                                    me.start_session(mint_pk, pool, Source::Digged, true, false).await;
                                                });
                                            }
                                        }
                                    }
                                }
                                mint.price = price;
                                mint.last_seen_slot = slot;
                                mint.liquidity = trade.real_sol_reserves as f64 / 1e9;
                                map.insert(trade.mint, mint);
                            }
                        })
                    },

                    PumpFunEvent::Create(Some(create)) => {
                        let mints = Arc::clone(&self.mints);
                        let goldmine = Arc::clone(&self.goldmine);
                        let me = self.clone();
                        let creators = self.creators.read().await.clone();

                        tokio::spawn(async move {
                            let name = create.name.clone();
                            let symbol = create.symbol.clone();
                            let uri = create.uri.clone();
                            let creator = create.creator;
                            let created_time = std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .expect("Time error.");
                            let ts: f64 = created_time.as_secs() as f64 + f64::from(created_time.subsec_nanos()) * 1e-9;

                            let price = PumpFun::get_open_price(&create);
                            log!(cc::LIGHT_GRAY, "Mint: {:?} at slot: {:?}", create.mint, slot);
                            let new_mint = MewMint {
                                mint: create.mint,
                                bonding_curve: create.bonding_curve,
                                price,
                                highest_price: price,
                                name: name.clone(),
                                symbol: symbol.clone(),
                                uri: uri.clone(),
                                creator,
                                creator_sold: false,
                                creator_token_amount: 0.0,
                                buys: 0,
                                sells: 0,
                                volume: 0.0,
                                txns_in_zero: 0,
                                txns_in_n: 0,
                                volume_in_n: 0.0,
                                first_seen_slot: slot,
                                last_seen_slot: slot,
                                liquidity: 0.0,
                                is_migrated: false,
                                created_time: ts,
                            };

                            let mut map = mints.write().await;
                            map.insert(create.mint, new_mint.clone());

                            if goldmine.get_dupes(&name, &symbol, &uri).await.unwrap().is_some() {
                                return;
                            }
                            // TODO: Uncomment this when we have a way to blacklist creators
                            // if goldmine.blacklist_check(&creator.to_string()).await.unwrap() {
                            //     return;
                            // }

                            if let Some((_, creator_source)) = creators.iter().find(|(c, _)| c == &creator.to_string()) {
                                log!(
                                    cc::LIGHT_WHITE,
                                    "Mew mint {:?} from creator {:?}\n Source: {:?}",
                                    new_mint.mint,
                                    creator,
                                    creator_source
                                );
                                let src = creator_source.clone();

                                let me = me.clone();
                                tokio::spawn(async move {
                                    me.start_session(new_mint.mint, new_mint.bonding_curve, src, false, false).await;
                                });
                            }

                            // let socials = match me.pump_fun.check_socials(&create.uri, 5).await {
                            //     Ok(s) => s,
                            //     Err(e) => {
                            //         warn!("check_socials error: {e}");
                            //         return;
                            //     }
                            // };
                            // log!(cc::LIGHT_WHITE, "Socials: {:?}", socials);

                            // let mut twitter_score = 0;
                            // if let Some(twitter) = socials.twitter {
                            //     if let Some(handle) = sanitize_twitter_input(&twitter) {
                            //         match me.goldmine.get_twitter(&handle).await {
                            //             Ok(Some(twitter)) => {
                            //                 let mut creators = twitter.creators;
                            //                 creators.push(creator.to_string());
                            //                 me.goldmine.upsert_twitters(&handle, twitter.score, creators).await.unwrap();
                            //                 log!(cc::LIGHT_WHITE, "Existing twitter handle: {:?}", handle);
                            //                 twitter_score = twitter.score;
                            //             }
                            //             Ok(None) => {
                            //                 let score = fetch_twitter_score(&me.client, &handle).await;
                            //                 log!(cc::LIGHT_GRAY, "Twitter score: {:?}", score);
                            //                 twitter_score = score;
                            //                 me.goldmine.upsert_twitters(&handle, score, Json(vec![creator.to_string()])).await.unwrap();
                            //                 log!(cc::LIGHT_WHITE, "Upserted twitter handle: {:?}", handle);
                            //             }
                            //             Err(e) => {
                            //                 warn!("get_twitter error: {e}");
                            //             }
                            //         }

                            //     } else {
                            //         log!(cc::LIGHT_WHITE, "Skipping non-user Twitter link: {:?}", twitter);
                            //     }
                            // }
                            // if twitter_score > 1000 && twitter_score < 10000 {
                            //     log!(cc::LIGHT_CYAN, "Buying mint {:?} from creator {:?} because twitter score is {:?}", create.mint, creator, twitter_score);
                            //     let me = me.clone();
                            //     let mint = create.mint.clone().to_string();
                            //     tokio::spawn(async move {
                            //         me.holdings.insert(create.mint, Holdings {
                            //             pool: create.bonding_curve,
                            //             buy_price: None,
                            //         });
                            //         log!(cc::LIGHT_WHITE, "Holdings: {:?}", me.holdings.clone());
                            //         if let Err(e) = me.sim_session(&mint, &Source::Twitter).await {
                            //             warn!("sim_session error: {e}");
                            //         }
                            //         me.holdings.remove(&create.mint);
                            //         log!(cc::LIGHT_WHITE, "Holdings: {:?}", me.holdings.clone());
                            //     });
                            // }
                        })
                    },
                    _ => { tokio::spawn(async move { return; }) },
                };
            }
        }
        Ok(())
    }

    pub async fn subscribe_grpc_pump_swap(&self) -> anyhow::Result<()> {
        // let buy_amount = config().buy_amount;
        let grpc_url = self.grpc_url.clone();
        let grpc_token = self.grpc_token.clone();

        let mut grpc_client = SolHook::grpc_connect(&grpc_url, Some(&grpc_token)).await.unwrap();
        let pump_address = Pubkey::from_str("pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA").unwrap();
        let accounts = vec![pump_address];
        let mut stream = SolHook::grpc_subscribe_transactions(&mut grpc_client, &accounts).await.unwrap();

        log!(cc::LIGHT_WHITE, "Subscribed to PumpSwap using gRPC");

        while let Some(msg) = stream.next().await {
            let update = match msg {
                Ok(u) => u,
                Err(e) => { warn!("grpc error: {e}"); continue; }
            };

            let Some(UpdateOneof::Transaction(tx_update)) = update.update_oneof else { continue };

            let Some(SubscribeUpdateTransactionInfo {
                meta: Some(TransactionStatusMeta { log_messages, .. }),
                ..
            }) = tx_update.transaction.clone() else { continue };

            let sig: Vec<u8> = tx_update.transaction.clone().unwrap().signature;
            let sig_str = SolHook::parse_signature(&sig).unwrap().to_string();

            let events = PumpSwap::parse_logs(log_messages.iter(), Some(&sig_str));

            let me = self.clone();

            tokio::spawn(async move {
                for event in events {
                    match event {
                        PumpSwapEvent::CreatePool(Some(create)) => {
                            if create.base_mint == WSOL_MINT {
                                continue;
                            }
                            // log!(cc::LIGHT_WHITE, "Create pool: {:?}", create);
                            if me.holdings.contains_key(&create.base_mint) && create.quote_mint == WSOL_MINT {
                                log!(cc::LIGHT_WHITE, "Token from holdings has migrated! {:?}", create.base_mint);
                                let mut mew_mint = me.mints.read().await.get(&Pubkey::from_str(create.base_mint.clone().to_string().as_str()).unwrap()).unwrap().clone();
                                mew_mint.is_migrated = true;
                                me.mints.write().await.insert(create.base_mint, mew_mint);
                            }
                            if create.quote_mint == WSOL_MINT && create.quote_amount_in >= 83990359346 && create.base_amount_in == 206900000000000 {
                                let price = PumpSwap::price_from_create(&create);
                                let mint = MewMint {
                                    mint: create.base_mint,
                                    bonding_curve: create.pool,
                                    price: price,
                                    highest_price: price,
                                    name: "".to_string(),
                                    symbol: "".to_string(),
                                    uri: "".to_string(),
                                    creator: create.creator,
                                    creator_sold: false,
                                    creator_token_amount: 0.0,
                                    buys: 0,
                                    sells: 0,
                                    volume: 0.0,
                                    liquidity: 0.0,
                                    txns_in_zero: 0,
                                    txns_in_n: 0,
                                    volume_in_n: 0.0,
                                    first_seen_slot: tx_update.slot,
                                    last_seen_slot: tx_update.slot,
                                    is_migrated: true,
                                    created_time: timestamp_now(),
                                };
                                log!(cc::LIGHT_WHITE, "Tracking dip: {:?} | Base mint: {:?}", create.pool, create.base_mint);
                                let mew = me.clone();
                                tokio::spawn(async move {
                                    if let Err(e) = mew.dip_session(&mint.mint.to_string()).await {
                                        warn!("dip_session error: {e}");
                                    }
                                });

                                me.mints.write().await.insert(create.base_mint, mint);
                            }
                        }
                        PumpSwapEvent::Buy(Some(buy)) => {
                            let mut mew_mint = {
                                let map = me.mints.read().await; // guard lives for this block
                                match map.iter().find(|(_, m)| m.bonding_curve == buy.pool) {
                                    Some((_, m)) => m.clone(),
                                    None => continue,
                                }
                            };

                            let user = buy.user;
                            let price = PumpSwap::price_from_buy(&buy);
                            let creator = mew_mint.creator;
                            if price > mew_mint.highest_price {
                                mew_mint.highest_price = price;
                            }
                            mew_mint.buys += 1;
                            mew_mint.volume += buy.quote_amount_in as f64 / 1e9;

                            if user == creator {
                                mew_mint.creator_token_amount += buy.base_amount_out as f64 / 1e6;
                            }
                            mew_mint.price = price;
                            mew_mint.last_seen_slot = tx_update.slot;
                            mew_mint.liquidity = buy.pool_quote_token_reserves as f64 / 1e9;
                            me.mints.write().await.insert(mew_mint.mint, mew_mint);
                        }
                        PumpSwapEvent::Sell(Some(sell)) => {
                            let mut mew_mint = {
                                let map = me.mints.read().await; // guard lives for this block
                                match map.iter().find(|(_, m)| m.bonding_curve == sell.pool) {
                                    Some((_, m)) => m.clone(),
                                    None => continue,
                                }
                            };
                            let user = sell.user;
                            let price = PumpSwap::price_from_sell(&sell);
                            let creator = mew_mint.creator;

                            mew_mint.sells += 1;
                            mew_mint.volume += sell.quote_amount_out as f64 / 1e9;

                            let tok_amt = sell.base_amount_in as f64 / 1e6;
                            if user == creator {
                                let dev_sell_pct =  if mew_mint.creator_token_amount >= tok_amt && mew_mint.creator_token_amount > 0.0 { tok_amt / mew_mint.creator_token_amount } else { 0.0 };
                                if dev_sell_pct >= 0.5 {
                                    mew_mint.creator_sold = true;
                                }    
                                mew_mint.creator_token_amount -= tok_amt;
                                if mew_mint.creator_token_amount <= 0.0 {
                                    mew_mint.creator_sold = true;
                                }
                            }
                            mew_mint.price = price;
                            mew_mint.last_seen_slot = tx_update.slot;
                            mew_mint.liquidity = sell.pool_quote_token_reserves as f64 / 1e9;
                            me.mints.write().await.insert(mew_mint.mint, mew_mint);
                        }
                        _ => {}
                    }
                }
            });
        }
        Ok(())
    }

    pub async fn subscribe_ws_pump_swap(&self) -> anyhow::Result<()> {
        // let buy_amount = config().buy_amount;
        let ws_url = config().ws_url.clone();

        let (mut rx, _handle) = self.sol_hook.subscribe_logs_channel(&ws_url, RpcTransactionLogsFilter::Mentions(vec![Pubkey::from_str("pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA").unwrap().to_string()]), CommitmentConfig::processed()).await.unwrap();
        log!(cc::LIGHT_WHITE, "Subscribed to PumpSwap using WS");

        while let Some(msg) = rx.recv().await {
            let sig = msg.signature.clone();
            let slot = self.sol_hook.rpc_client.get_slot_with_commitment(CommitmentConfig::processed()).await.unwrap();

            let events = PumpSwap::parse_logs(msg.logs.iter(), Some(&sig));

            let me = self.clone();

            tokio::spawn(async move {
                for event in events {
                    match event {
                        PumpSwapEvent::CreatePool(Some(create)) => {
                            if create.base_mint == WSOL_MINT {
                                continue;
                            }
                            // log!(cc::LIGHT_WHITE, "Create pool: {:?}", create);
                            if me.holdings.contains_key(&create.base_mint) && create.quote_mint == WSOL_MINT {
                                log!(cc::LIGHT_WHITE, "Token from holdings has migrated! {:?}", create.base_mint);
                                let mut mew_mint = me.mints.read().await.get(&Pubkey::from_str(create.base_mint.clone().to_string().as_str()).unwrap()).unwrap().clone();
                                mew_mint.is_migrated = true;
                                me.mints.write().await.insert(create.base_mint, mew_mint);
                            }
                            if create.quote_mint == WSOL_MINT && create.quote_amount_in >= 83990359346 && create.base_amount_in == 206900000000000 {
                                let price = PumpSwap::price_from_create(&create);
                                let mint = MewMint {
                                    mint: create.base_mint,
                                    bonding_curve: create.pool,
                                    price: price,
                                    highest_price: price,
                                    name: "".to_string(),
                                    symbol: "".to_string(),
                                    uri: "".to_string(),
                                    creator: create.creator,
                                    creator_sold: false,
                                    creator_token_amount: 0.0,
                                    buys: 0,
                                    sells: 0,
                                    volume: 0.0,
                                    liquidity: 0.0,
                                    txns_in_zero: 0,
                                    txns_in_n: 0,
                                    volume_in_n: 0.0,
                                    first_seen_slot: slot,
                                    last_seen_slot: slot,
                                    is_migrated: true,
                                    created_time: timestamp_now(),
                                };
                                log!(cc::LIGHT_WHITE, "Tracking dip: {:?} | Base mint: {:?}", create.pool, create.base_mint);
                                let mew = me.clone();
                                tokio::spawn(async move {
                                    if let Err(e) = mew.dip_session(&mint.mint.to_string()).await {
                                        warn!("dip_session error: {e}");
                                    }
                                });

                                me.mints.write().await.insert(create.base_mint, mint);
                            }
                        }
                        PumpSwapEvent::Buy(Some(buy)) => {
                            let mut mew_mint = {
                                let map = me.mints.read().await; // guard lives for this block
                                match map.iter().find(|(_, m)| m.bonding_curve == buy.pool) {
                                    Some((_, m)) => m.clone(),
                                    None => continue,
                                }
                            };

                            let user = buy.user;
                            let price = PumpSwap::price_from_buy(&buy);
                            let creator = mew_mint.creator;
                            if price > mew_mint.highest_price {
                                mew_mint.highest_price = price;
                            }
                            mew_mint.buys += 1;
                            mew_mint.volume += buy.quote_amount_in as f64 / 1e9;

                            if user == creator {
                                mew_mint.creator_token_amount += buy.base_amount_out as f64 / 1e6;
                            }
                            mew_mint.price = price;
                            mew_mint.last_seen_slot = slot;
                            mew_mint.liquidity = buy.pool_quote_token_reserves as f64 / 1e9;
                            me.mints.write().await.insert(mew_mint.mint, mew_mint);
                        }
                        PumpSwapEvent::Sell(Some(sell)) => {
                            let mut mew_mint = {
                                let map = me.mints.read().await; // guard lives for this block
                                match map.iter().find(|(_, m)| m.bonding_curve == sell.pool) {
                                    Some((_, m)) => m.clone(),
                                    None => continue,
                                }
                            };
                            let user = sell.user;
                            let price = PumpSwap::price_from_sell(&sell);
                            let creator = mew_mint.creator;

                            mew_mint.sells += 1;
                            mew_mint.volume += sell.quote_amount_out as f64 / 1e9;

                            let tok_amt = sell.base_amount_in as f64 / 1e6;
                            if user == creator {
                                let dev_sell_pct =  if mew_mint.creator_token_amount >= tok_amt && mew_mint.creator_token_amount > 0.0 { tok_amt / mew_mint.creator_token_amount } else { 0.0 };
                                if dev_sell_pct >= 0.5 {
                                    mew_mint.creator_sold = true;
                                }    
                                mew_mint.creator_token_amount -= tok_amt;
                                if mew_mint.creator_token_amount <= 0.0 {
                                    mew_mint.creator_sold = true;
                                }
                            }
                            mew_mint.price = price;
                            mew_mint.last_seen_slot = slot;
                            mew_mint.liquidity = sell.pool_quote_token_reserves as f64 / 1e9;
                            me.mints.write().await.insert(mew_mint.mint, mew_mint);
                        }
                        _ => {}
                    }
                }
            });
        }
        Ok(())
    }
}