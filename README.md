<div align="center">
  <img width="256" height="256" alt="mewxxx" src="https://github.com/user-attachments/assets/be176357-2e7b-4b57-90d9-2d3720a0dde1" />
</div>

High-performance trading bot powered by analytics.

# Prerequisities

- A processor & some electricity
- Preferably Linux distro
- [PostgreSQL](https://www.postgresql.org/)
- Faith.
- & an RPC provider

## Features

- Check current consensus leader's location to make decisions 
- A Postgre database hooked into Pump.fun and Pump.fun AMM
- Integrated SWQoS Services:
  - Blox
  - Jito
  - Helius
  - Temporal
  - ZeroSlot
  - NextBlock
- Durable nonce when using SWQoS
- Choose between gRPC or Websocket
- Plug-n-play trading strategies:
   - Deagles - Creators who funded their wallet (or sub-wallet) with more than X SOL **using CEX**
   - VolCreators - Creators from analytics, sorted by volume
   - GrandChillers - ...sorted by highest market cap
   - Digged - Doesn't use analytics, checks if the number of transactions between first and n slot from mint's creation is higher or equal to configured number
   - Dip - Enter into dips of migrated Pump.fun tokens
   - DevBestFriend - Enter only if the dev is alone in his slot after 100ms, avoids bundlers
- Configurable trading filters (see `.env`)
- Avoids duplicate tokens in both analytics and trading
- Calculates best priority fee for the transaction

## Setup

### Download the repository

```
$ git clone https://github.com/FLOCK4H/Mew-X
$ cd Mew-X
```

### Create databases

```bash
$ psql -U your_postgre_user
$ > CREATE DATABASE vacation;
$ > CREATE DATABASE goldmine;
```

Tables will be created on launch.

### Configure the `.env` file

**All variables should be self-explanatory, if something isn't clear or you're stuck and need help, please visit [Telegram](https://t.me/flock4hcave) or [Discord](https://discord.gg/thREUECv2a) groups.**

<details>
  <summary>Click to see full `.env` file</summary>

```bash
PRIVATE_KEY="4knkrbw0238XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
RPC_URL="http://127.0.0.1:8899"
WS_URL="ws://127.0.0.1:8900"
DB_URL = "postgres://<user>:<password>@<host>:<port>" # need a valid postgre path 

USE_GRPC = false
GRPC_URL="http://127.0.0.1:10000"
GRPC_TOKEN=""
NONCE_ACCOUNT="" # Needed when using SWQoS in TX_STRAT below, otherwise an error happens (thread 'tokio-runtime-worker' panicked).

TX_STRAT = "swqos" # rpc | swqos; TIP: When you've got no keys to anything, no worries, you can still use Helius and Jito, just pass the swqos option.
NEXTBLOCK_KEY = ""
ZERO_SLOT_KEY = ""
TEMPORAL_KEY = ""
BLOX_KEY = ""
TIP_SOL = 0.001
PRIORITY_FEE_LVL = "high" # low | medium | high | turbo | max

BUY_AMOUNT_SOL = 0.0001
SLIPPAGE = 30
MAX_TOKENS_AT_ONCE = 1
USE_REGIONS = false
REGIONS = "Germany, Netherlands, United Kingdom"
MAX_LOSS = 10
TAKE_PROFIT = 0 # disabled
MIN_DEV_SOLD = 20_000_000 # min token amount bought by the dev at the start to consider exit-sell with him
MAX_NO_ACTIVITY_MS = 60000 # 60 seconds
MAX_NA_ON_START_MS = 5000 # 5 seconds

MODE = "trade" # sim || trade *dynamic, mutable while the program is running
DEAGLE_DEBUG = false # true || false
MIN_TRANSFER_SOL = 1.0 # Collect potential devs who fund their wallet with X sol from exchanges

ALGO_LIMIT = 100000 # Limit the number of tracked creators
ALGO_USE_DEAGLES = true # Deagles are creators that have funded their wallet (via exchange) with more than X sol in a single transfer
ALGO_MIN_DEAGLE_SOL = 1.0 # Filter out deagle creators by minimum amount they got to have

# VolCreators: Creators in the db sorted by volume
ALGO_USE_VOLCREATORS=false # true || false
ALGO_MIN_VOLUME = 10.0
ALGO_MIN_MINTS = 1
ALGO_MIN_BUYS = 200

# GrandChillers: Creators in the db sorted by highest market cap
ALGO_USE_GRAND_CHILLERS=false # true || false
ALGO_GC_MIN_HMC = 15000.0
ALGO_GC_MIN_BUYS = 50
ALGO_GC_MIN_MINTS = 2

# Creator's Holding Percentage - if creator owns more than LOWER or less than UPPER we skip
USE_CHP=true # true || false
CHP_LOWER = 0.09
CHP_UPPER = 0.21

# Txns in Zero - mimics above; number of txns in the slot where token was created
USE_TIZ=true # true || false
TIZ_LOWER = 3
TIZ_UPPER = 12

# Additional strat [Digged]: Avoid bundlers, but enter high tx count between n
ENABLE_ABS=false
ABS_MIN_BUYS=5
ABS_MIN_VOL=5 # in SOL
ABS_N=2

# Additional strat [Dip]: Trade on dips of migrated tokens
ENABLE_MTD=true
MTD_PCT=20
MTD_STABLE_TIME=10 # in seconds
MTD_MIN_MC=20 # in SOL
MTD_MAX_LOSS=15 # pct
MTD_TAKE_PROFIT=40 # pct

# Additional strat: Dev's best friend, avoid bundlers - enter if dev is alone
ENABLE_DBF=true
DBF_MAX_CHP=10
```

</details>

### Build & run the project

```
$ cargo run
```

First, Mew will fetch locations of nodes participating in the cluster. **Indexing of (at the time of writing) 6000+ validators takes around 1.5 hour, we do that to speed up the location checks when trading.**

```bash
Indexing HrLAr7y9k8qYd14uF5KdTz5z9p9BvZuf1o6cKY255wtD @ 80.240.31.38 -> ("Frankfurt", "Germany")
Indexing B7PbdWDgqc5h5rbTyi5Yyz2e1DJZEMoM3uXoqobk35n9 @ 202.8.11.203 -> ("Singapore", "Singapore")
Indexing Apt9PHrFBt1sji788VzwBYpYensM43eA6qLVybqZrKak @ 65.21.90.166 -> ("Helsinki", "Finland")
```

### (Optional) Development

There are lots of tests in `sol.rs` of `sol_hook`, to save you some trouble here is the command:

```bash
$ cargo test mew::sol_hook::sol::tests::test_pump_buy -- --exact --no-capture
```

## Socials & Support

Telegram private: `@dubskii420`

Telegram group: https://t.me/flock4hcave

Discord handle: `flockahh`

Discord group: https://discord.gg/thREUECv2a

## License

Copyright 2025 FLOCK4H

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the “Software”), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
