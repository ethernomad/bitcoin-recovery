# bitcoin-recovery

Extract spendable addresses from a Bitcoin Core wallet file and check their on-chain balances.

## Prerequisites

- Rust toolchain (edition 2024)
- Berkeley DB 5.3 headers and library (`libdb-5.3`)
  - Debian/Ubuntu: `sudo apt install libdb5.3-dev`
  - The build script expects the header at `/usr/include/db5.3/db.h` and the library in `/usr/lib/`

## Building

```sh
cargo build --release
```

## Usage

The tool has two subcommands: `extract` and `balance`.

### extract

Reads a Bitcoin Core wallet database file, parses key/metadata records, and outputs all spendable P2PKH addresses to a JSON file.

```sh
bitcoin-recovery extract <WALLET_PATH> [OPTIONS]
```

| Option | Default | Description |
|--------|---------|-------------|
| `--network` | `bitcoin` | Network: `bitcoin`, `testnet`, `signet`, `regtest` |
| `--output` | `addresses.json` | Path for the JSON output file |

Example:

```sh
bitcoin-recovery extract wallet.dat --output addresses.json
```

### balance

Reads the JSON file produced by `extract` and queries an Esplora-compatible API for the confirmed/unconfirmed balance of each address.

```sh
bitcoin-recovery balance <INPUT> [OPTIONS]
```

| Option | Default | Description |
|--------|---------|-------------|
| `--esplora` | `https://blockstream.info/api` | Esplora-compatible API base URL |
| `--price-url` | CoinGecko BTC/USD endpoint | URL for fetching the BTC/USD price |
| `--output` | `balances.json` | Path for the JSON output file |

Example:

```sh
bitcoin-recovery balance addresses.json --esplora https://mempool.space/api
```

#### Alternative Esplora endpoints

Any server implementing the [Esplora](https://github.com/blockstream/esplora) protocol can be used with `--esplora`:

| URL | Network |
|-----|---------|
| `https://blockstream.info/api` | Mainnet (default) |
| `https://mempool.space/api` | Mainnet |
| `https://mempool.space/testnet/api` | Testnet |
| `https://mempool.space/signet/api` | Signet |
| Self-hosted electrs/esplora instance | Any |

## Output

Both subcommands write JSON files:

- **`addresses.json`** — List of spendable addresses with public keys, compression flags, labels, HD keypaths, and birth timestamps extracted from the wallet.
- **`balances.json`** — Per-address confirmed/unconfirmed satoshi balances, transaction counts, aggregated totals, and an estimated USD value.

## Logging

Set the `RUST_LOG` environment variable for diagnostic output:

```sh
RUST_LOG=debug bitcoin-recovery balance addresses.json
```

Defaults to `info` level.