use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use bitcoin::{Address, Network, PublicKey};
use clap::{Parser, Subcommand, ValueEnum};
use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};
use tokio::time::sleep;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

mod bdb;

const DEFAULT_ESPLORA_URL: &str = "https://blockstream.info/api";
const DEFAULT_PRICE_URL: &str =
    "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd";

#[derive(Parser)]
#[command(author, version, about = "Extract spendable Bitcoin Core wallet addresses and check balances")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Extract {
        wallet: PathBuf,
        #[arg(long, value_enum, default_value_t = CliNetwork::Bitcoin)]
        network: CliNetwork,
        #[arg(long, default_value = "addresses.json")]
        output: PathBuf,
    },
    Balance {
        input: PathBuf,
        #[arg(long, default_value = DEFAULT_ESPLORA_URL)]
        esplora: String,
        #[arg(long, default_value = DEFAULT_PRICE_URL)]
        price_url: String,
        #[arg(long, default_value = "balances.json")]
        output: PathBuf,
    },
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum CliNetwork {
    Bitcoin,
    Testnet,
    Signet,
    Regtest,
}

impl From<CliNetwork> for Network {
    fn from(value: CliNetwork) -> Self {
        match value {
            CliNetwork::Bitcoin => Network::Bitcoin,
            CliNetwork::Testnet => Network::Testnet,
            CliNetwork::Signet => Network::Signet,
            CliNetwork::Regtest => Network::Regtest,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct ExtractReport {
    wallet_path: String,
    network: String,
    records_scanned: usize,
    record_type_counts: BTreeMap<String, usize>,
    spendable_addresses: Vec<SpendableAddress>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SpendableAddress {
    address: String,
    public_key: String,
    compressed: bool,
    source_records: Vec<String>,
    birth_time: Option<i64>,
    hd_keypath: Option<String>,
    label: Option<String>,
    purpose: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct BalanceReport {
    input_path: String,
    network: String,
    address_count: usize,
    esplora_base_url: String,
    bitcoin_price_usd: Option<f64>,
    totals: BalanceTotals,
    addresses: Vec<BalanceEntry>,
}

#[derive(Debug, Serialize, Deserialize)]
struct BalanceTotals {
    confirmed_sats: u64,
    unconfirmed_sats: i64,
    addresses_with_funds: usize,
    estimated_confirmed_value_usd: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BalanceEntry {
    address: String,
    public_key: String,
    compressed: bool,
    source_records: Vec<String>,
    birth_time: Option<i64>,
    hd_keypath: Option<String>,
    label: Option<String>,
    purpose: Option<String>,
    confirmed_sats: u64,
    unconfirmed_sats: i64,
    chain_tx_count: u64,
    mempool_tx_count: u64,
}

#[derive(Debug, Default, Clone)]
struct KeyMetadata {
    birth_time: Option<i64>,
    hd_keypath: Option<String>,
}

#[derive(Debug, Deserialize)]
struct EsploraAddressResponse {
    chain_stats: EsploraStats,
    mempool_stats: EsploraStats,
}

#[derive(Debug, Deserialize)]
struct EsploraStats {
    funded_txo_sum: u64,
    spent_txo_sum: u64,
    tx_count: u64,
}

#[derive(Debug, Deserialize)]
struct PriceEnvelope {
    bitcoin: PricePoint,
}

#[derive(Debug, Deserialize)]
struct PricePoint {
    usd: f64,
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();

    let cli = Cli::parse();

    match cli.command {
        Command::Extract {
            wallet,
            network,
            output,
        } => {
            let network: Network = network.into();
            info!(wallet = %wallet.display(), %network, output = %output.display(), "Starting wallet extraction");

            let report = extract_wallet(&wallet, network)?;
            write_json(&output, &report)?;
            info!(
                records_scanned = report.records_scanned,
                spendable_addresses = report.spendable_addresses.len(),
                output = %output.display(),
                "Wallet extraction finished"
            );
            println!(
                "Extracted {} spendable addresses from {} into {}",
                report.spendable_addresses.len(),
                wallet.display(),
                output.display()
            );
        }
        Command::Balance {
            input,
            esplora,
            price_url,
            output,
        } => {
            info!(
                input = %input.display(),
                esplora = %esplora,
                price_url = %price_url,
                output = %output.display(),
                "Starting balance lookup"
            );
            let extract_report = read_extract_report(&input)?;
            info!(
                address_count = extract_report.spendable_addresses.len(),
                network = %extract_report.network,
                "Loaded extracted addresses"
            );
            let report = build_balance_report(&extract_report, &esplora, &price_url, &output).await?;
            info!(
                address_count = report.address_count,
                confirmed_sats = report.totals.confirmed_sats,
                unconfirmed_sats = report.totals.unconfirmed_sats,
                addresses_with_funds = report.totals.addresses_with_funds,
                output = %output.display(),
                "Balance lookup finished"
            );
            println!(
                "Checked {} addresses. Confirmed balance: {} sats. Report written to {}",
                report.address_count,
                report.totals.confirmed_sats,
                output.display()
            );
        }
    }

    Ok(())
}

fn init_tracing() {
    let env_filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_target(false)
        .compact()
        .init();
}

fn extract_wallet(wallet_path: &Path, network: Network) -> Result<ExtractReport> {
    info!(wallet = %wallet_path.display(), %network, "Opening wallet database");
    let database = open_wallet_database(wallet_path)?;
    let mut cursor = database.cursor().context("failed to create Berkeley DB cursor")?;

    let mut record_type_counts = BTreeMap::new();
    let mut labels = HashMap::<String, String>::new();
    let mut purposes = HashMap::<String, String>::new();
    let mut metadata = HashMap::<String, KeyMetadata>::new();
    let mut spendable = HashMap::<String, SpendableAddress>::new();
    let mut records_scanned = 0usize;

    loop {
        match cursor.next() {
            Ok(Some((key_bytes, value_bytes))) => {
                records_scanned += 1;
                let record_type = parse_record_type(&key_bytes).unwrap_or_else(|_| "<unparsed>".to_string());
                *record_type_counts.entry(record_type.clone()).or_insert(0) += 1;

                match record_type.as_str() {
                    "ckey" | "key" | "wkey" => {
                        if let Ok(pubkey_bytes) = parse_key_record_pubkey(&key_bytes) {
                            let entry = spendable_entry_from_pubkey(&pubkey_bytes, &record_type, network)?;
                            merge_spendable_entry(&mut spendable, entry);
                        }
                    }
                    "keymeta" => {
                        if let Ok((pubkey_bytes, key_metadata)) =
                            parse_keymeta_record(&key_bytes, &value_bytes)
                        {
                            metadata.insert(hex::encode(pubkey_bytes), key_metadata);
                        }
                    }
                    "name" => {
                        if let Ok((address, label)) =
                            parse_address_string_record(&key_bytes, &value_bytes)
                        {
                            if !label.is_empty() {
                                labels.insert(address, label);
                            }
                        }
                    }
                    "purpose" => {
                        if let Ok((address, purpose)) =
                            parse_address_string_record(&key_bytes, &value_bytes)
                        {
                            if !purpose.is_empty() {
                                purposes.insert(address, purpose);
                            }
                        }
                    }
                    _ => {}
                }
            }
            Ok(None) => break,
            Err(err) => return Err(anyhow!("cursor iteration failed: {err}")),
        }
    }

    let mut spendable_addresses: Vec<_> = spendable.into_values().collect();
    for entry in &mut spendable_addresses {
        if let Some(key_metadata) = metadata.get(&entry.public_key) {
            entry.birth_time = key_metadata.birth_time;
            entry.hd_keypath = key_metadata.hd_keypath.clone();
        }
        entry.label = labels.get(&entry.address).cloned();
        entry.purpose = purposes.get(&entry.address).cloned();
        entry.source_records.sort();
        entry.source_records.dedup();
    }

    spendable_addresses.sort_by(|left, right| {
        left.hd_keypath
            .cmp(&right.hd_keypath)
            .then_with(|| left.address.cmp(&right.address))
    });

    info!(
        wallet = %wallet_path.display(),
        records_scanned,
        spendable_addresses = spendable_addresses.len(),
        "Wallet scan complete"
    );

    Ok(ExtractReport {
        wallet_path: wallet_path.display().to_string(),
        network: network.to_string(),
        records_scanned,
        record_type_counts,
        spendable_addresses,
    })
}

fn open_wallet_database(wallet_path: &Path) -> Result<bdb::Database> {
    let candidates = [Some("main"), None];
    let mut errors = Vec::new();

    for database_name in candidates {
        match bdb::Database::open(wallet_path, database_name) {
            Ok(database) => {
                info!(
                    wallet = %wallet_path.display(),
                    database_name = database_name.unwrap_or("<none>"),
                    "Opened Berkeley DB wallet"
                );
                return Ok(database);
            }
            Err(err) => errors.push(match database_name {
                Some(name) => format!("name={name}: {err}"),
                None => format!("name=<none>: {err}"),
            }),
        }
    }

    bail!(
        "failed to open wallet database {} ({})",
        wallet_path.display(),
        errors.join("; ")
    )
}

fn parse_record_type(bytes: &[u8]) -> Result<String> {
    let mut cursor = ByteCursor::new(bytes);
    cursor.read_string()
}

fn parse_key_record_pubkey(bytes: &[u8]) -> Result<Vec<u8>> {
    let mut cursor = ByteCursor::new(bytes);
    let record_type = cursor.read_string()?;
    if !matches!(record_type.as_str(), "ckey" | "key" | "wkey") {
        bail!("unexpected key record type: {record_type}");
    }
    cursor.read_var_bytes()
}

fn parse_keymeta_record(key_bytes: &[u8], value_bytes: &[u8]) -> Result<(Vec<u8>, KeyMetadata)> {
    let mut key_cursor = ByteCursor::new(key_bytes);
    let record_type = key_cursor.read_string()?;
    if record_type != "keymeta" {
        bail!("unexpected keymeta record type: {record_type}");
    }
    let pubkey = key_cursor.read_var_bytes()?;

    let mut value_cursor = ByteCursor::new(value_bytes);
    let _version = value_cursor.read_u32_le()?;
    let birth_time = value_cursor.read_i64_le().ok();
    let hd_keypath = value_cursor.read_string().ok();

    Ok((
        pubkey,
        KeyMetadata {
            birth_time,
            hd_keypath,
        },
    ))
}

fn parse_address_string_record(key_bytes: &[u8], value_bytes: &[u8]) -> Result<(String, String)> {
    let mut key_cursor = ByteCursor::new(key_bytes);
    let _record_type = key_cursor.read_string()?;
    let address = key_cursor.read_string()?;

    let mut value_cursor = ByteCursor::new(value_bytes);
    let value = value_cursor.read_string()?;
    Ok((address, value))
}

fn spendable_entry_from_pubkey(pubkey_bytes: &[u8], source_record: &str, network: Network) -> Result<SpendableAddress> {
    let public_key = PublicKey::from_slice(pubkey_bytes)
        .with_context(|| format!("failed to parse public key {}", hex::encode(pubkey_bytes)))?;
    let address = Address::p2pkh(public_key, network);

    Ok(SpendableAddress {
        address: address.to_string(),
        public_key: hex::encode(pubkey_bytes),
        compressed: pubkey_bytes.len() == 33,
        source_records: vec![source_record.to_string()],
        birth_time: None,
        hd_keypath: None,
        label: None,
        purpose: None,
    })
}

fn merge_spendable_entry(entries: &mut HashMap<String, SpendableAddress>, entry: SpendableAddress) {
    match entries.get_mut(&entry.address) {
        Some(existing) => {
            existing.source_records.extend(entry.source_records);
            existing.source_records.sort();
            existing.source_records.dedup();
        }
        None => {
            entries.insert(entry.address.clone(), entry);
        }
    }
}

async fn build_balance_report(
    extract_report: &ExtractReport,
    esplora_url: &str,
    price_url: &str,
    output_path: &Path,
) -> Result<BalanceReport> {
    if extract_report.spendable_addresses.is_empty() {
        bail!("input extract report does not contain spendable addresses");
    }

    let total_addresses = extract_report.spendable_addresses.len();
    info!(
        address_count = total_addresses,
        network = %extract_report.network,
        esplora = %esplora_url,
        mode = "serial",
        "Fetching balances from public API"
    );

    let client = Client::builder()
        .timeout(Duration::from_secs(20))
        .user_agent("bitcoin-recovery/0.1")
        .build()
        .context("failed to build HTTP client")?;

    let esplora_base_url = esplora_url.trim_end_matches('/').to_string();
    let mut balance_entries = load_resume_balance_entries(extract_report, &esplora_base_url, output_path)?;
    let mut completed_addresses: HashSet<_> = balance_entries
        .iter()
        .map(|entry| entry.address.clone())
        .collect();
    let mut running_confirmed_sats: u64 = balance_entries.iter().map(|entry| entry.confirmed_sats).sum();

    if !balance_entries.is_empty() {
        let resumed_count = balance_entries.len();
        info!(
            completed = resumed_count,
            total = total_addresses,
            remaining = total_addresses.saturating_sub(resumed_count),
            running_confirmed_btc = format!("{:.8}", running_confirmed_sats as f64 / 100_000_000.0),
            output = %output_path.display(),
            "Resuming balance lookup from saved progress"
        );
        println!(
            "Resuming from {} completed addresses in {}. Skipping {} already-fetched addresses.",
            resumed_count,
            output_path.display(),
            resumed_count
        );
    }

    for (index, address) in extract_report.spendable_addresses.iter().cloned().enumerate() {
        if completed_addresses.contains(&address.address) {
            continue;
        }

        let stats = fetch_address_stats(&client, &esplora_base_url, &address.address).await?;
        let confirmed_sats = stats
            .chain_stats
            .funded_txo_sum
            .saturating_sub(stats.chain_stats.spent_txo_sum);
        let unconfirmed_sats =
            stats.mempool_stats.funded_txo_sum as i64 - stats.mempool_stats.spent_txo_sum as i64;
        let fetched = index + 1;
        running_confirmed_sats = running_confirmed_sats.saturating_add(confirmed_sats);

        if fetched % 100 == 0 || fetched == total_addresses {
            info!(
                completed = fetched,
                total = total_addresses,
                running_confirmed_btc = format!("{:.8}", running_confirmed_sats as f64 / 100_000_000.0),
                "Fetched address balances"
            );
        }

        let entry = BalanceEntry {
            address: address.address,
            public_key: address.public_key,
            compressed: address.compressed,
            source_records: address.source_records,
            birth_time: address.birth_time,
            hd_keypath: address.hd_keypath,
            label: address.label,
            purpose: address.purpose,
            confirmed_sats,
            unconfirmed_sats,
            chain_tx_count: stats.chain_stats.tx_count,
            mempool_tx_count: stats.mempool_stats.tx_count,
        };

        completed_addresses.insert(entry.address.clone());
        balance_entries.push(entry);

        let partial_report = assemble_balance_report(extract_report, &esplora_base_url, None, &balance_entries);
        write_json(output_path, &partial_report)?;
    }

    info!(price_url = %price_url, "Fetching BTC/USD price");
    let bitcoin_price_usd = match fetch_bitcoin_price_usd(&client, price_url).await {
        Ok(price) => {
            info!(bitcoin_price_usd = price, "Fetched BTC/USD price");
            Some(price)
        }
        Err(err) => {
            warn!(error = %err, price_url = %price_url, "Failed to fetch BTC/USD price; continuing without fiat estimate");
            None
        }
    };

    let report = assemble_balance_report(
        extract_report,
        &esplora_base_url,
        bitcoin_price_usd,
        &balance_entries,
    );
    write_json(output_path, &report)?;

    info!(
        confirmed_sats = report.totals.confirmed_sats,
        unconfirmed_sats = report.totals.unconfirmed_sats,
        addresses_with_funds = report.totals.addresses_with_funds,
        "Balance aggregation complete"
    );

    Ok(report)
}

fn assemble_balance_report(
    extract_report: &ExtractReport,
    esplora_base_url: &str,
    bitcoin_price_usd: Option<f64>,
    balance_entries: &[BalanceEntry],
) -> BalanceReport {
    let mut addresses = balance_entries.to_vec();
    addresses.sort_by(|left, right| {
        right
            .confirmed_sats
            .cmp(&left.confirmed_sats)
            .then_with(|| right.unconfirmed_sats.cmp(&left.unconfirmed_sats))
            .then_with(|| left.address.cmp(&right.address))
    });

    let confirmed_sats = addresses.iter().map(|entry| entry.confirmed_sats).sum();
    let unconfirmed_sats = addresses.iter().map(|entry| entry.unconfirmed_sats).sum();
    let addresses_with_funds = addresses
        .iter()
        .filter(|entry| entry.confirmed_sats > 0 || entry.unconfirmed_sats != 0)
        .count();
    let estimated_confirmed_value_usd = bitcoin_price_usd
        .map(|price| (confirmed_sats as f64 / 100_000_000.0) * price);

    BalanceReport {
        input_path: extract_report.wallet_path.clone(),
        network: extract_report.network.clone(),
        address_count: addresses.len(),
        esplora_base_url: esplora_base_url.to_string(),
        bitcoin_price_usd,
        totals: BalanceTotals {
            confirmed_sats,
            unconfirmed_sats,
            addresses_with_funds,
            estimated_confirmed_value_usd,
        },
        addresses,
    }
}

fn load_resume_balance_entries(
    extract_report: &ExtractReport,
    esplora_base_url: &str,
    output_path: &Path,
) -> Result<Vec<BalanceEntry>> {
    let Some(report) = read_balance_report_if_exists(output_path)? else {
        return Ok(Vec::new());
    };

    validate_resume_report(extract_report, &report, esplora_base_url, output_path)?;
    Ok(report.addresses)
}

fn read_balance_report_if_exists(path: &Path) -> Result<Option<BalanceReport>> {
    if !path.exists() {
        return Ok(None);
    }

    read_balance_report(path).map(Some)
}

fn validate_resume_report(
    extract_report: &ExtractReport,
    report: &BalanceReport,
    esplora_base_url: &str,
    output_path: &Path,
) -> Result<()> {
    if report.network != extract_report.network {
        bail!(
            "cannot resume from {}: network {} does not match current input {}",
            output_path.display(),
            report.network,
            extract_report.network
        );
    }

    if report.esplora_base_url != esplora_base_url {
        bail!(
            "cannot resume from {}: esplora URL {} does not match current URL {}",
            output_path.display(),
            report.esplora_base_url,
            esplora_base_url
        );
    }

    let spendable_by_address: HashMap<_, _> = extract_report
        .spendable_addresses
        .iter()
        .map(|address| (address.address.as_str(), address))
        .collect();
    let mut seen_addresses = HashSet::new();

    for entry in &report.addresses {
        let Some(spendable) = spendable_by_address.get(entry.address.as_str()) else {
            bail!(
                "cannot resume from {}: address {} is not present in the current input",
                output_path.display(),
                entry.address
            );
        };

        if !seen_addresses.insert(entry.address.as_str()) {
            bail!(
                "cannot resume from {}: address {} appears more than once",
                output_path.display(),
                entry.address
            );
        }

        if !balance_entry_matches_spendable(entry, spendable) {
            bail!(
                "cannot resume from {}: saved metadata for address {} does not match the current input",
                output_path.display(),
                entry.address
            );
        }
    }

    if report.addresses.len() > extract_report.spendable_addresses.len() {
        bail!(
            "cannot resume from {}: saved report has more addresses than the current input",
            output_path.display()
        );
    }

    Ok(())
}

fn balance_entry_matches_spendable(entry: &BalanceEntry, spendable: &SpendableAddress) -> bool {
    entry.address == spendable.address
        && entry.public_key == spendable.public_key
        && entry.compressed == spendable.compressed
        && entry.source_records == spendable.source_records
        && entry.birth_time == spendable.birth_time
        && entry.hd_keypath == spendable.hd_keypath
        && entry.label == spendable.label
        && entry.purpose == spendable.purpose
}

fn read_balance_report(path: &Path) -> Result<BalanceReport> {
    info!(path = %path.display(), "Reading JSON report");
    let raw = fs::read(path).with_context(|| format!("failed to read {}", path.display()))?;
    serde_json::from_slice(&raw).with_context(|| format!("failed to parse {}", path.display()))
}

async fn fetch_address_stats(client: &Client, esplora_base_url: &str, address: &str) -> Result<EsploraAddressResponse> {
    let url = format!("{esplora_base_url}/address/{address}");
    fetch_json_with_retry(client, &url).await
}

async fn fetch_bitcoin_price_usd(client: &Client, price_url: &str) -> Result<f64> {
    let price = fetch_json_with_retry::<PriceEnvelope>(client, price_url).await?;
    Ok(price.bitcoin.usd)
}

async fn fetch_json_with_retry<T>(client: &Client, url: &str) -> Result<T>
where
    T: for<'de> Deserialize<'de>,
{
    let mut last_error = None;

    for attempt in 0..4 {
        match client.get(url).send().await {
            Ok(response) if response.status().is_success() => {
                return response
                    .json::<T>()
                    .await
                    .with_context(|| format!("failed to decode response from {url}"));
            }
            Ok(response)
                if response.status() == StatusCode::TOO_MANY_REQUESTS
                    || response.status().is_server_error() =>
            {
                last_error = Some(anyhow!(
                    "transient HTTP {} from {url}",
                    response.status().as_u16()
                ));
            }
            Ok(response) => {
                let status = response.status();
                let body = response.text().await.unwrap_or_default();
                bail!("HTTP {} from {url}: {body}", status.as_u16());
            }
            Err(err) => {
                last_error = Some(anyhow!(err).context(format!("request failed for {url}")));
            }
        }

        if attempt < 3 {
            let backoff_ms = 500 * (1u64 << attempt);
            if let Some(err) = last_error.as_ref() {
                warn!(
                    url = %url,
                    attempt = attempt + 1,
                    max_attempts = 4,
                    backoff_ms,
                    error = %err,
                    "Transient request failure; retrying"
                );
            }
            sleep(Duration::from_millis(backoff_ms)).await;
        }
    }

    Err(last_error.unwrap_or_else(|| anyhow!("request failed for {url}")))
}

fn read_extract_report(path: &Path) -> Result<ExtractReport> {
    info!(path = %path.display(), "Reading JSON report");
    let raw = fs::read(path).with_context(|| format!("failed to read {}", path.display()))?;
    serde_json::from_slice(&raw).with_context(|| format!("failed to parse {}", path.display()))
}

fn write_json<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    let bytes = serde_json::to_vec_pretty(value).context("failed to serialize JSON")?;
    info!(path = %path.display(), bytes = bytes.len(), "Writing JSON report");
    fs::write(path, bytes).with_context(|| format!("failed to write {}", path.display()))
}

struct ByteCursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> ByteCursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn read_u8(&mut self) -> Result<u8> {
        let byte = *self
            .bytes
            .get(self.offset)
            .ok_or_else(|| anyhow!("unexpected end of buffer"))?;
        self.offset += 1;
        Ok(byte)
    }

    fn read_u16_le(&mut self) -> Result<u16> {
        let bytes = self.read_exact(2)?;
        Ok(u16::from_le_bytes([bytes[0], bytes[1]]))
    }

    fn read_u32_le(&mut self) -> Result<u32> {
        let bytes = self.read_exact(4)?;
        Ok(u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    fn read_i64_le(&mut self) -> Result<i64> {
        let bytes = self.read_exact(8)?;
        Ok(i64::from_le_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
    }

    fn read_compact_size(&mut self) -> Result<u64> {
        match self.read_u8()? {
            value @ 0x00..=0xfc => Ok(value as u64),
            0xfd => Ok(self.read_u16_le()? as u64),
            0xfe => Ok(self.read_u32_le()? as u64),
            0xff => {
                let bytes = self.read_exact(8)?;
                Ok(u64::from_le_bytes([
                    bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6],
                    bytes[7],
                ]))
            }
        }
    }

    fn read_exact(&mut self, len: usize) -> Result<&'a [u8]> {
        let end = self.offset + len;
        if end > self.bytes.len() {
            bail!("unexpected end of buffer");
        }
        let slice = &self.bytes[self.offset..end];
        self.offset = end;
        Ok(slice)
    }

    fn read_var_bytes(&mut self) -> Result<Vec<u8>> {
        let len = self.read_compact_size()? as usize;
        Ok(self.read_exact(len)?.to_vec())
    }

    fn read_string(&mut self) -> Result<String> {
        let raw = self.read_var_bytes()?;
        String::from_utf8(raw).context("invalid UTF-8 string")
    }
}

#[cfg(test)]
mod tests {
    use super::{
        BalanceEntry, BalanceReport, BalanceTotals, ByteCursor, ExtractReport, KeyMetadata,
        SpendableAddress, assemble_balance_report, parse_keymeta_record, validate_resume_report,
    };
    use std::collections::BTreeMap;
    use std::path::Path;

    fn sample_spendable_address(address: &str) -> SpendableAddress {
        SpendableAddress {
            address: address.to_string(),
            public_key: format!("pubkey-{address}"),
            compressed: true,
            source_records: vec!["ckey".to_string()],
            birth_time: Some(1_700_000_000),
            hd_keypath: Some("m/0'/0'/0'".to_string()),
            label: Some("label".to_string()),
            purpose: Some("receive".to_string()),
        }
    }

    fn sample_balance_entry(address: &str, confirmed_sats: u64) -> BalanceEntry {
        let spendable = sample_spendable_address(address);
        BalanceEntry {
            address: spendable.address,
            public_key: spendable.public_key,
            compressed: spendable.compressed,
            source_records: spendable.source_records,
            birth_time: spendable.birth_time,
            hd_keypath: spendable.hd_keypath,
            label: spendable.label,
            purpose: spendable.purpose,
            confirmed_sats,
            unconfirmed_sats: 0,
            chain_tx_count: 1,
            mempool_tx_count: 0,
        }
    }

    fn sample_extract_report() -> ExtractReport {
        ExtractReport {
            wallet_path: "wallet.dat".to_string(),
            network: "bitcoin".to_string(),
            records_scanned: 1,
            record_type_counts: BTreeMap::new(),
            spendable_addresses: vec![
                sample_spendable_address("addr-1"),
                sample_spendable_address("addr-2"),
            ],
        }
    }

    fn sample_resume_report(addresses: Vec<BalanceEntry>) -> BalanceReport {
        BalanceReport {
            input_path: "wallet.dat".to_string(),
            network: "bitcoin".to_string(),
            address_count: addresses.len(),
            esplora_base_url: "https://blockstream.info/api".to_string(),
            bitcoin_price_usd: None,
            totals: BalanceTotals {
                confirmed_sats: addresses.iter().map(|entry| entry.confirmed_sats).sum(),
                unconfirmed_sats: 0,
                addresses_with_funds: addresses.len(),
                estimated_confirmed_value_usd: None,
            },
            addresses,
        }
    }

    #[test]
    fn compact_size_and_string_parsing_work() {
        let bytes = [0x04, b't', b'e', b's', b't', 0x03, 0xaa, 0xbb, 0xcc];
        let mut cursor = ByteCursor::new(&bytes);
        assert_eq!(cursor.read_string().unwrap(), "test");
        assert_eq!(cursor.read_var_bytes().unwrap(), vec![0xaa, 0xbb, 0xcc]);
    }

    #[test]
    fn keymeta_parser_extracts_birth_time_and_hd_path() {
        let mut key = vec![0x07];
        key.extend_from_slice(b"keymeta");
        key.push(0x21);
        key.extend_from_slice(&[0x02; 33]);

        let mut value = Vec::new();
        value.extend_from_slice(&10u32.to_le_bytes());
        value.extend_from_slice(&1_535_042_081i64.to_le_bytes());
        value.push(0x0c);
        value.extend_from_slice(b"m/0'/0'/266'");
        value.extend_from_slice(&[0x11, 0x22, 0x33]);

        let (pubkey, metadata) = parse_keymeta_record(&key, &value).unwrap();
        assert_eq!(pubkey, vec![0x02; 33]);
        assert_eq!(
            metadata.birth_time,
            Some(1_535_042_081),
            "birth time should be parsed from little-endian i64"
        );
        assert_eq!(metadata.hd_keypath.as_deref(), Some("m/0'/0'/266'"));
    }

    #[test]
    fn keymeta_parser_handles_missing_hd_path() {
        let mut key = vec![0x07];
        key.extend_from_slice(b"keymeta");
        key.push(0x21);
        key.extend_from_slice(&[0x03; 33]);

        let mut value = Vec::new();
        value.extend_from_slice(&10u32.to_le_bytes());
        value.extend_from_slice(&1_600_000_000i64.to_le_bytes());

        let (_, metadata) = parse_keymeta_record(&key, &value).unwrap();
        assert_eq!(metadata.birth_time, Some(1_600_000_000));
        assert_eq!(metadata.hd_keypath, None);
    }

    #[test]
    fn key_metadata_defaults_empty() {
        let metadata = KeyMetadata::default();
        assert_eq!(metadata.birth_time, None);
        assert_eq!(metadata.hd_keypath, None);
    }

    #[test]
    fn resume_report_validation_accepts_matching_partial_output() {
        let extract_report = sample_extract_report();
        let report = sample_resume_report(vec![sample_balance_entry("addr-1", 42)]);

        validate_resume_report(
            &extract_report,
            &report,
            "https://blockstream.info/api",
            Path::new("balances.json"),
        )
        .unwrap();
    }

    #[test]
    fn resume_report_validation_rejects_mismatched_saved_metadata() {
        let extract_report = sample_extract_report();
        let mut report = sample_resume_report(vec![sample_balance_entry("addr-1", 42)]);
        report.addresses[0].public_key = "different-pubkey".to_string();

        let error = validate_resume_report(
            &extract_report,
            &report,
            "https://blockstream.info/api",
            Path::new("balances.json"),
        )
        .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("saved metadata for address addr-1 does not match the current input")
        );
    }

    #[test]
    fn assemble_balance_report_sorts_entries_and_recomputes_totals() {
        let extract_report = sample_extract_report();
        let report = assemble_balance_report(
            &extract_report,
            "https://blockstream.info/api",
            Some(50_000.0),
            &[
                sample_balance_entry("addr-1", 10),
                sample_balance_entry("addr-2", 20),
            ],
        );

        assert_eq!(report.address_count, 2);
        assert_eq!(report.addresses[0].address, "addr-2");
        assert_eq!(report.addresses[1].address, "addr-1");
        assert_eq!(report.totals.confirmed_sats, 30);
        assert_eq!(report.totals.addresses_with_funds, 2);
        assert_eq!(report.totals.estimated_confirmed_value_usd, Some(0.015));
    }
}
