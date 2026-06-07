use clap::{Parser, Subcommand};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::str::FromStr;

use braid::progress::reporter::ProgressVerbosity;

mod braid_receive;
mod braid_send;

#[derive(Parser, Debug)]
#[command(name = "braid", version, about = "Braid CLI")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Send data to a receiver
    #[command(alias = "s")]
    Send(SendArgs),
    /// Receive data from a sender
    #[command(alias = "recv", alias = "rx")]
    Receive(ReceiveArgs),
}

#[derive(Parser, Debug)]
struct SendArgs {
    /// Destination address as IP:PORT
    #[arg(long, short = 'd', value_parser = parse_socket_addr)]
    destination: SocketAddr,

    /// Chunk size in bytes (0 = adaptive)
    #[arg(long, short = 'c', default_value_t = 0, value_parser = parse_byte_size)]
    chunk_size: usize,

    /// Number of parallel channels (0 = adaptive)
    #[arg(long, default_value_t = 0, value_parser = parse_usize)]
    channels: usize,

    /// MTU for fragment sizing (default: 1500)
    #[arg(long, default_value_t = 1500, value_parser = parse_positive_usize)]
    mtu: usize,

    /// Quiet mode: suppress progress output
    #[arg(long, short = 'q', default_value_t = false)]
    quiet: bool,

    /// Verbose mode: detailed progress output
    #[arg(long, short = 'v', default_value_t = false)]
    verbose: bool,

    /// Maximum send rate in bytes per second (e.g. 125000000 for 1Gbps).
    /// 0 = unlimited. Use this to match the receiver's link capacity.
    #[arg(long, short = 'r', default_value_t = 0, value_parser = parse_data_rate)]
    max_rate: u64,
}

#[derive(Parser, Debug)]
struct ReceiveArgs {
    /// Bind address as IP:PORT
    #[arg(long, short = 'b', value_parser = parse_socket_addr)]
    bind: SocketAddr,

    /// Maximum receive buffer size in bytes
    #[arg(long, short = 's', value_parser = parse_byte_size)]
    buffer_size: usize,

    /// Path to output file (default: stdout)
    #[arg(long, short = 'o')]
    output: Option<PathBuf>,

    /// MTU for receive buffer sizing (default: 1500)
    #[arg(long, default_value_t = 1500, value_parser = parse_positive_usize)]
    mtu: usize,

    /// Quiet mode: suppress progress output
    #[arg(long, short = 'q', default_value_t = false)]
    quiet: bool,

    /// Verbose mode: detailed progress output
    #[arg(long, short = 'v', default_value_t = false)]
    verbose: bool,
}

fn parse_socket_addr(value: &str) -> Result<SocketAddr, String> {
    SocketAddr::from_str(value).map_err(|_| format!("invalid socket address: {value}"))
}

fn parse_positive_usize(value: &str) -> Result<usize, String> {
    let parsed: usize = value
        .parse()
        .map_err(|_| format!("invalid positive integer: {value}"))?;
    if parsed == 0 {
        Err(format!("value must be positive: {value}"))
    } else {
        Ok(parsed)
    }
}

fn parse_usize(value: &str) -> Result<usize, String> {
    value
        .parse()
        .map_err(|_| format!("invalid integer: {value}"))
}

/// Parse a byte-size string with optional K/M/G suffix (case-insensitive, decimal).
/// Examples: "64m" -> 64_000_000, "1G" -> 1_000_000_000, "65536" -> 65536
fn parse_byte_size(value: &str) -> Result<usize, String> {
    let (number, multiplier) = match value.chars().last() {
        Some('k') | Some('K') => (&value[..value.len().saturating_sub(1)], 1_000),
        Some('m') | Some('M') => (&value[..value.len().saturating_sub(1)], 1_000_000),
        Some('g') | Some('G') => (&value[..value.len().saturating_sub(1)], 1_000_000_000),
        _ => (value, 1),
    };

    let parsed: usize = number
        .parse()
        .map_err(|_| format!("invalid size: {value}"))?;

    parsed
        .checked_mul(multiplier)
        .ok_or_else(|| format!("invalid size: {value}"))
}

fn parse_data_rate(value: &str) -> Result<u64, String> {
    let (number, multiplier) = match value.chars().last() {
        Some('k') | Some('K') => (&value[..value.len().saturating_sub(1)], 1_000u64),
        Some('m') | Some('M') => (&value[..value.len().saturating_sub(1)], 1_000_000u64),
        Some('g') | Some('G') => (&value[..value.len().saturating_sub(1)], 1_000_000_000u64),
        _ => (value, 1),
    };

    let parsed: u64 = number
        .parse()
        .map_err(|_| format!("invalid rate: {value}"))?;

    parsed
        .checked_mul(multiplier)
        .ok_or_else(|| format!("invalid rate: {value}"))
}

#[tokio::main]
async fn main() {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Send(args) => {
            let verbosity = if args.quiet {
                ProgressVerbosity::Quiet
            } else if args.verbose {
                ProgressVerbosity::Verbose
            } else {
                ProgressVerbosity::Normal
            };

            let sender = braid_send::BraidSend::new(
                args.destination,
                args.chunk_size,
                args.channels,
                args.mtu,
                args.max_rate,
                verbosity,
            );

            if let Err(e) = sender.run().await {
                eprintln!("braid send error: {e}");
                std::process::exit(1);
            }
        }
        Commands::Receive(args) => {
            let verbosity = if args.quiet {
                ProgressVerbosity::Quiet
            } else if args.verbose {
                ProgressVerbosity::Verbose
            } else {
                ProgressVerbosity::Normal
            };

            let receiver = braid_receive::BraidReceive::new(
                args.bind,
                args.output,
                args.buffer_size,
                args.mtu,
                verbosity,
            );

            if let Err(e) = receiver.run().await {
                eprintln!("braid receive error: {e}");
                std::process::exit(1);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_send_command() {
        let cli = Cli::try_parse_from([
            "braid",
            "send",
            "--destination",
            "127.0.0.1:9000",
            "--chunk-size",
            "4096",
            "--channels",
            "8",
            "--mtu",
            "9000",
        ])
        .expect("send args should parse");

        match cli.command {
            Commands::Send(args) => {
                assert_eq!(args.destination.to_string(), "127.0.0.1:9000");
                assert_eq!(args.chunk_size, 4096);
                assert_eq!(args.channels, 8);
                assert_eq!(args.mtu, 9000);
                assert!(!args.quiet);
                assert!(!args.verbose);
                assert_eq!(args.max_rate, 0);
            }
            _ => panic!("expected send command"),
        }
    }

    #[test]
    fn parses_short_flags_send() {
        let cli = Cli::try_parse_from([
            "braid",
            "send",
            "-d",
            "127.0.0.1:9000",
            "-c",
            "4096",
            "-q",
            "-r",
            "125m",
        ])
        .expect("send args with short flags should parse");

        match cli.command {
            Commands::Send(args) => {
                assert_eq!(args.destination.to_string(), "127.0.0.1:9000");
                assert_eq!(args.chunk_size, 4096);
                assert!(args.quiet);
                assert!(!args.verbose);
                assert_eq!(args.max_rate, 125_000_000);
            }
            _ => panic!("expected send command"),
        }
    }

    #[test]
    fn parses_send_with_max_rate() {
        let cli = Cli::try_parse_from([
            "braid",
            "send",
            "--destination",
            "10.0.0.2:9000",
            "--max-rate",
            "125000000",
        ])
        .expect("send with max-rate should parse");

        match cli.command {
            Commands::Send(args) => {
                assert_eq!(args.max_rate, 125000000);
            }
            _ => panic!("expected send command"),
        }
    }

    #[test]
    fn parses_short_flags_receive() {
        let cli = Cli::try_parse_from([
            "braid",
            "receive",
            "-b",
            "0.0.0.0:9000",
            "-s",
            "64m",
            "-o",
            "output.bin",
        ])
        .expect("receive args with short flags should parse");

        match cli.command {
            Commands::Receive(args) => {
                assert_eq!(args.bind.to_string(), "0.0.0.0:9000");
                assert_eq!(args.buffer_size, 64_000_000);
                assert_eq!(args.output, Some(PathBuf::from("output.bin")));
                assert!(!args.quiet);
                assert!(!args.verbose);
            }
            _ => panic!("expected receive command"),
        }
    }

    #[test]
    fn parses_receive_command() {
        let cli = Cli::try_parse_from([
            "braid",
            "receive",
            "--bind",
            "[::1]:8080",
            "--buffer-size",
            "8192",
            "--output",
            "output.bin",
            "--mtu",
            "65536",
        ])
        .expect("receive args should parse");

        match cli.command {
            Commands::Receive(args) => {
                assert_eq!(args.bind.to_string(), "[::1]:8080");
                assert_eq!(args.buffer_size, 8192);
                assert_eq!(args.output, Some(PathBuf::from("output.bin")));
                assert_eq!(args.mtu, 65536);
            }
            _ => panic!("expected receive command"),
        }
    }

    #[test]
    fn parses_byte_size_suffixes() {
        assert_eq!(parse_byte_size("64m").unwrap(), 64_000_000);
        assert_eq!(parse_byte_size("1G").unwrap(), 1_000_000_000);
        assert_eq!(parse_byte_size("65536").unwrap(), 65536);
        assert_eq!(parse_byte_size("125k").unwrap(), 125_000);
    }

    #[test]
    fn parses_data_rate_suffixes() {
        assert_eq!(parse_data_rate("125m").unwrap(), 125_000_000);
        assert_eq!(parse_data_rate("1g").unwrap(), 1_000_000_000);
        assert_eq!(parse_data_rate("1000000").unwrap(), 1_000_000);
    }

    #[test]
    fn parses_receive_without_output() {
        let cli = Cli::try_parse_from([
            "braid",
            "receive",
            "--bind",
            "127.0.0.1:5000",
            "--buffer-size",
            "65536",
        ])
        .expect("receive without output should parse");

        match cli.command {
            Commands::Receive(args) => {
                assert_eq!(args.bind.to_string(), "127.0.0.1:5000");
                assert!(args.output.is_none());
            }
            _ => panic!("expected receive command"),
        }
    }

    #[test]
    fn parses_subcommand_alias_s() {
        let cli = Cli::try_parse_from(["braid", "s", "-d", "127.0.0.1:9000"]).unwrap();

        assert!(matches!(cli.command, Commands::Send(_)));
    }

    #[test]
    fn parses_subcommand_alias_recv() {
        let cli =
            Cli::try_parse_from(["braid", "recv", "-b", "0.0.0.0:9000", "-s", "1024"]).unwrap();

        assert!(matches!(cli.command, Commands::Receive(_)));
    }

    #[test]
    fn parses_subcommand_alias_rx() {
        let cli = Cli::try_parse_from(["braid", "rx", "-b", "0.0.0.0:9000", "-s", "1024"]).unwrap();

        assert!(matches!(cli.command, Commands::Receive(_)));
    }

    #[test]
    fn parses_send_with_defaults() {
        let cli = Cli::try_parse_from(["braid", "send", "--destination", "127.0.0.1:9000"])
            .expect("send args with defaults should parse");

        match cli.command {
            Commands::Send(args) => {
                assert_eq!(args.chunk_size, 0);
                assert_eq!(args.channels, 0);
                assert_eq!(args.mtu, 1500);
                assert!(!args.quiet);
                assert!(!args.verbose);
            }
            _ => panic!("expected send command"),
        }
    }

    #[test]
    fn rejects_invalid_socket_address() {
        let err = Cli::try_parse_from([
            "braid",
            "receive",
            "--bind",
            "not-an-ip",
            "--buffer-size",
            "1024",
        ])
        .unwrap_err();

        assert!(err.to_string().contains("invalid socket address"));
    }
}
