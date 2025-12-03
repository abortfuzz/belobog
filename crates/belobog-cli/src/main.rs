#![allow(clippy::result_large_err)]

use belobog::operations::{fuzz::FuzzArgs, replay::FuzzReplayArgs};
use belobog_fork::{dump::DumpPackageArgs, fork::ForkArgs};
use belobog_types::error::BelobogError;
use clap::{Parser, Subcommand};

#[derive(Subcommand)]
pub enum BelobogCommand {
    Fork(ForkArgs),
    Replay(FuzzReplayArgs),
    Fuzz(FuzzArgs),
}

#[derive(Parser)]
pub struct BelobogCLI {
    #[clap(subcommand)]
    pub cmd: BelobogCommand,
}

async fn main_entry() -> Result<(), BelobogError> {
    let args = tokio::task::spawn_blocking(BelobogCLI::parse).await?;
    match args.cmd {
        BelobogCommand::Fork(args) => args.run().await?,
        BelobogCommand::Fuzz(args) => tokio::task::spawn_blocking(move || args.fuzz()).await??,
        BelobogCommand::Replay(args) => tokio::task::spawn_blocking(move || args.run()).await??,
    }
    Ok(())
}

fn main() -> Result<(), BelobogError> {
    color_eyre::install().expect("Fail to install color_eyre");
    if let Ok(dot_file) = std::env::var("DOT") {
        dotenvy::from_path(dot_file).expect("fail to import");
    } else {
        // Allows failure
        let _ = dotenvy::dotenv();
    }
    env_logger::init();

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(main_entry())
}
