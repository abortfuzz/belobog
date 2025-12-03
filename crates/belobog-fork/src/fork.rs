use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use crate::db::{MemoryDatabase, ObjectForkDatabase};
use crate::snapshot::{
    OrphanRuleWrapper, download_epoch_database, find_first_checkpoint_in_epoch, load_checkpoints,
    manifest_from_local_or_remote,
};
use belobog_types::error::BelobogError;
use belobog_types::range::RangeChunksExt;
use clap::Args;
use color_eyre::eyre::eyre;
use itertools::Itertools;
use log::{debug, info};
use mdbx_derive::MDBXDatabase;
use object_store::{ClientOptions, RetryConfig};
use sui_config::object_storage_config::{ObjectStoreConfig, ObjectStoreType};
use sui_sdk::SuiClientBuilder;
use sui_snapshot::{FileType, reader::LiveObjectIter};
use sui_storage::object_store::ObjectStorePutExt;
use sui_storage::object_store::{ObjectStoreGetExt, http::HttpDownloaderBuilder};

pub fn defaut_http_checkpoionts_store() -> Arc<dyn object_store::ObjectStore> {
    let retry_config = RetryConfig {
        max_retries: usize::MAX,
        retry_timeout: Duration::from_secs(60),
        ..Default::default()
    };
    let client_options = ClientOptions::new()
        .with_timeout(Duration::from_secs(60))
        .with_allow_http(true);
    let http_store = object_store::http::HttpBuilder::new()
        .with_url("https://checkpoints.mainnet.sui.io")
        .with_client_options(client_options)
        .with_retry(retry_config)
        .build()
        .unwrap();
    Arc::new(http_store) as _
}

#[derive(Args)]
pub struct ForkArgs {
    #[arg(short, long)]
    pub db: String,
    #[arg(short, long, help = "download parallel", default_value_t = 1024)]
    pub parallel: usize,
    #[arg(
        short,
        long,
        help = "how many checkpots to start to commit",
        default_value_t = 8192
    )]
    pub interval: usize,
    #[arg(
        short,
        long,
        help = "how many checkpoints to commit once",
        default_value_t = 1024
    )]
    pub segment: usize,
    #[arg(short, long)]
    pub target: Option<u64>,
    #[arg(short, long)]
    pub epoch_directory: Option<PathBuf>,
    #[arg(short, long, default_value = "http://127.0.0.1:9000")]
    pub rpc: String,
    #[arg(long, default_value_t = 60)]
    pub retry_timeout: u64,
    #[arg(long, default_value_t = 60)]
    pub http_timeout: u64,
    #[arg(long, default_value_t = 10)]
    pub checkpoint_timeout: u64,
}

impl ForkArgs {
    pub async fn run(self) -> Result<(), BelobogError> {
        let db = ObjectForkDatabase::new(&self.db, false).await?;

        let retry_config = RetryConfig {
            max_retries: usize::MAX,
            retry_timeout: Duration::from_secs(60),
            ..Default::default()
        };
        let client_options = ClientOptions::new()
            .with_timeout(Duration::from_secs(60))
            .with_allow_http(true);
        let http_store = object_store::http::HttpBuilder::new()
            .with_url("https://checkpoints.mainnet.sui.io")
            .with_client_options(client_options)
            .with_retry(retry_config)
            .build()?;
        let checkpoint_store = Arc::new(http_store) as Arc<dyn object_store::ObjectStore>;
        let rpc = SuiClientBuilder::default().build(&self.rpc).await?;
        let fork_checkpoint = if let Some(target) = self.target {
            target
        } else {
            rpc.read_api()
                .get_latest_checkpoint_sequence_number()
                .await?
        };

        let checkpoint_timeout = Duration::from_secs(self.checkpoint_timeout);
        if let Some(epoch_dir) = self.epoch_directory.as_ref() {
            let db_meta = db.db.metadata().await?.unwrap();
            if db_meta.checkpoint != 0 {
                return Err(eyre!(
                    "can not fork epoch for non-empty db, the db already at {}",
                    db_meta.checkpoint
                )
                .into());
            }
            std::fs::create_dir_all(epoch_dir)?;
            let current_ckpt = rpc
                .read_api()
                .get_checkpoint(fork_checkpoint.into())
                .await?;
            let current_epoch = current_ckpt.epoch;
            let (fork_epoch, fork_epoch_checkpoint) = if current_ckpt.end_of_epoch_data.is_some() {
                (current_epoch, fork_checkpoint + 1)
            } else {
                let first_checkpoint_in_epoch = find_first_checkpoint_in_epoch(
                    OrphanRuleWrapper(checkpoint_store.clone()),
                    current_epoch,
                    current_ckpt.sequence_number,
                    checkpoint_timeout,
                )
                .await?;
                (current_epoch - 1, first_checkpoint_in_epoch)
            };

            let snapshot_store_config = ObjectStoreConfig {
                object_store: Some(ObjectStoreType::S3),
                bucket: None,
                aws_access_key_id: None,
                aws_secret_access_key: None,
                aws_region: None,
                aws_endpoint: Some("https://formal-snapshot.mainnet.sui.io".to_string()),
                aws_virtual_hosted_style_request: true,
                object_store_connection_limit: 200,
                no_sign_request: true,
                ..Default::default()
            };
            let snapshot_store = snapshot_store_config.make_http()?;

            let local_store_config = ObjectStoreConfig {
                object_store: Some(ObjectStoreType::File),
                directory: Some(epoch_dir.clone()),
                ..Default::default()
            };
            let local_store = local_store_config.make()?;
            info!("Downloading epoch database for {fork_epoch}...");

            let (manifest_bytes, manifest) = manifest_from_local_or_remote(
                snapshot_store.clone(),
                local_store.clone(),
                fork_epoch,
            )
            .await?;

            local_store
                .put_bytes(
                    &object_store::path::Path::from(format!("epoch_{}/MANIFEST", fork_epoch)),
                    manifest_bytes.into(),
                )
                .await?;

            download_epoch_database(
                &manifest,
                snapshot_store,
                local_store.clone(),
                fork_epoch,
                self.parallel,
                checkpoint_timeout,
            )
            .await?;
            let epoch_path = object_store::path::Path::from(format!("epoch_{fork_epoch}"));

            for object in manifest
                .file_metadata()
                .iter()
                .filter(|t| t.file_type == FileType::Object)
            {
                let object_path = object.file_path(&epoch_path);
                let object_bytes = local_store.get_bytes(&object_path).await?;

                let mut memdb = MemoryDatabase::default();
                for object in
                    LiveObjectIter::new(object, object_bytes)?.filter_map(|t| t.to_normal())
                {
                    memdb.commit_object(object);
                }

                info!("Committing memdb for {object_path}");
                let tx = db.db.env.begin_rw_txn().await?;
                db.unsafe_import_memory_db(&tx, memdb).await?;
                tx.commit().await?;
            }

            info!("Committing metadata...");
            let mut new_meta = db_meta.clone();
            new_meta.checkpoint = fork_epoch_checkpoint;
            db.db.write_metadata(&new_meta).await?;
        }

        loop {
            let metadata = db.db.metadata().await?.expect("no meta?!");
            let lhs = metadata.checkpoint as usize;

            let (rhs, target) = if let Some(target) = self.target {
                let target = target as usize + 1;
                if lhs == target {
                    break;
                }
                ((lhs + self.interval).min(target), target)
            } else {
                let target = rpc
                    .read_api()
                    .get_latest_checkpoint_sequence_number()
                    .await? as usize
                    + 1; // exclusive
                ((lhs + self.interval).min(target as usize), target)
            };

            if lhs == target {
                tokio::time::sleep(Duration::from_secs(1)).await;
            } else {
                for commit_range in (lhs..rhs).range_chunks(self.interval) {
                    let mut ckpts = vec![];
                    let start = commit_range.start;
                    let end = commit_range.end;
                    for download_range in commit_range.range_chunks(self.parallel) {
                        debug!(
                            "Downloading [{}..{})",
                            &download_range.start, &download_range.end
                        );
                        let cs = load_checkpoints(
                            checkpoint_store.clone(),
                            download_range.start as _,
                            download_range.end as _,
                            Duration::from_secs(self.checkpoint_timeout),
                        )
                        .await?;
                        ckpts.extend(cs.into_iter());
                    }

                    info!("Fetching [{start}..{end}) done, committing...");
                    for segs in ckpts.into_iter().chunks(self.segment).into_iter() {
                        let segs = segs.into_iter().collect_vec();
                        debug!(
                            "Committing [{}, {})",
                            segs.first().unwrap().checkpoint_summary.sequence_number,
                            segs.last().unwrap().checkpoint_summary.sequence_number
                        );
                        let mut memdb = MemoryDatabase::default();
                        for ckpt in segs.into_iter() {
                            memdb.commit_checkpoint(ckpt)?;
                        }

                        db.commit_memory_db(memdb).await?;
                    }
                }
            }
        }

        Ok(())
    }
}
