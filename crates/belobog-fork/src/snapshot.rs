use std::{io::Read, path::Path, time::Duration};

use belobog_types::error::BelobogError;
use byteorder::{BigEndian, ReadBytesExt};
use color_eyre::eyre::eyre;
use fastcrypto::hash::{HashFunction, Sha3_256};
use itertools::Itertools as _;
use log::{debug, info, trace, warn};
use move_core_types::language_storage::StructTag;
use std::str::FromStr;
use sui_sdk::{SuiClient, rpc_types::EventFilter, types::full_checkpoint_content::CheckpointData};
use sui_snapshot::Manifest;
use sui_storage::{
    blob::Blob,
    object_store::{ObjectStoreGetExt, ObjectStoreListExt, ObjectStorePutExt, util::copy_file},
};
use tokio::task::JoinSet;
use tokio_stream::StreamExt;

const MANIFEST_FILE_MAGIC: u32 = 0x00C0FFEE;
const MAGIC_BYTES: usize = 4;

pub fn read_snapshot_manifest_path(path: &Path) -> Result<Manifest, BelobogError> {
    let mut fp = std::fs::File::open(path)?;
    let mut manifest = vec![];
    fp.read_to_end(&mut manifest)?;
    read_snapshot_manifest(&manifest)
}

pub fn read_snapshot_manifest(bytes: &[u8]) -> Result<Manifest, BelobogError> {
    if bytes.len() < MAGIC_BYTES + sui_storage::SHA3_BYTES {
        return Err(eyre!("Manifest data too short").into());
    }

    let mut reader = std::io::Cursor::new(bytes);
    let magic = reader.read_u32::<BigEndian>()?;
    if magic != MANIFEST_FILE_MAGIC {
        return Err(eyre!("Unexpected magic byte: {}", magic).into());
    }

    // Get the content and digest parts
    let content_end = bytes.len() - sui_storage::SHA3_BYTES;
    let content = &bytes[MAGIC_BYTES..content_end];
    let sha3_digest = &bytes[content_end..];

    // Verify checksum
    let mut hasher = Sha3_256::default();
    hasher.update(&bytes[..content_end]);
    let computed_digest = hasher.finalize().digest;
    if computed_digest != sha3_digest {
        return Err(eyre!(
            "Checksum: {:?} don't match: {:?}",
            computed_digest,
            sha3_digest
        )
        .into());
    }

    let manifest = bcs::from_bytes(content)?;
    Ok(manifest)
}

pub async fn manifest_from_local_or_remote<S, D>(
    snapshot_store: S,
    local_store: D,
    epoch: u64,
) -> Result<(Vec<u8>, Manifest), BelobogError>
where
    S: Clone + ObjectStoreGetExt,
    D: Clone + ObjectStoreGetExt + ObjectStoreListExt + ObjectStorePutExt,
{
    let epoch_dir = format!("epoch_{epoch}");
    let manifest_file_path = object_store::path::Path::from(epoch_dir.clone()).child("MANIFEST");

    match local_store.get_bytes(&manifest_file_path).await {
        Ok(v) => read_snapshot_manifest(&v).map(|m| (Vec::from_iter(v.into_iter()), m)),
        Err(e) => {
            debug!("Fail to fetch from local: {}", e);
            info!("Local store does not have manifest, fetching from remote...");
            let manifest_bytes = snapshot_store.get_bytes(&manifest_file_path).await?;
            read_snapshot_manifest(&manifest_bytes)
                .map(|m| (Vec::from_iter(manifest_bytes.into_iter()), m))
        }
    }
}

pub async fn download_epoch_database<S, D>(
    manifest: &Manifest,
    snapshot_store: S,
    local_store: D,
    epoch: u64,
    download_parallel: usize,
    timeout: Duration,
) -> Result<(), BelobogError>
where
    S: Clone + ObjectStoreGetExt,
    D: Clone + ObjectStoreListExt + ObjectStorePutExt,
{
    let epoch_dir = format!("epoch_{epoch}");
    let epoch_dir_path = object_store::path::Path::from(epoch_dir.clone());

    // Filter files that need to be downloaded
    let files_to_download = {
        let mut list_stream = local_store.list_objects(Some(&epoch_dir_path)).await;
        let mut existing_files = std::collections::HashSet::new();
        while let Some(meta) = list_stream.try_next().await? {
            existing_files.insert(meta.location);
        }
        let mut missing_files = Vec::new();
        for file_metadata in manifest.file_metadata() {
            let file_path = file_metadata.file_path(&epoch_dir_path);
            if !existing_files.contains(&file_path) {
                missing_files.push(file_path);
            }
        }
        missing_files
    };

    info!(
        "Snapshot download status: {}/{} files already exist locally ({}% complete), {} files to download",
        manifest.file_metadata().len() - files_to_download.len(),
        manifest.file_metadata().len(),
        ((manifest.file_metadata().len() - files_to_download.len()) * 100)
            / manifest.file_metadata().len(),
        files_to_download.len()
    );

    for chunk in files_to_download
        .clone()
        .into_iter()
        .zip(files_to_download.clone().into_iter())
        .chunks(download_parallel)
        .into_iter()
        .map(|t| t.into_iter())
    {
        let mut js = JoinSet::new();
        for (src, dst) in chunk.into_iter() {
            let src_store = snapshot_store.clone();
            let dst_store = local_store.clone();

            js.spawn(async move {
                loop {
                    match tokio::time::timeout(
                        timeout,
                        copy_file(&src, &dst, &src_store, &dst_store),
                    )
                    .await
                    {
                        Ok(r) => {
                            debug!("Downloaded {} to {}", &src, &dst);
                            return r;
                        }
                        Err(_) => {
                            warn!(
                                "Download {} timeout after {} seconds",
                                &src,
                                timeout.as_secs_f64()
                            );
                            continue;
                        }
                    }
                }
            });
        }
        let l = js.len();
        js.join_all()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;
        info!("Finished {l} downloads");
    }

    Ok(())
}

pub async fn load_checkpoints<S>(
    checkpoint_store: S,
    from: u64, // inclusive!
    to: u64,   // exclusive!
    timeout: Duration,
) -> Result<Vec<CheckpointData>, BelobogError>
where
    S: object_store::ObjectStore + Clone,
{
    let mut js = JoinSet::new();
    for ckpt in from..to {
        let remote = checkpoint_store.clone();
        js.spawn(async move {
            let ckpt_path = object_store::path::Path::from(format!("{ckpt}.chk"));
            loop {
                match tokio::time::timeout(timeout, remote.get(&ckpt_path)).await {
                    Ok(resp) => {
                        debug!("Downloaded {}", &ckpt_path);
                        match resp {
                            Ok(resp) => match resp.bytes().await {
                                Ok(data) => {
                                    let ckpt_data = Blob::from_bytes::<CheckpointData>(&data)?;
                                    return Ok::<_, BelobogError>((ckpt, ckpt_data));
                                }
                                Err(e) => {
                                    warn!("Fail to stream data due to {e}");
                                    continue;
                                }
                            },
                            Err(e) => {
                                warn!("Fail to get resp due to {e}");
                                continue;
                            }
                        }
                    }
                    Err(_) => {
                        warn!(
                            "Downloading {} timeout after {} seconds",
                            &ckpt_path,
                            timeout.as_secs_f64()
                        );
                        continue;
                    }
                }
            }
        });
    }
    debug!("Downloading {} ckpts", js.len());
    let outs = js.join_all().await;
    Ok(outs
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .sorted_by_key(|t| t.0)
        .map(|t| t.1)
        .collect())
}

pub async fn load_single_checkpoints<S>(
    checkpoint_store: S,
    checkpoint: u64,
    timeout: Duration,
) -> Result<CheckpointData, BelobogError>
where
    S: object_store::ObjectStore + Clone,
{
    Ok(
        load_checkpoints(checkpoint_store, checkpoint, checkpoint + 1, timeout)
            .await?
            .remove(0),
    )
}

#[auto_impl::auto_impl(&, Arc)]
pub trait CanFindCheckpointEpoch {
    fn checkpoint_epoch(
        &self,
        checkpoint: u64,
        timeout: Duration,
    ) -> impl Future<Output = Result<u64, BelobogError>> + Send;
}

pub struct OrphanRuleWrapper<S>(pub S);

impl<S> CanFindCheckpointEpoch for OrphanRuleWrapper<S>
where
    S: object_store::ObjectStore + Clone,
{
    async fn checkpoint_epoch(
        &self,
        checkpoint: u64,
        timeout: Duration,
    ) -> Result<u64, BelobogError> {
        Ok(load_single_checkpoints(self.0.clone(), checkpoint, timeout)
            .await?
            .checkpoint_summary
            .epoch)
    }
}

impl CanFindCheckpointEpoch for SuiClient {
    async fn checkpoint_epoch(
        &self,
        checkpoint: u64,
        _timeout: Duration,
    ) -> Result<u64, BelobogError> {
        Ok(self
            .read_api()
            .get_checkpoint(checkpoint.into())
            .await?
            .epoch)
    }
}

pub async fn find_first_checkpoint_in_epoch<K>(
    store: K,
    target_epoch: u64,
    start_checkpoint: u64,
    timeout: Duration,
) -> Result<u64, BelobogError>
where
    K: CanFindCheckpointEpoch,
{
    if target_epoch == 0 {
        return Err(eyre!("Invalid epoch_id: epochs start at 1").into());
    }

    let ckpt_epoch = store.checkpoint_epoch(start_checkpoint, timeout).await?;

    let (low, high) = if ckpt_epoch >= target_epoch {
        // Find a low enough checkpoint
        let mut low = start_checkpoint;
        while low >= 1 {
            low = low.saturating_div(2);
            let low_ckpt_epoch = store.checkpoint_epoch(low, timeout).await?;
            trace!(
                "Search low checkpoint, low = {}, epoch = {}",
                low, low_ckpt_epoch
            );
            if low_ckpt_epoch < target_epoch {
                break;
            }
        }
        (low, start_checkpoint)
    } else {
        let mut high = start_checkpoint;
        while high < u64::MAX {
            high = high.saturating_mul(2);
            let high_ckpt_epoch = store.checkpoint_epoch(high, timeout).await?;
            trace!(
                "Search hihg checkpoint, hihg = {}, epoch = {}",
                high, high_ckpt_epoch
            );
            if high_ckpt_epoch >= target_epoch {
                break;
            }
        }
        (start_checkpoint, high)
    };

    let mut left = low;
    let mut right = high;
    let mut result = None;

    while left <= right {
        let mid = left + (right - left) / 2;
        let mid_eopch = store.checkpoint_epoch(mid, timeout).await?;
        trace!(
            "left = {}, right = {}, mid = {}, ckpt.epoch = {}, target = {}",
            left, right, mid, mid_eopch, target_epoch
        );
        if mid_eopch < target_epoch {
            left = mid + 1;
        } else {
            result = Some(mid);
            if mid == 0 {
                break;
            }
            right = mid.saturating_sub(1);
        }
    }

    // Verify we found a checkpoint and it belongs exactly to epoch_id
    if let Some(first) = result {
        let first_epoch = store.checkpoint_epoch(first, timeout).await?;
        if first_epoch == target_epoch {
            if first > 0 {
                let previous_ckpt_epoch = store.checkpoint_epoch(first - 1, timeout).await?;
                if previous_ckpt_epoch == target_epoch - 1 {
                    return Ok(first);
                } else {
                    return Err(eyre!(
                        "we found {}:{} but not the first checkpoint?! previous {}:{}",
                        first,
                        first_epoch,
                        first - 1,
                        previous_ckpt_epoch
                    )
                    .into());
                }
            } else {
                return Ok(first);
            }
        }
    }

    Err(eyre!(
        "Epoch {} not found in checkpoints from {}",
        target_epoch,
        start_checkpoint
    )
    .into())
}

const EPOCH_CHANGE_STRUCT_TAG: &str = "0x3::sui_system_state_inner::SystemEpochInfoEvent";

pub async fn find_reference_gas_price(
    client: &SuiClient,
    epoch: u64,
    reverse: bool,
) -> Result<Option<u64>, BelobogError> {
    let struct_tag_str = EPOCH_CHANGE_STRUCT_TAG.to_string();
    let struct_tag = StructTag::from_str(&struct_tag_str)?;

    let mut has_next_page = true;
    let mut cursor = None;

    while has_next_page {
        trace!("Query event with cursor = {:?}", cursor);
        let page_data = client
            .event_api()
            .query_events(
                EventFilter::MoveEventType(struct_tag.clone()),
                cursor,
                None,
                reverse,
            )
            .await?;

        for data in page_data.data.into_iter() {
            trace!("Extracted event: {:?}", &data.parsed_json);
            if let serde_json::Value::Object(w) = data.parsed_json {
                let ex_epoch = u64::from_str(&w["epoch"].to_string().replace('\"', ""))
                    .map_err(|_| eyre!("can not convert int"))?;
                // let version = u64::from_str(&w["protocol_version"].to_string().replace('\"', "")).map_err(|_| eyre!("can not convert int"))?;
                let ex_rgp = u64::from_str(&w["reference_gas_price"].to_string().replace('\"', ""))
                    .map_err(|_| eyre!("can not convert int"))?;
                if ex_epoch == epoch {
                    return Ok(Some(ex_rgp));
                }
            }
        }

        has_next_page = page_data.has_next_page;
        cursor = page_data.next_cursor;
    }

    Ok(None)
}
