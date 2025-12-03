use std::{
    collections::{BTreeMap, HashMap, HashSet},
    io::{Read, Write},
    path::PathBuf,
};

use belobog_types::error::BelobogError;
use clap::Args;
use color_eyre::eyre::{OptionExt, eyre};
use log::{debug, info, warn};
use mdbx_derive::{TableObjectDecode, TableObjectEncode, ZstdBcsObject};
use move_binary_format::{
    CompiledModule,
    binary_config::BinaryConfig,
    normalized::{Module, NoPool},
};
use serde::{Deserialize, Serialize};
use sui_sdk::{
    SuiClient, SuiClientBuilder,
    rpc_types::{
        SuiData, SuiMoveAbility, SuiMoveAbilitySet, SuiMoveNormalizedModule, SuiMoveNormalizedType,
        SuiObjectDataOptions, SuiRawData,
    },
    types::base_types::ObjectID,
};
use sui_snapshot::{FileType, reader::LiveObjectIter};
use sui_types::{
    object::Object,
    transaction::{Command, TransactionDataAPI, TransactionKind},
};

use crate::{
    db::ObjectForkDatabase,
    fork::defaut_http_checkpoionts_store,
    snapshot::{load_checkpoints, read_snapshot_manifest_path},
};

#[derive(Serialize, Deserialize, Clone, Debug, ZstdBcsObject)]
#[serde(transparent)]
pub struct DumpedPackages {
    pub values: Vec<ObjectID>,
}

#[derive(Args)]
pub struct DumpPackageArgs {
    #[arg(short, long)]
    pub epoch: PathBuf,
    #[arg(short, long)]
    pub output: PathBuf,
}

impl DumpPackageArgs {
    pub async fn run(self) -> Result<(), BelobogError> {
        let manifest = self.epoch.join("MANIFEST");
        let manifest = read_snapshot_manifest_path(&manifest)?;
        let mut out = vec![];
        for object in manifest
            .file_metadata()
            .iter()
            .filter(|t| t.file_type == FileType::Object)
        {
            let object_path = self
                .epoch
                .join(format!("{}_{}.obj", object.bucket_num, object.part_num));
            let mut fp = std::fs::File::open(&object_path)?;
            let mut objects = vec![];
            fp.read_to_end(&mut objects)?;
            for obj in LiveObjectIter::new(object, objects.into())?.filter_map(|t| t.to_normal()) {
                if obj.is_package() {
                    out.push(obj.id());
                }
            }
            info!("Done with {}", object_path.display());
        }

        let mut fp = std::fs::File::create(&self.output)?;
        fp.write_all(&DumpedPackages { values: out }.table_encode()?)?;

        Ok(())
    }
}
