#![allow(clippy::field_reassign_with_default)]

use std::{
    collections::{BTreeMap, BTreeSet, HashSet},
    fmt::Display,
    path::Path,
    str::FromStr,
};

use belobog_types::error::BelobogError;
use color_eyre::eyre::{OptionExt, eyre};
use itertools::Itertools;
use log::{debug, info, trace};
use move_binary_format::{
    CompiledModule,
    file_format::Bytecode,
    normalized::{Module, NoPool},
};
use move_compiler::editions::Flavor;
use move_core_types::{account_address::AccountAddress, u256::U256};
use move_package::{
    resolution::dependency_graph::DependencyMode, source_package::layout::SourcePackageLayout,
};
use move_vm_types::values::IntegerValue;
use serde::{Deserialize, Serialize};
use sui_json_rpc_types::{
    SuiCallArg, SuiMoveNormalizedModule, SuiMoveVisibility, SuiObjectArg, SuiObjectDataOptions,
    SuiRawData, SuiTransactionBlockData, SuiTransactionBlockKind,
    SuiTransactionBlockResponseOptions, SuiTransactionBlockResponseQuery, TransactionFilter,
};
use sui_move_build::{BuildConfig, CompiledPackage, implicit_deps};
use sui_package_management::system_package_versions::latest_system_packages;
use sui_sdk::SuiClient;
use sui_types::{
    base_types::{ObjectID, SequenceNumber},
    digests::get_mainnet_chain_identifier,
    object::{Data, Object},
};

pub fn build_package(folder: &Path) -> Result<CompiledPackage, BelobogError> {
    let mut cfg = move_package::BuildConfig::default();
    cfg.implicit_dependencies = implicit_deps(latest_system_packages());
    cfg.default_flavor = Some(Flavor::Sui);
    cfg.lock_file = Some(folder.join(SourcePackageLayout::Lock.path()));

    let cfg = BuildConfig {
        config: cfg,
        run_bytecode_verifier: false,
        print_diags_to_stderr: false,
        chain_id: Some(get_mainnet_chain_identifier().to_string()),
    };
    trace!("Build config is {:?}", &cfg.config);

    // cfg.compile_package(path, writer) // reference
    let artifacts = cfg.build(folder)?;
    Ok(artifacts)
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Magic {
    U8(u8),
    U16(u16),
    U32(u32),
    U64(u64),
    U128(u128),
    U256(U256),
    Bytes(Vec<u8>),
}

impl Display for Magic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::U8(v) => f.write_fmt(format_args!("U8({})", v)),
            Self::U16(v) => f.write_fmt(format_args!("U16({})", v)),
            Self::U32(v) => f.write_fmt(format_args!("U32({})", v)),
            Self::U64(v) => f.write_fmt(format_args!("U64({})", v)),
            Self::U128(v) => f.write_fmt(format_args!("U128({})", v)),
            Self::U256(v) => f.write_fmt(format_args!("U256({})", v)),
            Self::Bytes(v) => f.write_fmt(format_args!("Bytes({})", const_hex::encode(v))),
        }
    }
}

impl From<IntegerValue> for Magic {
    fn from(value: IntegerValue) -> Self {
        match value {
            IntegerValue::U8(v) => Self::U8(v),
            IntegerValue::U16(v) => Self::U16(v),
            IntegerValue::U32(v) => Self::U32(v),
            IntegerValue::U64(v) => Self::U64(v),
            IntegerValue::U128(v) => Self::U128(v),
            IntegerValue::U256(v) => Self::U256(v),
        }
    }
}

pub type MagicIntegers = BTreeMap<String, BTreeMap<String, Vec<Magic>>>;

pub struct CompiledPackageMeta {
    pub packages: Vec<(String, Vec<Vec<u8>>, Vec<ObjectID>)>,
}

// Everything we need about a package
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PackageMetadata {
    pub abis: BTreeMap<String, SuiMoveNormalizedModule>,
    pub magics: MagicIntegers,
}

impl PackageMetadata {
    pub fn new() -> Self {
        Self {
            abis: BTreeMap::new(),
            magics: BTreeMap::new(),
        }
    }

    pub fn normalize_compiled_module(
        module: &CompiledModule,
    ) -> Result<SuiMoveNormalizedModule, BelobogError> {
        let module = Module::new(&mut NoPool {}, module, true);
        Ok(SuiMoveNormalizedModule::from(&module))
    }

    // fn decode_bytecode_to_abi(mv: &[u8]) -> Result<SuiMoveNormalizedModule, BelobogError> {
    //     let module = CompiledModule::deserialize_with_defaults(mv)?;
    //     Self::normalize_compiled_module(&module)
    // }

    fn magic(module: &CompiledModule) -> BTreeMap<String, Vec<Magic>> {
        let mut magics: BTreeMap<String, Vec<Magic>> = BTreeMap::new();

        for fc in module.function_defs() {
            let fh = module.function_handle_at(fc.function);
            let fname = module.identifier_at(fh.name).to_string();

            let mut fc_magics = BTreeSet::new();
            for code in fc.code.iter().flat_map(|t| t.code.iter()) {
                match code {
                    Bytecode::LdConst(cst) => {
                        let cst = module.constant_at(*cst);
                        fc_magics.insert(Magic::Bytes(cst.data.clone()));
                    }
                    Bytecode::LdU8(v) => {
                        fc_magics.insert(Magic::U8(*v));
                    }
                    Bytecode::LdU16(v) => {
                        fc_magics.insert(Magic::U16(*v));
                    }
                    Bytecode::LdU32(v) => {
                        fc_magics.insert(Magic::U32(*v));
                    }
                    Bytecode::LdU64(v) => {
                        fc_magics.insert(Magic::U64(*v));
                    }
                    Bytecode::LdU128(v) => {
                        fc_magics.insert(Magic::U128(**v));
                    }
                    Bytecode::LdU256(v) => {
                        fc_magics.insert(Magic::U256(**v));
                    }
                    _ => {}
                }
            }

            magics.insert(fname, fc_magics.into_iter().collect_vec());
        }

        magics
    }

    pub fn from_folder_unpublished_or_root(
        folder: &Path,
    ) -> Result<Vec<(Vec<CompiledModule>, Vec<ObjectID>, Self)>, BelobogError> {
        let artifacts = build_package(folder)?;
        debug!(
            "artifacts dep: {:?}",
            artifacts.dependency_graph.topological_order()
        );
        debug!("published: {:?}", artifacts.dependency_ids.published);

        let mut packages_with_deps = vec![];
        // let mut dependency_ids = artifacts.dependency_ids.published.clone().into_iter().map(|(k, v)| (k, vec![v])).collect::<BTreeMap<_, _>>();
        let mut id_with_deps: BTreeMap<ObjectID, Vec<ObjectID>> = BTreeMap::new();
        let mut dependency_package_ids = vec![];
        for package in artifacts.dependency_graph.topological_order() {
            if artifacts.dependency_ids.published.contains_key(&package) {
                debug!("Package {} is published, skip", package);
                continue;
            }
            let package_id = artifacts
                .package
                .all_compiled_units()
                .find(|m| m.package_name().unwrap() == package)
                .map(|m| (*m.module.self_id().address()).into())
                .expect("package must have at least one module");
            if !dependency_package_ids.contains(&package_id) {
                dependency_package_ids.push(package_id);
            }
        }
        for package_id in dependency_package_ids.iter().rev() {
            if id_with_deps.contains_key(package_id) {
                debug!("Package id {} already exists", package_id);
                continue;
            }
            let modules = artifacts
                .package
                .all_compiled_units()
                .filter(|m| *package_id == m.address.into_inner().into())
                .map(|m| m.module.clone())
                .collect::<Vec<_>>();
            debug!("Package {} has {} modules", package_id, modules.len());
            let immidiate_deps = modules
                .iter()
                .flat_map(|m| {
                    m.immediate_dependencies()
                        .iter()
                        .map(|m| m.address().to_owned().into())
                        .filter(|a| a != package_id)
                        .collect::<Vec<_>>()
                })
                .collect::<BTreeSet<ObjectID>>();
            // transitive deps
            let deps = immidiate_deps
                .iter()
                .flat_map(|dep| id_with_deps.get(dep).cloned().unwrap_or_default())
                .chain(artifacts.dependency_ids.published.values().cloned())
                .collect::<BTreeSet<_>>();
            let deps = deps.into_iter().collect::<Vec<_>>();
            id_with_deps.insert(
                *package_id,
                deps.iter()
                    .chain(std::iter::once(package_id))
                    .cloned()
                    .collect(),
            );

            debug!(
                "Package {} transitively depends on {}",
                package_id,
                deps.iter().map(|t| t.to_string()).join(",")
            );

            let mut magics = MagicIntegers::new();
            for md in modules.iter() {
                magics.insert(md.name().to_string(), Self::magic(md));
            }
            let mut abis = BTreeMap::new();
            for (i, module) in modules.iter().enumerate() {
                let normed = Self::normalize_compiled_module(module)?;
                debug!(
                    "Module[{}] {} with address {} has {} functions",
                    i,
                    module.name(),
                    module.address(),
                    normed.exposed_functions.len(),
                );
                abis.insert(module.name().to_string(), normed);
            }

            packages_with_deps.push((modules, deps, Self { abis, magics }));
        }

        debug!("all modules: {}", artifacts.get_modules_and_deps().count());
        debug!(
            "packages with deps: {:?}",
            packages_with_deps
                .iter()
                .map(|(_, deps, _)| deps)
                .collect::<Vec<_>>()
        );
        Ok(packages_with_deps)
    }

    pub async fn from_rpc(
        rpc: &SuiClient,
        address: ObjectID,
    ) -> Result<(Object, Self), BelobogError> {
        let abis = rpc
            .read_api()
            .get_normalized_move_modules_by_package(address)
            .await?;
        let resp = rpc
            .read_api()
            .get_object_with_options(
                address,
                SuiObjectDataOptions {
                    show_bcs: true,
                    show_previous_transaction: true,
                    ..Default::default()
                },
            )
            .await?
            .data
            .ok_or_eyre(eyre!("no data?!"))?;
        let content = resp.bcs.expect("no content?!");
        let previous_digest = resp.previous_transaction.expect("no previous digest?!");
        match content {
            SuiRawData::Package(p) => {
                let mut magics: MagicIntegers = BTreeMap::new();
                for md in p.module_map.values() {
                    let md: CompiledModule = CompiledModule::deserialize_with_defaults(md)?;
                    let functions = Self::magic(&md);
                    magics.insert(md.name().to_string(), functions);
                }
                let object = Object::new_package_from_data(
                    // Note max_move_package_size does NOT go to object content
                    Data::Package(
                        p.to_move_package(1_000_000_000_000_000)
                            .expect("fail to create object"),
                    ),
                    previous_digest,
                );

                Ok((object, Self { abis, magics }))
            }
            SuiRawData::MoveObject(_) => Err(eyre!("{} is not a move package", address).into()),
        }
    }

    async fn fetch_move_objects_from_function(
        rpc: &SuiClient,
        ptbs: &mut HashSet<sui_types::digests::TransactionDigest>,
        address: ObjectID,
        module_name: &str,
        function_name: &str,
    ) -> Option<Vec<(ObjectID, SequenceNumber)>> {
        let query = SuiTransactionBlockResponseQuery {
            filter: Some(TransactionFilter::MoveFunction {
                package: address,
                module: Some(module_name.to_string()),
                function: Some(function_name.to_string()),
            }),
            options: Some(SuiTransactionBlockResponseOptions {
                show_input: true,
                show_effects: false,
                show_events: false,
                show_raw_input: false,
                show_raw_effects: false,
                show_object_changes: false,
                show_balance_changes: false,
            }),
        };
        let cursor = None;
        let limit = Some(5);
        let descending_order = false;
        let response = rpc
            .read_api()
            .query_transaction_blocks(query, cursor, limit, descending_order)
            .await
            .ok()?;
        for tx in response.data {
            let digest = tx.digest;
            if ptbs.contains(&digest) {
                continue;
            } else {
                ptbs.insert(digest);
            }
            let Some(tx) = tx.transaction else {
                continue;
            };
            let SuiTransactionBlockData::V1(tx) = tx.data;
            let SuiTransactionBlockKind::ProgrammableTransaction(ptb) = tx.transaction else {
                continue;
            };
            let tx_objects = ptb
                .inputs
                .iter()
                .filter_map(|input| {
                    if let SuiCallArg::Object(obj_arg) = input {
                        match obj_arg {
                            SuiObjectArg::ImmOrOwnedObject {
                                object_id,
                                version,
                                digest: _,
                            } => Some((*object_id, *version)),
                            SuiObjectArg::SharedObject {
                                object_id,
                                initial_shared_version,
                                mutable: _,
                            } => Some((*object_id, *initial_shared_version)),
                            SuiObjectArg::Receiving {
                                object_id,
                                version,
                                digest: _,
                            } => Some((*object_id, *version)),
                        }
                    } else {
                        None
                    }
                })
                .collect::<Vec<(ObjectID, SequenceNumber)>>();
            if tx_objects.is_empty() {
                continue;
            }
            info!("Found {} objects from ptb {}", tx_objects.len(), digest);
            return Some(tx_objects);
        }
        None
    }

    pub async fn fetch_move_objects(
        &self,
        rpc: &SuiClient,
    ) -> Result<BTreeSet<(ObjectID, SequenceNumber)>, BelobogError> {
        let mut ptbs = HashSet::new();
        let mut objects: BTreeSet<(ObjectID, SequenceNumber)> = BTreeSet::new();
        for (mname, module) in self.abis.iter() {
            for (fname, function) in module.exposed_functions.iter() {
                if function.visibility == SuiMoveVisibility::Public || function.is_entry {
                    if let Some(tx_objects) = Self::fetch_move_objects_from_function(
                        rpc,
                        &mut ptbs,
                        self.module_id(),
                        mname,
                        fname,
                    )
                    .await
                    {
                        for obj in tx_objects {
                            objects.insert(obj);
                        }
                    }
                }
            }
        }
        Ok(objects)
    }

    pub fn module_id(&self) -> ObjectID {
        if self.abis.is_empty() {
            panic!("No module found");
        }
        let module = self.abis.values().next().unwrap();
        ObjectID::from_str(&module.address).unwrap()
    }
}
