use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Display,
    str::FromStr,
};

use crate::{
    build::PackageMetadata, r#const::INIT_FUNCTION_SCORE,
    object_sampler::sui_move_ability_set_to_ability_set, type_utils::TypeUtils,
};
use crate::{r#const::PRIVILEGE_FUNCTION_SCORE, metrics::EvaluationMetrics};
use belobog_fork::{ObjectStoreCommit, ObjectTypesStore};
use belobog_types::error::BelobogError;
use color_eyre::eyre::eyre;
use libafl::{HasMetadata, state::HasRand};
use libafl_bolts::{impl_serdeany, rands::Rand};
use log::{debug, trace};
use move_binary_format::{CompiledModule, file_format::AbilitySet};
use move_compiler::sui_mode::{
    BRIDGE_ADDR_VALUE, STD_ADDR_VALUE, SUI_ADDR_VALUE, SUI_SYSTEM_ADDR_VALUE,
};
use move_core_types::{
    account_address::AccountAddress,
    annotated_value::{MoveDatatypeLayout, MoveFieldLayout, MoveStructLayout},
    language_storage::StructTag,
};
use serde::{Deserialize, Serialize};
use serde_json_any_key::*;
use sui_json_rpc_types::{
    SuiMoveNormalizedEnum, SuiMoveNormalizedFunction, SuiMoveNormalizedStruct,
    SuiMoveNormalizedType, SuiMoveVisibility, type_and_fields_from_move_event_data,
};
use sui_sdk::{SuiClient, json::MoveTypeLayout};
use sui_types::{
    Identifier, SUI_CLOCK_ADDRESS, SUI_RANDOMNESS_STATE_ADDRESS, SUI_SYSTEM_STATE_ADDRESS, TypeTag,
    base_types::{ObjectID, SuiAddress},
    digests::TransactionDigest,
    event::Event,
    object::{Data, Object},
    storage::ObjectStore,
};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct FunctionIdent(pub ObjectID, pub String, pub String);

impl From<String> for FunctionIdent {
    fn from(s: String) -> Self {
        let parts: Vec<&str> = s.split("::").collect();
        if parts.len() != 3 {
            panic!("Invalid function ident: {}", s);
        }
        let package_id = ObjectID::from_str(parts[0]).expect("Invalid package id");
        let module_name = parts[1].to_string();
        let function_name = parts[2].to_string();
        FunctionIdent(package_id, module_name, function_name)
    }
}

impl Display for FunctionIdent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}::{}::{}", self.0, self.1, self.2)
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DeterminedDeployment {
    pub counter: u64,
}

impl DeterminedDeployment {
    pub fn chosen_random_digest() -> TransactionDigest {
        // By super fair dice roll
        TransactionDigest::from_str("FV6abUaqa1xrNUtcbaJM3tEyqQeEDwKQnsh9eniZL8k1").unwrap()
    }

    pub fn next_deploy_address(&mut self) -> ObjectID {
        let out = ObjectID::derive_id(Self::chosen_random_digest(), self.counter);
        self.counter += 1;
        out
    }
}

pub trait HasFuzzMetadata {
    fn fuzz_state_mut(&mut self) -> &mut FuzzMetadata;

    fn fuzz_state(&self) -> &FuzzMetadata;

    fn eval_metrics(&self) -> &Option<EvaluationMetrics> {
        &self.fuzz_state().eval_metrics
    }

    fn eval_metrics_mut(&mut self) -> &mut Option<EvaluationMetrics> {
        &mut self.fuzz_state_mut().eval_metrics
    }

    fn may_load_target_module(&mut self, compiled: &CompiledModule) {
        if let Some(metrics) = self.eval_metrics_mut() {
            metrics.load_target_module(compiled);
        }
    }
}

impl<T: HasMetadata> HasFuzzMetadata for T {
    fn fuzz_state(&self) -> &FuzzMetadata {
        self.metadata().expect("meta not installed yet?")
    }

    fn fuzz_state_mut(&mut self) -> &mut FuzzMetadata {
        self.metadata_mut().expect("meta not installed yet?")
    }
}

pub trait HasCaller {
    /// Get a random address from the address set, used for ABI mutation
    fn get_rand_address(&mut self) -> SuiAddress;
    /// Get a random caller from the caller set, used for transaction sender
    /// mutation
    fn get_rand_caller(&mut self) -> SuiAddress;
    /// Does the address exist in the caller set
    fn has_caller(&self, addr: &SuiAddress) -> bool;
    /// Add a caller to the caller set
    fn add_caller(&mut self, caller: &SuiAddress);
    /// Add an address to the address set
    fn add_address(&mut self, caller: &SuiAddress);
}

impl<T: HasFuzzMetadata + HasRand> HasCaller for T {
    /// Get a random address from the address pool, used for ABI mutation
    fn get_rand_address(&mut self) -> SuiAddress {
        let length = self.fuzz_state().addresses_pool.len();
        let idx = self.rand_mut().below_or_zero(length);
        self.fuzz_state_mut().addresses_pool[idx]
    }

    /// Get a random caller from the caller pool, used for mutating the caller
    fn get_rand_caller(&mut self) -> SuiAddress {
        let length = self.fuzz_state().callers_pool.len();
        let idx = self.rand_mut().below_or_zero(length);
        self.fuzz_state_mut().callers_pool[idx]
    }

    /// Get a random caller from the caller pool, used for mutating the caller
    fn has_caller(&self, addr: &SuiAddress) -> bool {
        self.fuzz_state().callers_pool.contains(addr)
    }

    /// Add a caller to the caller pool
    fn add_caller(&mut self, addr: &SuiAddress) {
        let callers_pool = &mut self.fuzz_state_mut().callers_pool;
        if !callers_pool.contains(addr) {
            callers_pool.push(*addr);
        }
    }

    /// Add an address to the address pool
    fn add_address(&mut self, caller: &SuiAddress) {
        let addresses_pool = &mut self.fuzz_state_mut().addresses_pool;
        if !addresses_pool.contains(caller) {
            addresses_pool.push(*caller);
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum MutatorKind {
    Sequence,
    Arg,
    Magic,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FuzzMetadata {
    pub attacker: SuiAddress,
    pub admin: SuiAddress,
    pub current_sender: SuiAddress,
    pub packages: BTreeMap<ObjectID, PackageMetadata>,
    #[serde(with = "any_key_map")]
    pub module_address_to_package: BTreeMap<ObjectID, ObjectID>,
    pub target_functions: Vec<FunctionIdent>,
    pub deploy: DeterminedDeployment,
    // value function consumes a specific hot potato produced by key struct
    pub consuming_graph: Vec<(SuiMoveNormalizedType, Vec<FunctionIdent>)>,
    // value function produces a specific struct value consumed by key struct
    pub producing_graph: Vec<(SuiMoveNormalizedType, Vec<(FunctionIdent, u16)>)>,
    #[serde(with = "any_key_map")]
    pub function_scores: BTreeMap<FunctionIdent, u64>,
    pub privilege_functions: BTreeSet<FunctionIdent>,
    pub callers_pool: Vec<SuiAddress>,
    pub addresses_pool: Vec<SuiAddress>,
    #[serde(with = "any_key_map")]
    pub ability_to_type_tag: BTreeMap<AbilitySet, Vec<TypeTag>>,
    pub gas_id: Option<ObjectID>,
    pub current_mutator: Option<MutatorKind>,
    pub eval_metrics: Option<EvaluationMetrics>,
}

impl_serdeany!(FuzzMetadata);

impl FuzzMetadata {
    // pub fn deploy<T: ObjectStoreCommit>(
    //     &mut self,
    //     packages: PackageMetadata,
    //     db: &T
    // ) -> Result<(), BelobogError> {
    //     let address = self.deploy.next_deploy_address();
    //     PackageObject::new(package_object)
    //     db.commit_single_object(object);
    // }

    pub fn set_to_attacker(&mut self) {
        self.current_sender = self.attacker;
    }

    pub fn set_to_admin(&mut self) {
        self.current_sender = self.admin;
    }

    pub async fn register_system_packages(&mut self, rpc: &SuiClient) -> Result<(), BelobogError> {
        for sys in [
            STD_ADDR_VALUE,
            SUI_ADDR_VALUE,
            SUI_SYSTEM_ADDR_VALUE,
            BRIDGE_ADDR_VALUE,
        ] {
            let package = PackageMetadata::from_rpc(rpc, sys.into()).await?;
            debug!("Registered system package: {}", package.0.id());
            self.packages.insert(package.0.id(), package.1);
        }
        Ok(())
    }

    pub async fn register_sui_objects<T>(&mut self, db: &T) -> Result<(), BelobogError>
    where
        T: ObjectStore,
    {
        let sui_objects = [
            SUI_CLOCK_ADDRESS,
            SUI_SYSTEM_STATE_ADDRESS,
            SUI_RANDOMNESS_STATE_ADDRESS,
        ];
        for obj in sui_objects {
            db.get_object(&obj.into()) // just check if exist
                .ok_or(eyre!("Fail to get sui object {}", obj))?;
        }
        Ok(())
    }

    fn decode_event(
        event: &Event,
        ty: MoveDatatypeLayout,
    ) -> Result<(StructTag, serde_json::Value), BelobogError> {
        debug!(
            "may_decode: {} with ty = {:?}",
            const_hex::encode(&event.contents),
            &ty
        );
        Ok(Event::move_event_to_move_value(&event.contents, ty)
            .and_then(type_and_fields_from_move_event_data)?)
    }

    pub fn field_struct_to_move_ty(
        &self,
        package_id: &ObjectID,
        type_params: &Vec<MoveTypeLayout>,
        ty: &SuiMoveNormalizedType,
    ) -> Option<MoveTypeLayout> {
        let out = match ty {
            SuiMoveNormalizedType::Address => Some(MoveTypeLayout::Address),
            SuiMoveNormalizedType::Bool => Some(MoveTypeLayout::Bool),
            SuiMoveNormalizedType::Signer => Some(MoveTypeLayout::Signer),
            SuiMoveNormalizedType::U128 => Some(MoveTypeLayout::U128),
            SuiMoveNormalizedType::U16 => Some(MoveTypeLayout::U16),
            SuiMoveNormalizedType::U256 => Some(MoveTypeLayout::U256),
            SuiMoveNormalizedType::U32 => Some(MoveTypeLayout::U32),
            SuiMoveNormalizedType::U64 => Some(MoveTypeLayout::U64),
            SuiMoveNormalizedType::U8 => Some(MoveTypeLayout::U8),
            SuiMoveNormalizedType::Struct { inner } => {
                // inner is just a ref in sui ecosystem, try to find definitions if any
                trace!("An struct: {:?}, typs = {:?}", &inner, type_params);
                let address = AccountAddress::from_str(&inner.address).unwrap();
                let mut new_ty_params = vec![];

                for typ in inner.type_arguments.iter() {
                    new_ty_params.push(self.field_struct_to_move_ty(
                        package_id,
                        type_params,
                        typ,
                    )?);
                }
                trace!("Calculted typs: {:?}", new_ty_params);
                self.may_know_struct(package_id, &address.into(), &inner.module, &inner.name)
                    .map(|s| {
                        match self.norm_fields_ty_to_move_ty(
                            package_id,
                            StructTag {
                                address,
                                module: Identifier::new(inner.module.clone()).unwrap(),
                                name: Identifier::new(inner.name.clone()).unwrap(),
                                type_params: vec![], // safe empty...
                            },
                            &new_ty_params,
                            s.fields.iter().map(|t| (Some(t.name.clone()), &t.type_)),
                        ) {
                            Some(ty) => Some(MoveTypeLayout::Struct(Box::new(ty))),
                            None => {
                                trace!("Fail to decode struct {:?} even find {:?} ?!", &inner, s);
                                None
                            }
                        }
                    })
                    .flatten()
                    .or_else(|| {
                        trace!("{:?} not found", &inner);
                        match self
                            .norm_fields_ty_to_move_ty(
                                package_id,
                                StructTag {
                                    address: AccountAddress::from_str(&inner.address).unwrap(),
                                    module: Identifier::new(inner.module.clone()).unwrap(),
                                    name: Identifier::new(inner.name.clone()).unwrap(),
                                    type_params: vec![], // safe empty...
                                },
                                &new_ty_params,
                                inner.type_arguments.iter().map(|t| (None, t)),
                            )
                            .map(|t| MoveTypeLayout::Struct(Box::new(t)))
                        {
                            Some(ty) => Some(ty),
                            None => {
                                trace!("Fail to decode struct {:?}", &inner);
                                None
                            }
                        }
                    })
            }
            SuiMoveNormalizedType::Vector(v) => Some(MoveTypeLayout::Vector(Box::new(
                self.field_struct_to_move_ty(package_id, type_params, v)?,
            ))),
            SuiMoveNormalizedType::TypeParameter(idx) => match type_params.get(*idx as usize) {
                Some(v) => Some(v.clone()),
                None => {
                    trace!("type parameter {} not in {:?}", idx, &type_params);
                    None
                }
            },
            SuiMoveNormalizedType::Reference(r) => {
                self.field_struct_to_move_ty(package_id, type_params, r)
            }
            SuiMoveNormalizedType::MutableReference(ty) => {
                self.field_struct_to_move_ty(package_id, type_params, ty)
            }
        };

        trace!("{:?} => {:?}", ty, &out);
        out
    }

    fn norm_fields_ty_to_move_ty<'a>(
        &self,
        package_id: &ObjectID,
        tag: StructTag, // ignore typ
        ty_params: &Vec<MoveTypeLayout>,
        iter_fields: impl Iterator<Item = (Option<String>, &'a SuiMoveNormalizedType)>,
    ) -> Option<MoveStructLayout> {
        let mut cnt = 0;
        let mut fields = vec![];
        for (fdname, fd) in iter_fields {
            let name = if let Some(name) = fdname {
                name
            } else {
                cnt += 1;
                format!("field{}", cnt)
            };
            trace!("Fd = {:?}", &fd);
            let out = match self.field_struct_to_move_ty(package_id, ty_params, fd) {
                Some(v) => v,
                None => {
                    trace!("Fail to decode field {:?}", fd);
                    return None;
                }
            };

            let field = MoveFieldLayout::new(Identifier::new(name).unwrap(), out);
            fields.push(field);
        }
        Some(MoveStructLayout::new(tag, fields))
    }

    fn may_know_struct(
        &self,
        package_id: &ObjectID,
        ty_address: &ObjectID,
        module: &str,
        name: &str,
    ) -> Option<&SuiMoveNormalizedStruct> {
        // TODO: Why what the heck ty_address could be 0x0?!
        match self
            .packages
            .get(ty_address)
            .or_else(|| self.packages.get(package_id))
            .and_then(|package| package.abis.get(&module.to_string()))
            .and_then(|t| t.structs.get(&name.to_string()))
        {
            Some(s) => Some(s),
            None => {
                trace!(
                    "Struct {}:{}:{}:{} not found",
                    package_id, ty_address, module, name
                );
                None
            }
        }
    }

    pub fn try_decode_event(
        &self,
        event: &Event,
    ) -> Result<Option<(StructTag, serde_json::Value)>, BelobogError> {
        self.may_know_struct(
            &event.package_id,
            &event.type_.address.into(),
            event.type_.module.as_str(),
            event.type_.name.as_str(),
        )
        .map(|t| {
            trace!("Move struct: {:?}", t);
            self.norm_fields_ty_to_move_ty(
                &event.package_id,
                event.type_.clone(),
                &vec![], // should be empty anyway
                t.fields.iter().map(|t| (Some(t.name.clone()), &t.type_)),
            )
            .map(|t| Self::decode_event(event, MoveDatatypeLayout::Struct(Box::new(t))))
        })
        .flatten()
        .transpose()
    }

    pub fn iter_functions(
        &self,
    ) -> impl Iterator<
        Item = (
            &ObjectID,                                      // package_addr
            &String,                                        // module_name
            &sui_json_rpc_types::SuiMoveNormalizedModule,   // module_data
            &String,                                        // func_name
            &sui_json_rpc_types::SuiMoveNormalizedFunction, // func_data
        ),
    > {
        self.packages
            .iter()
            .flat_map(|(package_addr, package_meta)| {
                package_meta
                    .abis
                    .iter()
                    .flat_map(move |(module_name, module_data)| {
                        module_data
                            .exposed_functions
                            .iter()
                            .map(move |(func_name, func_data)| {
                                (package_addr, module_name, module_data, func_name, func_data)
                            })
                    })
            })
    }

    pub fn iter_target_functions(
        &self,
    ) -> impl Iterator<
        Item = (
            &ObjectID,                                      // package_addr
            &String,                                        // module_name
            &sui_json_rpc_types::SuiMoveNormalizedModule,   // module_data
            &String,                                        // func_name
            &sui_json_rpc_types::SuiMoveNormalizedFunction, // func_data
        ),
    > {
        self.target_functions.iter().filter_map(
            move |FunctionIdent(package_addr, module_name, func_name)| {
                self.get_function(package_addr, module_name, func_name)
                    .map(|func_data| {
                        let package_meta = self.get_package_metadata(package_addr).unwrap();
                        let module_data = package_meta.abis.get(module_name).unwrap();
                        (package_addr, module_name, module_data, func_name, func_data)
                    })
            },
        )
    }

    pub fn may_add_coverage_target_object(&mut self, object: Object) {
        if let Some(metrics) = &mut self.eval_metrics {
            metrics.load_target_package(object);
        }
    }

    pub fn may_add_coverage_target<DB: ObjectStore>(&mut self, package_id: &ObjectID, db: &DB) {
        if let Some(object) = db.get_object(package_id) {
            self.may_add_coverage_target_object(object);
        } else {
            log::warn!("no such package to coverage metrics: {}", package_id);
        }
    }

    pub fn add_package_to_target(&mut self, package_id: &ObjectID) {
        let mut target_functions = self.target_functions.clone();
        for (mname, module) in self
            .get_package_metadata(package_id)
            .unwrap_or_else(|| {
                panic!("packages {:?}", self.packages);
            })
            .abis
            .iter()
        {
            for (fname, func) in module.exposed_functions.iter() {
                if func.visibility != SuiMoveVisibility::Public {
                    continue; // skip non-public functions
                }
                let ident = FunctionIdent(*package_id, mname.clone(), fname.clone());
                if !target_functions.contains(&ident) {
                    target_functions.push(ident);
                }
            }
        }
        self.target_functions = target_functions;
    }

    pub fn add_function_to_target(
        &mut self,
        package_id: &ObjectID,
        module_name: &str,
        function_name: &str,
    ) {
        let ident = FunctionIdent(
            *package_id,
            module_name.to_string(),
            function_name.to_string(),
        );
        if !self.target_functions.contains(&ident) {
            self.target_functions.push(ident);
        }
    }

    pub fn initialize(&mut self, gas_id: ObjectID, privilege_functions: Vec<FunctionIdent>) {
        self.addresses_pool = vec![
            SuiAddress::from_str(
                "0xeb725b7ffe521cc855c0b5ef71fd41f3c4904b1fa6952ec08c237c5adbe911d8",
            )
            .unwrap(),
        ];
        self.privilege_functions = privilege_functions.into_iter().collect();
        let specified_functions = vec![
            FunctionIdent(
                AccountAddress::TWO.into(),
                "coin".into(),
                "from_balance".into(),
            ),
            FunctionIdent(
                AccountAddress::TWO.into(),
                "coin".into(),
                "into_balance".into(),
            ),
            FunctionIdent(AccountAddress::ONE.into(), "string".into(), "utf8".into()),
            FunctionIdent(AccountAddress::ONE.into(), "ascii".into(), "string".into()),
        ];
        for f in specified_functions.iter() {
            if !self.target_functions.contains(f) {
                self.target_functions.push(f.clone());
            }
        }

        self.callers_pool = vec![self.attacker];
        let mut consuming_graph: Vec<(SuiMoveNormalizedType, Vec<FunctionIdent>)> = Vec::new();
        let mut producing_graph: Vec<(SuiMoveNormalizedType, Vec<(FunctionIdent, u16)>)> =
            Vec::new();
        let mut producing_graph_all: Vec<(SuiMoveNormalizedType, Vec<(FunctionIdent, u16)>)> =
            Vec::new();
        let mut function_scores: BTreeMap<FunctionIdent, u64> = BTreeMap::new();
        for (package_addr, module_name, _module_data, func_name, func_data) in self.iter_functions()
        {
            if func_data.visibility != SuiMoveVisibility::Public {
                continue; // skip non-public functions
            }
            let func_ident = FunctionIdent(*package_addr, module_name.clone(), func_name.clone());
            if !self.target_functions.contains(&func_ident) {
                // skip if function is not in target functions
                continue;
            }
            debug!("Analyzing function: {:?}", &func_ident);
            trace!(
                "Function parameters' ability: {:?}",
                func_data
                    .parameters
                    .iter()
                    .map(|t| t.ability(self))
                    .collect::<Vec<_>>()
            );
            let score = if self.privilege_functions.contains(&func_ident) {
                PRIVILEGE_FUNCTION_SCORE
            } else {
                INIT_FUNCTION_SCORE
            };
            function_scores.entry(func_ident.clone()).or_insert(score);
            for param_ty in func_data.parameters.iter() {
                let SuiMoveNormalizedType::Struct { .. } = param_ty else {
                    continue;
                };
                if !param_ty.is_hot_potato(self) {
                    // skip if parameter is not a hot potato
                    continue;
                }
                consuming_graph
                    .iter_mut()
                    .find(|(ty, _)| ty == param_ty)
                    .map(|(_, funcs)| funcs.push(func_ident.clone()))
                    .unwrap_or_else(|| {
                        consuming_graph.push((param_ty.clone(), vec![func_ident.clone()]));
                    });
            }
        }
        let mut remove_idxs = vec![];
        for (package_addr, module_name, module_data, func_name, func_data) in self.iter_functions()
        {
            if func_data.visibility != SuiMoveVisibility::Public {
                continue; // skip non-public functions
            }
            let func_ident = FunctionIdent(*package_addr, module_name.clone(), func_name.clone());
            if !self.target_functions.contains(&func_ident) {
                // skip if function is not in target functions
                continue;
            }
            debug!("Re-analyzing function: {:?}", &func_ident);
            if func_data.parameters.iter().all(|t| {
                (t.ability(self).is_some_and(|a| a.has_drop()))
                    || matches!(t, SuiMoveNormalizedType::Reference(_))
                    || t.is_tx_context()
            }) && func_data
                .return_
                .iter()
                .all(|t| t.is_mutable() || t.ability(self).is_some_and(|a| a.has_drop()))
            {
                // remove read-only functions
                if !func_data.is_entry {
                    remove_idxs.push(
                        self.target_functions
                            .iter()
                            .position(|f| f == &func_ident)
                            .unwrap(),
                    );
                }
            }

            for (i, ret_ty) in func_data.return_.iter().enumerate() {
                let self_used = func_data.parameters.iter().any(|t| {
                    t == ret_ty
                        || matches!(t, SuiMoveNormalizedType::Reference(r) if r.as_ref() == ret_ty)
                        || matches!(t, SuiMoveNormalizedType::MutableReference(r) if r.as_ref() == ret_ty)
                });
                let ret_ref = matches!(
                    ret_ty,
                    SuiMoveNormalizedType::Reference(_)
                        | SuiMoveNormalizedType::MutableReference(_)
                );
                let hanging_hot_potato = ret_ty.is_hot_potato(self)
                    && consuming_graph
                        .iter()
                        .all(|(ty, _)| ret_ty.partial_extract_ty_args(ty).is_none());
                if self_used || ret_ref || hanging_hot_potato {
                    remove_idxs.push(
                        self.target_functions
                            .iter()
                            .position(|f| f == &func_ident)
                            .unwrap(),
                    );
                    break;
                }
                let SuiMoveNormalizedType::Struct { inner } = ret_ty else {
                    continue;
                };
                if AccountAddress::from_str(&inner.address).unwrap() == AccountAddress::TWO
                    || module_data.structs.keys().any(|s| s == &inner.name)
                {
                    producing_graph
                        .iter_mut()
                        .find(|(ty, _)| ty == ret_ty)
                        .map(|(_, funcs)| funcs.push((func_ident.clone(), i as u16)))
                        .unwrap_or_else(|| {
                            producing_graph
                                .push((ret_ty.clone(), vec![(func_ident.clone(), i as u16)]));
                        });
                }
                producing_graph_all
                    .iter_mut()
                    .find(|(ty, _)| ty == ret_ty)
                    .map(|(_, funcs)| funcs.push((func_ident.clone(), i as u16)))
                    .unwrap_or_else(|| {
                        producing_graph_all
                            .push((ret_ty.clone(), vec![(func_ident.clone(), i as u16)]));
                    });
            }
        }

        // merge producing graph to avoid isolated nodes
        for (ty, funcs) in producing_graph_all.into_iter() {
            if producing_graph.iter().all(|(t, _)| t != &ty) {
                producing_graph.push((ty, funcs));
            }
        }

        // Remove functions that cannot cause any state change
        remove_idxs.sort_unstable();
        remove_idxs.dedup();
        remove_idxs.retain(|&idx| {
            let func = &self.target_functions[idx];
            !specified_functions.contains(func)
        });
        if self.target_functions.len() <= remove_idxs.len() + specified_functions.len() {
            debug!("No target functions left, skip removing");
            remove_idxs.clear();
        }
        for idx in remove_idxs.into_iter().rev() {
            self.target_functions.remove(idx);
        }
        for f in specified_functions.into_iter() {
            self.target_functions.retain(|x| x != &f);
        }

        self.consuming_graph = consuming_graph;
        self.producing_graph = producing_graph;
        self.function_scores = function_scores;

        let mut ability_to_type_tag: BTreeMap<AbilitySet, Vec<TypeTag>> = BTreeMap::from([
            (
                AbilitySet::PRIMITIVES,
                vec![
                    TypeTag::Bool,
                    TypeTag::Address,
                    TypeTag::U8,
                    TypeTag::U16,
                    TypeTag::U32,
                    TypeTag::U64,
                    TypeTag::U128,
                    TypeTag::U256,
                ],
            ),
            (AbilitySet::SIGNER, vec![TypeTag::Signer]),
            (
                AbilitySet::VECTOR,
                vec![TypeTag::Vector(Box::new(TypeTag::U8))],
            ),
        ]);
        for (ability, tags) in self.ability_to_type_tag.iter() {
            ability_to_type_tag
                .entry(*ability)
                .or_default()
                .extend(tags.iter().cloned());
        }
        for (pname, pkg) in self.packages.iter() {
            for (mname, module) in pkg.abis.iter() {
                if pname == &STD_ADDR_VALUE.into()
                    || pname == &SUI_ADDR_VALUE.into()
                    || pname == &SUI_SYSTEM_ADDR_VALUE.into()
                    || pname == &BRIDGE_ADDR_VALUE.into()
                {
                    if mname != "sui" && mname != "vector" && mname != "string" {
                        continue; // skip std and sui package
                    }
                }
                for (sname, move_struct) in module.structs.iter() {
                    if move_struct.type_parameters.len() > 0 {
                        continue; // skip generic structs
                    }
                    let abilities = sui_move_ability_set_to_ability_set(&move_struct.abilities);
                    let type_tag = TypeTag::Struct(Box::new(StructTag {
                        address: AccountAddress::from_str(&module.address).unwrap(),
                        module: Identifier::new(mname.clone()).unwrap(),
                        name: Identifier::new(sname.clone()).unwrap(),
                        type_params: vec![],
                    }));
                    ability_to_type_tag
                        .entry(abilities)
                        .or_default()
                        .push(type_tag);
                }
            }
        }
        self.ability_to_type_tag = ability_to_type_tag;
        self.gas_id = Some(gas_id);
    }

    pub async fn add_single_package<T>(
        &mut self,
        rpc: &SuiClient,
        addr: ObjectID,
        db: &T,
    ) -> Result<(), BelobogError>
    where
        T: ObjectStoreCommit + ObjectStore,
    {
        if self.packages.contains_key(&addr) || self.module_address_to_package.contains_key(&addr) {
            return Ok(());
        }
        let (object, abis) = PackageMetadata::from_rpc(rpc, addr).await?;
        let module_address = abis.abis.iter().next().unwrap().1.address.clone();
        let module_address = ObjectID::from_str(&module_address).unwrap();
        if let Some(old_addr) = self.module_address_to_package.get(&module_address) {
            let new_pkg = object.data.try_as_package().unwrap();
            let Data::Package(old_pkg) = &db
                .get_object(old_addr)
                .unwrap_or_else(|| {
                    panic!("Old package {} not found in DB", old_addr);
                })
                .data
            else {
                return Err(BelobogError::Other(eyre!("Expected package data",)));
            };
            if new_pkg.version() > old_pkg.version() {
                debug!(
                    "Updating package {} from {} to {}",
                    module_address, old_addr, addr
                );
                self.module_address_to_package.insert(module_address, addr);
            }
        } else {
            self.module_address_to_package.insert(module_address, addr);
        }
        // db.commit_single_object(object.clone());
        self.packages.insert(addr, abis);
        Ok(())
    }

    pub async fn analyze_dependency<T>(
        &mut self,
        rpc: &SuiClient,
        package_object: &Object,
        db: &T,
    ) -> Result<(), BelobogError>
    where
        T: ObjectStoreCommit + ObjectStore,
    {
        let pkg = package_object
            .data
            .try_as_package()
            .ok_or_else(|| BelobogError::Other(eyre!("Expected package data",)))?;
        for upgrade_info in pkg.linkage_table().values() {
            self.add_single_package(rpc, upgrade_info.upgraded_id, db)
                .await?;
        }
        Ok(())
    }

    fn analyze_type(type_tag: &TypeTag) -> Vec<ObjectID> {
        match type_tag {
            TypeTag::Struct(ty) => {
                let mut sub_addrs = ty
                    .type_params
                    .iter()
                    .flat_map(Self::analyze_type)
                    .collect::<Vec<_>>();
                sub_addrs.push(ty.address.into());
                sub_addrs
            }
            TypeTag::Vector(inner) => Self::analyze_type(inner),
            _ => vec![],
        }
    }

    pub async fn analyze_object_types<T>(
        &mut self,
        rpc: &SuiClient,
        db: &T,
    ) -> Result<(), BelobogError>
    where
        T: ObjectStoreCommit + ObjectTypesStore + ObjectStore,
    {
        let tys = db.tys();
        for ty in tys {
            let addresses = Self::analyze_type(&ty.into());
            if addresses.is_empty() {
                continue;
            }
            for address in addresses {
                self.add_single_package(rpc, address, db).await?;
            }
        }
        Ok(())
    }

    pub fn get_package_metadata(&self, package_id: &ObjectID) -> Option<&PackageMetadata> {
        self.packages.get(
            self.module_address_to_package
                .get(package_id)
                .unwrap_or(package_id),
        )
    }

    pub fn get_function(
        &self,
        package_id: &ObjectID,
        module: &str,
        function: &str,
    ) -> Option<&SuiMoveNormalizedFunction> {
        self.get_package_metadata(package_id)
            .and_then(|pkg| pkg.abis.get(module))
            .and_then(|module| module.exposed_functions.get(function))
    }

    pub fn get_struct(
        &self,
        package_id: &ObjectID,
        module: &str,
        struct_name: &str,
    ) -> Option<&SuiMoveNormalizedStruct> {
        self.get_package_metadata(package_id)
            .and_then(|pkg| pkg.abis.get(module))
            .and_then(|module| module.structs.get(struct_name))
    }

    pub fn get_enum(
        &self,
        package_id: &ObjectID,
        module: &str,
        enum_name: &str,
    ) -> Option<&SuiMoveNormalizedEnum> {
        self.get_package_metadata(package_id)
            .and_then(|pkg| pkg.abis.get(module))
            .and_then(|module| module.enums.get(enum_name))
    }

    pub fn get_abilities(
        &self,
        package_id: &ObjectID,
        module: &str,
        struct_name: &str,
    ) -> Option<AbilitySet> {
        self.get_struct(package_id, module, struct_name)
            .map(|s| sui_move_ability_set_to_ability_set(&s.abilities))
            .or(self
                .get_enum(package_id, module, struct_name)
                .map(|e| sui_move_ability_set_to_ability_set(&e.abilities)))
    }

    pub fn generate_magic_number_pool(&self) -> BTreeSet<Vec<u8>> {
        // for magic in self
        //     .packages
        //     .get(&self.target_functions[0].0)
        //     .unwrap()
        //     .magics
        //     .values()
        //     .flat_map(|inner| inner.values())
        //     .flatten()
        //     .cloned()
        //     .collect::<Vec<Magic>>()
        // {
        //     let bytes = match magic {
        //         Magic::U8(v) => vec![v],
        //         Magic::U16(v) => v.to_le_bytes().to_vec(),
        //         Magic::U32(v) => v.to_le_bytes().to_vec(),
        //         Magic::U64(v) => v.to_le_bytes().to_vec(),
        //         Magic::U128(v) => v.to_le_bytes().to_vec(),
        //         Magic::U256(v) => v.to_le_bytes().to_vec(),
        //         Magic::Bytes(v) => v,
        //     };
        //     magic_number_pool.insert(bytes);
        // }
        BTreeSet::new()
    }
}
