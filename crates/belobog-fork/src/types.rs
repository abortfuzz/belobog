use std::{
    collections::{BTreeSet, HashMap},
    sync::Arc,
};

use itertools::Itertools;
use log::debug;
use sui_sdk::types::{
    base_types::{MoveObjectType, ObjectID, SuiAddress},
    effects::TransactionEffectsAPI,
    object::{Object, Owner},
    storage::{BackingPackageStore, ChildObjectResolver, ObjectStore, ParentSync},
};
use tokio::sync::RwLock;

#[derive(Debug, Clone, Default)]
pub struct TypeCachedStoreInner {
    pub objects_type_cache: HashMap<MoveObjectType, BTreeSet<(ObjectID, u64)>>,
}

#[derive(Debug, Clone)]
pub struct TypeCachedStore<T> {
    pub store: T,
    pub inner: Arc<RwLock<TypeCachedStoreInner>>,
}

impl<T> TypeCachedStore<T> {
    pub fn new(store: T) -> Self {
        Self {
            store,
            inner: Arc::new(RwLock::new(TypeCachedStoreInner::default())),
        }
    }

    fn may_cache(&self, object: &Object) {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                if object.is_package() {
                    return;
                }
                let object_id = object.id();
                let version: u64 = object.version().into();
                let tp = (object_id, version);
                let do_cache = {
                    if let Some(ty) = object.type_() {
                        !self
                            .inner
                            .read()
                            .await
                            .objects_type_cache
                            .get(ty)
                            .map(|v| v.contains(&tp))
                            .unwrap_or_default()
                    } else {
                        false
                    }
                };

                if do_cache {
                    self.inner
                        .write()
                        .await
                        .objects_type_cache
                        .entry(object.type_().unwrap().clone())
                        .or_default()
                        .insert(tp);
                }
            })
        })
    }
}

impl<T: BackingPackageStore> BackingPackageStore for TypeCachedStore<T> {
    fn get_package_object(
        &self,
        package_id: &ObjectID,
    ) -> sui_sdk::types::error::SuiResult<Option<sui_sdk::types::storage::PackageObject>> {
        let package = self.store.get_package_object(package_id)?;
        if let Some(package) = &package {
            self.may_cache(package.object());
        }
        Ok(package)
    }
}

impl<T: ObjectStore> ObjectStore for TypeCachedStore<T> {
    fn get_object(&self, object_id: &ObjectID) -> Option<Object> {
        let object = self.store.get_object(object_id)?;
        self.may_cache(&object);
        Some(object)
    }
    fn get_object_by_key(
        &self,
        object_id: &ObjectID,
        version: sui_sdk::types::base_types::VersionNumber,
    ) -> Option<Object> {
        let object = self.store.get_object_by_key(object_id, version)?;
        self.may_cache(&object);
        Some(object)
    }
}

impl<T: ParentSync> ParentSync for TypeCachedStore<T> {
    fn get_latest_parent_entry_ref_deprecated(
        &self,
        object_id: ObjectID,
    ) -> Option<sui_sdk::types::base_types::ObjectRef> {
        self.store.get_latest_parent_entry_ref_deprecated(object_id)
    }
}

impl<T: ChildObjectResolver> ChildObjectResolver for TypeCachedStore<T> {
    fn get_object_received_at_version(
        &self,
        owner: &ObjectID,
        receiving_object_id: &ObjectID,
        receive_object_at_version: sui_sdk::types::base_types::SequenceNumber,
        epoch_id: sui_sdk::types::committee::EpochId,
    ) -> sui_sdk::types::error::SuiResult<Option<Object>> {
        let object = self.store.get_object_received_at_version(
            owner,
            receiving_object_id,
            receive_object_at_version,
            epoch_id,
        )?;
        if let Some(object) = &object {
            self.may_cache(object);
        }
        Ok(object)
    }

    fn read_child_object(
        &self,
        parent: &ObjectID,
        child: &ObjectID,
        child_version_upper_bound: sui_sdk::types::base_types::SequenceNumber,
    ) -> sui_sdk::types::error::SuiResult<Option<Object>> {
        let object = self
            .store
            .read_child_object(parent, child, child_version_upper_bound)?;
        if let Some(object) = &object {
            self.may_cache(object);
        }
        Ok(object)
    }
}

impl<T: crate::ObjectStoreCommit> crate::ObjectStoreCommit for TypeCachedStore<T> {
    fn commit_single_object(&self, object: Object) {
        self.may_cache(&object);
        self.store.commit_single_object(object);
    }

    fn commit_store(
        &self,
        store: sui_sdk::types::inner_temporary_store::InnerTemporaryStore,
        effects: &sui_sdk::types::effects::TransactionEffects,
    ) {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let mut inner = self.inner.write().await;
                for (id, object) in store.written.iter() {
                    if let Some(ty) = object.type_() {
                        inner
                            .objects_type_cache
                            .entry(ty.clone())
                            .or_default()
                            .insert((*id, object.version().into()));
                    }
                }
                for (id, version) in effects
                    .deleted()
                    .into_iter()
                    .chain(effects.transferred_from_consensus())
                    .chain(effects.consensus_owner_changed())
                    .map(|oref| (oref.0, oref.1))
                    .filter_map(|(id, version)| store.input_objects.get(&id).map(|_| (id, version)))
                {
                    inner.objects_type_cache.retain(|_key, set| {
                        set.remove(&(id, version.into()));
                        !set.is_empty()
                    });
                }
            })
        });
        self.store.commit_store(store, effects);
    }
}

impl<T: ObjectStore> crate::ObjectTypesStore for TypeCachedStore<T> {
    fn get_objects_by_ty_anyone(&self, ty: &MoveObjectType) -> Vec<(ObjectID, u64)> {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                self.inner
                    .read()
                    .await
                    .objects_type_cache
                    .get(ty)
                    .map(|t| t.clone().into_iter().collect_vec())
                    .unwrap_or_default()
            })
        })
    }

    fn get_objects_by_ty(&self, ty: &MoveObjectType, owner: SuiAddress) -> Vec<(ObjectID, u64)> {
        debug!("get_objects_by_ty: {:?}, owner: {:?}", ty, owner);
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let objects = self
                    .inner
                    .read()
                    .await
                    .objects_type_cache
                    .get(ty)
                    .map(|t| t.clone().into_iter().collect_vec())
                    .unwrap_or_default();
                debug!("all objects: {:?}", objects);
                objects
                    .iter()
                    .filter(|(id, version)| {
                        if let Owner::AddressOwner(addr) = self
                            .store
                            .get_object_by_key(id, (*version).into())
                            .unwrap()
                            .owner
                        {
                            addr == owner
                        } else {
                            true
                        }
                    })
                    .cloned()
                    .collect_vec()
            })
        })
    }

    fn get_mutable_objects_by_ty(
        &self,
        ty: &MoveObjectType,
        owner: SuiAddress,
    ) -> Vec<(ObjectID, u64)> {
        let default_objects = self.get_objects_by_ty(ty, owner);
        default_objects
            .iter()
            .filter(|(id, v)| {
                !self
                    .store
                    .get_object_by_key(id, (*v).into())
                    .unwrap()
                    .is_immutable()
            })
            .cloned()
            .collect_vec()
    }

    fn get_owned_objects_by_ty(
        &self,
        ty: &MoveObjectType,
        owner: SuiAddress,
    ) -> Vec<(ObjectID, u64)> {
        let default_objects = self.get_objects_by_ty(ty, owner);
        default_objects
            .iter()
            .filter(|(id, version)| {
                self.store
                    .get_object_by_key(id, (*version).into())
                    .unwrap()
                    .is_address_owned()
            })
            .cloned()
            .collect_vec()
    }

    fn tys(&self) -> Vec<MoveObjectType> {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                self.inner
                    .read()
                    .await
                    .objects_type_cache
                    .keys()
                    .cloned()
                    .collect()
            })
        })
    }

    fn ty_cache(&self) -> HashMap<MoveObjectType, BTreeSet<(ObjectID, u64)>> {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current()
                .block_on(async { self.inner.read().await.objects_type_cache.clone() })
        })
    }
}
