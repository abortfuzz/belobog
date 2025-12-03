use std::{
    collections::{BTreeSet, HashMap},
    sync::{Arc, Mutex},
};

use itertools::Itertools;
use log::debug;
use sui_sdk::types::{
    base_types::{MoveObjectType, ObjectID, SuiAddress},
    effects::{TransactionEffects, TransactionEffectsAPI},
    object::Object,
    storage::{BackingPackageStore, ChildObjectResolver, ObjectStore, PackageObject, ParentSync},
};

use crate::ObjectStoreCommit;

#[derive(Debug, Default, Clone)]
pub struct CachedInner {
    pub objects: HashMap<ObjectID, HashMap<u64, Object>>,
    pub object_version_cache: HashMap<ObjectID, u64>, // checkpoint sensitive!
    pub resolvers: HashMap<ObjectID, HashMap<u64, u64>>,
}

impl CachedInner {
    pub fn object_version_cached(&self, object: &ObjectID, version: u64) -> Option<Object> {
        self.objects
            .get(object)
            .and_then(|t| t.get(&version).cloned())
    }

    pub fn object_id_cached(&self, object: &ObjectID) -> Option<Object> {
        self.object_version_cache.get(object).map(|d| {
            self.object_version_cached(object, *d)
                .expect("inconsistency")
        })
    }

    pub fn object_resolved(&self, object: &ObjectID, upbound: u64) -> Option<Object> {
        self.resolvers
            .get(object)
            .and_then(|t| t.get(&upbound))
            .and_then(|t| self.object_version_cached(object, *t))
            .or(self.object_version_cached(object, upbound))
    }

    pub fn cache_object(&mut self, object: Object) {
        let object_id = object.id();
        self.object_version_cache
            .insert(object_id, object.version().into());
        self.objects
            .entry(object_id)
            .or_default()
            .insert(object.version().into(), object);
    }

    pub fn cache_object_only(&mut self, object: Object) {
        let object_id = object.id();
        self.objects
            .entry(object_id)
            .or_default()
            .insert(object.version().into(), object);
    }

    pub fn cache_resolved(&mut self, upbound: u64, object: Object) {
        let object_id = object.id();
        self.resolvers
            .entry(object_id)
            .or_default()
            .insert(upbound, object.version().into());
        self.cache_object_only(object);
        // DO NOT UPDATE VERSION MAPPING
    }
}

#[derive(Clone, Debug)]
pub struct CachedStore<T> {
    pub inner: Arc<Mutex<CachedInner>>,
    pub store: T,
}

impl<T> CachedStore<T> {
    pub fn new(db: T) -> Self {
        Self {
            inner: Arc::new(Mutex::new(CachedInner::default())),
            store: db,
        }
    }

    pub fn to_delta(&self) -> CachedInner {
        let lock = self.inner.lock().expect("fail to lock");
        lock.clone()
    }
}

impl<T: BackingPackageStore> BackingPackageStore for CachedStore<T> {
    fn get_package_object(
        &self,
        package_id: &ObjectID,
    ) -> sui_sdk::types::error::SuiResult<Option<sui_sdk::types::storage::PackageObject>> {
        let hit = {
            let guard = self.inner.lock().expect("fail to lock?!");
            guard.object_id_cached(package_id)
        };

        if let Some(hit) = hit {
            debug!(
                "[CachedStore] get_package_object hit for {}:{}",
                package_id,
                hit.version()
            );
            Ok(Some(PackageObject::new(hit)))
        } else {
            debug!("[CachedStore] get_package_object miss for {}", package_id);
            let hit = self.store.get_package_object(package_id)?;
            if let Some(hit) = &hit {
                let mut guard = self.inner.lock().expect("fail to lock");
                guard.cache_object(hit.object().clone());
            }
            Ok(hit)
        }
    }
}

impl<T: ObjectStore> ObjectStore for CachedStore<T> {
    fn get_object(&self, object_id: &ObjectID) -> Option<Object> {
        let hit = {
            let guard = self.inner.lock().expect("fail to lock?!");
            guard.object_id_cached(object_id)
        };

        if let Some(hit) = hit {
            debug!(
                "[CachedStore] get_object hit for {}:{}",
                object_id,
                hit.version()
            );
            Some(hit)
        } else {
            debug!("[CachedStore] get_object miss for {}", object_id);
            let hit = self.store.get_object(object_id)?;
            self.inner
                .lock()
                .expect("fail to lock")
                .cache_object(hit.clone());
            Some(hit)
        }
    }
    fn get_object_by_key(
        &self,
        object_id: &ObjectID,
        version: sui_sdk::types::base_types::VersionNumber,
    ) -> Option<Object> {
        let hit = {
            let guard = self.inner.lock().expect("fail to lock?!");
            guard.object_version_cached(object_id, version.into())
        };

        if let Some(hit) = hit {
            debug!(
                "[CachedStore] get_object_by_key hit for {}:{}",
                object_id, version
            );
            Some(hit)
        } else {
            debug!(
                "[CachedStore] get_object_by_key miss for {}:{}",
                object_id, version
            );
            let hit = self.store.get_object_by_key(object_id, version)?;
            self.inner
                .lock()
                .expect("fail to lock")
                .cache_object_only(hit.clone());
            Some(hit)
        }
    }
}

impl<T: ParentSync> ParentSync for CachedStore<T> {
    fn get_latest_parent_entry_ref_deprecated(
        &self,
        object_id: ObjectID,
    ) -> Option<sui_sdk::types::base_types::ObjectRef> {
        self.store.get_latest_parent_entry_ref_deprecated(object_id)
    }
}

impl<T: ChildObjectResolver> ChildObjectResolver for CachedStore<T> {
    fn get_object_received_at_version(
        &self,
        owner: &ObjectID,
        receiving_object_id: &ObjectID,
        receive_object_at_version: sui_sdk::types::base_types::SequenceNumber,
        epoch_id: sui_sdk::types::committee::EpochId,
    ) -> sui_sdk::types::error::SuiResult<Option<Object>> {
        let hit = {
            let guard = self.inner.lock().expect("fail to lock?!");
            guard.object_version_cached(receiving_object_id, receive_object_at_version.into())
        };

        if let Some(hit) = hit {
            debug!(
                "[CachedStore] get_object_received_at_version hit for {}:{}",
                receiving_object_id, receive_object_at_version
            );
            Ok(Some(hit))
        } else {
            debug!(
                "[CachedStore] get_object_received_at_version miss for {}:{}",
                receiving_object_id, receive_object_at_version
            );
            let hit = self.store.get_object_received_at_version(
                owner,
                receiving_object_id,
                receive_object_at_version,
                epoch_id,
            )?;
            if let Some(hit) = hit {
                self.inner
                    .lock()
                    .expect("fail to lock")
                    .cache_object_only(hit.clone());
                Ok(Some(hit))
            } else {
                Ok(None)
            }
        }
    }

    fn read_child_object(
        &self,
        parent: &ObjectID,
        child: &ObjectID,
        child_version_upper_bound: sui_sdk::types::base_types::SequenceNumber,
    ) -> sui_sdk::types::error::SuiResult<Option<Object>> {
        let (hit, vs) = {
            let guard = self.inner.lock().expect("fail to lock?!");
            let object = guard.object_resolved(child, child_version_upper_bound.into());
            let vs = guard
                .resolvers
                .get(child)
                .map(|v| v.keys().copied().collect_vec())
                .unwrap_or_default();
            (object, vs)
        };

        if let Some(hit) = hit {
            debug!(
                "[CachedStore] read_child_object hit for {}:{}, digest {}",
                child,
                child_version_upper_bound,
                hit.digest()
            );
            Ok(Some(hit))
        } else {
            debug!(
                "[CachedStore] read_child_object miss for {}:{}, though we know [{}]",
                child,
                child_version_upper_bound,
                vs.into_iter().map(|t| t.to_string()).join(", ")
            );
            let hit = self
                .store
                .read_child_object(parent, child, child_version_upper_bound)?;
            if let Some(hit) = hit {
                self.inner
                    .lock()
                    .expect("fail to lock")
                    .cache_resolved(child_version_upper_bound.into(), hit.clone());
                Ok(Some(hit))
            } else {
                Ok(None)
            }
        }
    }
}

impl<T> ObjectStoreCommit for CachedStore<T> {
    fn commit_single_object(&self, object: Object) {
        let mut guard = self.inner.lock().expect("fail to lock");
        let id = object.id();
        let version = object.version();
        debug!("[CachedStore] Commit a single object {}:{}", id, version);
        guard.object_version_cache.insert(id, version.into());
        guard.cache_object(object);
    }
    fn commit_store(
        &self,
        mut store: sui_sdk::types::inner_temporary_store::InnerTemporaryStore,
        effects: &TransactionEffects,
    ) {
        let mut guard = self.inner.lock().expect("fail to lock");
        // Checkpoint cache is safe to keep
        guard.resolvers = HashMap::default(); // drop all resolvers cache, too hard to maintain

        for (id, object) in store.written {
            debug!("[CachedStore] Committing {}:{}", id, object.version());
            guard
                .object_version_cache
                .entry(id)
                .or_insert(object.version().into());
            guard.cache_object(object);
        }

        for (id, version) in effects
            .deleted()
            .into_iter()
            .chain(effects.transferred_from_consensus())
            .chain(effects.consensus_owner_changed())
            .map(|oref| (oref.0, oref.1))
            .filter_map(|(id, version)| store.input_objects.remove(&id).map(|_| (id, version)))
        {
            debug!(
                "[CachedStore] Removing deleted/transferred consensus objects {}:{}",
                id, version
            );
            guard.object_version_cache.remove(&id);
            guard.objects.entry(id).or_default().remove(&version.into());
        }

        let smeared_version = store.lamport_version;
        let deleted_accessed_objects = effects.stream_ended_mutably_accessed_consensus_objects();
        for object_id in deleted_accessed_objects.into_iter() {
            let (id, _) = store
                .input_objects
                .get(&object_id)
                .map(|obj| (obj.id(), obj.version()))
                .unwrap_or_else(|| {
                    let start_version = store.stream_ended_consensus_objects.get(&object_id)
                        .expect("stream-ended object must be in either input_objects or stream_ended_consensus_objects");
                    ( (*object_id).into(), *start_version)
                });
            debug!(
                "[CachedStore] Removing accessed consensus objects {}:{}",
                id, smeared_version
            );
            guard.object_version_cache.remove(&id);
            guard
                .objects
                .entry(id)
                .or_default()
                .remove(&smeared_version.into());
        }

        // Optionally prune history objects?
    }
}

impl<T: crate::ObjectTypesStore> crate::ObjectTypesStore for CachedStore<T> {
    fn get_mutable_objects_by_ty(
        &self,
        ty: &MoveObjectType,
        owner: SuiAddress,
    ) -> Vec<(ObjectID, u64)> {
        self.store.get_mutable_objects_by_ty(ty, owner)
    }

    fn get_objects_by_ty(&self, ty: &MoveObjectType, owner: SuiAddress) -> Vec<(ObjectID, u64)> {
        self.store.get_objects_by_ty(ty, owner)
    }

    fn get_objects_by_ty_anyone(&self, ty: &MoveObjectType) -> Vec<(ObjectID, u64)> {
        self.store.get_objects_by_ty_anyone(ty)
    }

    fn get_owned_objects_by_ty(
        &self,
        ty: &MoveObjectType,
        owner: SuiAddress,
    ) -> Vec<(ObjectID, u64)> {
        self.store.get_owned_objects_by_ty(ty, owner)
    }

    fn ty_cache(&self) -> HashMap<MoveObjectType, BTreeSet<(ObjectID, u64)>> {
        self.store.ty_cache()
    }
    fn tys(&self) -> Vec<MoveObjectType> {
        self.store.tys()
    }
}
