use belobog_types::error::BelobogError;
use color_eyre::eyre::eyre;
use log::{debug, warn};
use mdbx_derive::{
    HasMDBXEnvironment, MDBXDatabase, MDBXTable, ZstdBcsObject, ZstdJSONObject,
    mdbx::{
        Environment, EnvironmentFlags, Geometry, Mode, PageSize, RW, SyncMode, TransactionAny,
        TransactionKind, WriteFlags,
    },
};
use serde::{Deserialize, Serialize};
use sui_sdk::types::{
    base_types::ObjectID,
    error::SuiError,
    object::Object,
    storage::{BackingPackageStore, ChildObjectResolver, ObjectStore, PackageObject, ParentSync},
};

use crate::schema::{ObjectIDKey, ObjectIDVersionedKey};

pub struct ObjectTable;
pub struct ObjectVersionTable;
pub struct ObjectResolverTable;

#[derive(Debug, Serialize, Deserialize, ZstdJSONObject)]
pub struct DatabaseMetadata {
    pub checkpoint: u64, // inclusive, same from fuzz cli
}

#[derive(Debug, Serialize, Deserialize, ZstdBcsObject)]
pub struct PlainObjectValue {
    pub object: Object,
}

#[derive(Debug, Serialize, Deserialize, ZstdBcsObject)]
pub struct PlainVersion {
    pub version: u64,
}

mdbx_derive::mdbx_table!(
    ObjectTable,
    ObjectIDVersionedKey,
    PlainObjectValue,
    BelobogError
);
mdbx_derive::mdbx_table!(ObjectVersionTable, ObjectIDKey, PlainVersion, BelobogError);
mdbx_derive::mdbx_table!(
    ObjectResolverTable,
    ObjectIDVersionedKey,
    PlainVersion,
    BelobogError
); // reuse

mdbx_derive::mdbx_database!(
    ObjectCacheDatabase,
    BelobogError,
    DatabaseMetadata,
    ObjectTable,
    ObjectVersionTable,
    ObjectResolverTable
);

#[derive(Debug, Clone)]
pub struct MDBXCachedStore<T> {
    pub env: ObjectCacheDatabase,
    pub ro: bool,
    pub store: T,
}

impl<T> MDBXCachedStore<T> {
    pub async fn new(
        db: &str,
        store: T,
        fork_checkpoint: u64,
        ro: bool,
    ) -> Result<Self, BelobogError> {
        let mut defaults = Environment::builder();
        defaults
            .set_flags(EnvironmentFlags {
                mode: if ro {
                    Mode::ReadOnly
                } else {
                    Mode::ReadWrite {
                        sync_mode: SyncMode::default(),
                    }
                },
                ..Default::default()
            })
            .set_geometry(Geometry {
                size: Some(0usize..1024 * 1024 * 1024 * 128), // max 128G
                growth_step: Some(64 * 1024 * 1024),          // 64 MB
                shrink_threshold: None,
                page_size: Some(PageSize::Set(16384)),
            })
            .set_max_dbs(256)
            .set_max_readers(256);
        let env = if ro {
            ObjectCacheDatabase::open_tables_with_defaults(db, defaults).await?
        } else {
            ObjectCacheDatabase::open_create_tables_with_defaults(db, defaults).await?
        };

        if let Some(meta) = env.metadata().await? {
            if meta.checkpoint != fork_checkpoint {
                return Err(eyre!(
                    "cache is intended for {:?} but you want to fork {}",
                    &meta,
                    fork_checkpoint
                )
                .into());
            }
        } else {
            env.write_metadata(&DatabaseMetadata {
                checkpoint: fork_checkpoint,
            })
            .await?;
        }

        Ok(Self { env, ro, store })
    }

    pub async fn may_cache_object_only(
        &self,
        tx: &TransactionAny<RW>,
        object: Object,
    ) -> Result<(), BelobogError> {
        if !self.ro {
            debug!("[MDBXCachedStore] cache object {}", object.id());
            ObjectTable::put_item_tx(
                tx,
                Some(self.env.dbis.object_table),
                &ObjectIDVersionedKey {
                    id: object.id().into(),
                    version: object.version().into(),
                },
                &PlainObjectValue { object },
                WriteFlags::default(),
            )
            .await?;
        }
        Ok(())
    }

    pub async fn cache_object_and_version(
        &self,
        tx: &TransactionAny<RW>,
        object: Object,
    ) -> Result<(), BelobogError> {
        if !self.ro {
            debug!(
                "[MDBXCachedStore] cache object with version mapping {}",
                object.id()
            );
            ObjectVersionTable::put_item_tx(
                tx,
                Some(self.env.dbis.object_version_table),
                &object.id().into(),
                &PlainVersion {
                    version: object.version().into(),
                },
                WriteFlags::default(),
            )
            .await?;
            self.may_cache_object_only(tx, object).await?;
        }
        Ok(())
    }

    pub async fn cache_resolver(
        &self,
        tx: &TransactionAny<RW>,
        object_id: ObjectID,
        upper: u64,
        version: u64,
    ) -> Result<(), BelobogError> {
        if !self.ro {
            debug!(
                "[MDBXCachedStore] cache resolver {}:{} -> {}",
                &object_id, upper, version
            );
            ObjectResolverTable::put_item_tx(
                tx,
                Some(self.env.dbis.object_resolver_table),
                &ObjectIDVersionedKey {
                    id: object_id.into(),
                    version: upper,
                },
                &PlainVersion { version },
                WriteFlags::default(),
            )
            .await?;
        }
        Ok(())
    }

    pub async fn get_object_version_upperbound<K: TransactionKind>(
        &self,
        tx: &TransactionAny<K>,
        object_id: ObjectID,
        upper: u64,
    ) -> Result<Option<Object>, BelobogError> {
        let out = ObjectResolverTable::get_item_tx(
            tx,
            Some(self.env.dbis.object_resolver_table),
            &ObjectIDVersionedKey {
                id: object_id.into(),
                version: upper,
            },
        )
        .await?;

        if let Some(out) = out {
            self.get_object_full(tx, object_id, out.version).await
        } else {
            Ok(None)
        }
    }

    pub async fn get_object_full<K: TransactionKind>(
        &self,
        tx: &TransactionAny<K>,
        object_id: ObjectID,
        version: u64,
    ) -> Result<Option<Object>, BelobogError> {
        let out = ObjectTable::get_item_tx(
            tx,
            Some(self.env.dbis.object_table),
            &ObjectIDVersionedKey {
                id: object_id.into(),
                version,
            },
        )
        .await?;

        Ok(out.map(|t| t.object))
    }

    pub async fn get_object_version<K: TransactionKind>(
        &self,
        tx: &TransactionAny<K>,
        object_id: ObjectID,
    ) -> Result<Option<u64>, BelobogError> {
        let out = ObjectVersionTable::get_item_tx(
            tx,
            Some(self.env.dbis.object_version_table),
            &object_id.into(),
        )
        .await?;
        Ok(out.map(|v| v.version))
    }

    pub async fn get_object_by_id<K: TransactionKind>(
        &self,
        tx: &TransactionAny<K>,
        object_id: ObjectID,
    ) -> Result<Option<Object>, BelobogError> {
        if let Some(version) = self.get_object_version(tx, object_id).await? {
            self.get_object_full(tx, object_id, version).await
        } else {
            Ok(None)
        }
    }

    fn get_object_by_id_sync(&self, object_id: ObjectID) -> Result<Option<Object>, BelobogError> {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let tx = self.env.begin_ro_txn().await?;
                self.get_object_by_id(&tx, object_id).await
            })
        })
    }

    fn get_object_by_id_version_sync(
        &self,
        object_id: ObjectID,
        version: u64,
    ) -> Result<Option<Object>, BelobogError> {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let tx = self.env.begin_ro_txn().await?;
                self.get_object_full(&tx, object_id, version).await
            })
        })
    }

    fn cache_object_sync(&self, object: Object) -> Result<(), BelobogError> {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let tx = self.env.begin_rw_txn().await?;
                self.cache_object_and_version(&tx, object).await?;
                tx.commit().await?;
                Ok(())
            })
        })
    }

    fn cache_resolver_sync(&self, object: Object, upper: u64) -> Result<(), BelobogError> {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let tx = self.env.begin_rw_txn().await?;
                let id = object.id();
                let version = object.version();
                self.may_cache_object_only(&tx, object).await?;
                // DO NOT UPDATE VERSION MAPPING
                self.cache_resolver(&tx, id, upper, version.into()).await?;
                tx.commit().await?;
                Ok(())
            })
        })
    }

    fn get_object_version_upperbound_sync(
        &self,
        object_id: ObjectID,
        upper: u64,
    ) -> Result<Option<Object>, BelobogError> {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let tx = self.env.begin_ro_txn().await?;
                self.get_object_version_upperbound(&tx, object_id, upper)
                    .await
            })
        })
    }

    fn cache_object_only_sync(&self, object: Object) -> Result<(), BelobogError> {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let tx = self.env.begin_rw_txn().await?;
                self.may_cache_object_only(&tx, object).await?;
                tx.commit().await?;
                Ok(())
            })
        })
    }
}

impl<T: BackingPackageStore> BackingPackageStore for MDBXCachedStore<T> {
    fn get_package_object(
        &self,
        package_id: &ObjectID,
    ) -> sui_sdk::types::error::SuiResult<Option<sui_sdk::types::storage::PackageObject>> {
        if let Some(hit) = self
            .get_object_by_id_sync(*package_id)
            .map_err(|e| SuiError::Storage(e.to_string()))?
        {
            debug!("[MDBXCachedStore] package hit for {}", package_id);
            return Ok(Some(PackageObject::new(hit)));
        } else {
            debug!("[MDBXCachedStore] package miss for {}", package_id);
        }
        let package = self.store.get_package_object(package_id)?;
        if let Some(pkg) = &package {
            self.cache_object_sync(pkg.object().clone())
                .map_err(|e| SuiError::Storage(e.to_string()))?;
        }
        Ok(package)
    }
}

impl<T: ObjectStore> ObjectStore for MDBXCachedStore<T> {
    fn get_object(&self, object_id: &ObjectID) -> Option<Object> {
        let hit = match self.get_object_by_id_sync(*object_id) {
            Ok(v) => v,
            Err(e) => {
                warn!("Fail to get_object due to {}", e);
                None
            }
        };
        if let Some(hit) = hit {
            debug!("[MDBXCachedStore] get_object hit for {}", object_id);
            return Some(hit);
        } else {
            debug!("[MDBXCachedStore] get_object miss for {}", object_id);
        }

        let object = self.store.get_object(object_id);
        if let Some(object) = &object
            && let Err(e) = self.cache_object_sync(object.clone())
        {
            warn!("Fail to cache object due to {}", e);
        }

        object
    }

    fn get_object_by_key(
        &self,
        object_id: &ObjectID,
        version: sui_sdk::types::base_types::VersionNumber,
    ) -> Option<Object> {
        let hit = match self.get_object_by_id_version_sync(*object_id, version.into()) {
            Ok(v) => v,
            Err(e) => {
                warn!("Fail to get_object_by_key due to {}", e);
                None
            }
        };
        if let Some(hit) = hit {
            debug!("[MDBXCachedStore] get_object hit for {}", object_id);
            return Some(hit);
        } else {
            debug!("[MDBXCachedStore] get_object miss for {}", object_id);
        }

        let object = self.store.get_object_by_key(object_id, version);
        if let Some(object) = &object
            && let Err(e) = self.cache_object_only_sync(object.clone())
        {
            warn!("Fail to cache object due to {}", e);
        }

        object
    }
}

impl<T: ParentSync> ParentSync for MDBXCachedStore<T> {
    fn get_latest_parent_entry_ref_deprecated(
        &self,
        object_id: ObjectID,
    ) -> Option<sui_sdk::types::base_types::ObjectRef> {
        self.store.get_latest_parent_entry_ref_deprecated(object_id)
    }
}

impl<T: ChildObjectResolver> ChildObjectResolver for MDBXCachedStore<T> {
    fn get_object_received_at_version(
        &self,
        owner: &ObjectID,
        receiving_object_id: &ObjectID,
        receive_object_at_version: sui_sdk::types::base_types::SequenceNumber,
        epoch_id: sui_sdk::types::committee::EpochId,
    ) -> sui_sdk::types::error::SuiResult<Option<Object>> {
        let hit = self
            .get_object_by_id_version_sync(*receiving_object_id, receive_object_at_version.into())
            .map_err(|e| SuiError::Storage(e.to_string()))?;

        if let Some(hit) = hit {
            debug!(
                "[MDBXCachedStore] get_object_received_at_version hit for {}:{}",
                receiving_object_id, receive_object_at_version
            );
            Ok(Some(hit))
        } else {
            debug!(
                "[MDBXCachedStore] get_object_received_at_version miss for {}:{}",
                receiving_object_id, receive_object_at_version
            );
            let hit = self.store.get_object_received_at_version(
                owner,
                receiving_object_id,
                receive_object_at_version,
                epoch_id,
            )?;
            if let Some(hit) = hit {
                self.cache_object_only_sync(hit.clone())
                    .map_err(|e| SuiError::Storage(e.to_string()))?;
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
        let hit = self
            .get_object_version_upperbound_sync(*child, child_version_upper_bound.into())
            .map_err(|e| SuiError::Storage(e.to_string()))?;

        if let Some(hit) = hit {
            debug!(
                "[MDBXCachedStore] read_child_object hit for {}:{} -> {}, digest {}",
                child,
                child_version_upper_bound,
                hit.version(),
                hit.digest()
            );
            Ok(Some(hit))
        } else {
            debug!(
                "[MDBXCachedStore] read_child_object miss for {}:{}",
                child, child_version_upper_bound
            );
            let hit = self
                .store
                .read_child_object(parent, child, child_version_upper_bound)?;
            if let Some(hit) = hit {
                self.cache_resolver_sync(hit.clone(), child_version_upper_bound.into())
                    .map_err(|e| SuiError::Storage(e.to_string()))?;
                Ok(Some(hit))
            } else {
                Ok(None)
            }
        }
    }
}
