use sui_sdk::types::storage::{BackingPackageStore, ChildObjectResolver, ObjectStore, ParentSync};

use crate::ObjectStoreExt;

#[derive(Debug, Clone, Copy, Default)]
pub struct EmptyStore;

impl ObjectStoreExt for EmptyStore {
    fn get_object_at_checkpoint(
        &self,
        object_id: sui_sdk::types::base_types::ObjectID,
        checkpoint: u64,
    ) -> Result<Option<sui_sdk::types::object::Object>, belobog_types::error::BelobogError> {
        Ok(None)
    }
}

impl ObjectStore for EmptyStore {
    fn get_object(
        &self,
        _object_id: &sui_sdk::types::base_types::ObjectID,
    ) -> Option<sui_sdk::types::object::Object> {
        None
    }

    fn get_object_by_key(
        &self,
        _object_id: &sui_sdk::types::base_types::ObjectID,
        _version: sui_sdk::types::base_types::VersionNumber,
    ) -> Option<sui_sdk::types::object::Object> {
        None
    }
}

impl ParentSync for EmptyStore {
    fn get_latest_parent_entry_ref_deprecated(
        &self,
        _object_id: sui_sdk::types::base_types::ObjectID,
    ) -> Option<sui_sdk::types::base_types::ObjectRef> {
        None
    }
}

impl BackingPackageStore for EmptyStore {
    fn get_package_object(
        &self,
        _package_id: &sui_sdk::types::base_types::ObjectID,
    ) -> sui_sdk::types::error::SuiResult<Option<sui_sdk::types::storage::PackageObject>> {
        Ok(None)
    }
}

impl ChildObjectResolver for EmptyStore {
    fn get_object_received_at_version(
        &self,
        _owner: &sui_sdk::types::base_types::ObjectID,
        _receiving_object_id: &sui_sdk::types::base_types::ObjectID,
        _receive_object_at_version: sui_sdk::types::base_types::SequenceNumber,
        _epoch_id: sui_sdk::types::committee::EpochId,
    ) -> sui_sdk::types::error::SuiResult<Option<sui_sdk::types::object::Object>> {
        Ok(None)
    }
    fn read_child_object(
        &self,
        _parent: &sui_sdk::types::base_types::ObjectID,
        _child: &sui_sdk::types::base_types::ObjectID,
        _child_version_upper_bound: sui_sdk::types::base_types::SequenceNumber,
    ) -> sui_sdk::types::error::SuiResult<Option<sui_sdk::types::object::Object>> {
        Ok(None)
    }
}
