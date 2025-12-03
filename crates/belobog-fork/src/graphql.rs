use std::{collections::HashMap, ops::Deref, str::FromStr, sync::Arc};

use belobog_types::error::BelobogError;
use color_eyre::eyre::{Context, eyre};
use cynic::{GraphQlResponse, Operation, QueryBuilder};
use fastcrypto::encoding::Encoding;
use log::{debug, trace, warn};
use reqwest::header::USER_AGENT;
use serde::de::DeserializeOwned;
use sui_types::{
    base_types::ObjectID,
    digests::TransactionDigest,
    effects::TransactionEffects,
    error::SuiError,
    messages_checkpoint::{CheckpointContents, CheckpointSummary},
    object::Object,
    storage::{BackingPackageStore, PackageObject, ParentSync},
    transaction::TransactionData,
};
use tokio::sync::Semaphore;

#[cynic::schema("sui")]
mod schema {
    use chrono::{DateTime as ChronoDateTime, Utc};
    cynic::impl_scalar!(u64, UInt53);
    cynic::impl_scalar!(ChronoDateTime<Utc>, DateTime);
}

// query QueryTransactions($digests: [String!]!) {
//   multiGetTransactions(keys: $digests) {
//     transactionBcs
//   }
// }

// query QueryTransactions($digests: [String!]!) {
//   multiGetTransactions(keys: $digests) {
//     transactionBcs
//     effects {
//       checkpoint {
//         sequenceNumber
//       }
//       effectsBcs
//     }
//   }
// }

mod txns_query {
    use super::*;
    #[derive(cynic::QueryVariables, Debug)]
    pub struct QueryTransactionsVariables {
        pub digests: Vec<String>,
    }

    #[derive(cynic::QueryFragment, Debug)]
    #[cynic(graphql_type = "Query", variables = "QueryTransactionsVariables")]
    pub struct QueryTransactions {
        #[arguments(keys: $digests)]
        pub multi_get_transactions: Vec<Option<Transaction>>,
    }

    #[derive(cynic::QueryFragment, Debug)]
    pub struct Transaction {
        pub transaction_bcs: Option<Base64>,
        pub effects: Option<TransactionEffects>,
    }

    #[derive(cynic::QueryFragment, Debug)]
    pub struct TransactionEffects {
        pub checkpoint: Option<Checkpoint>,
        pub effects_bcs: Option<Base64>,
    }

    #[derive(cynic::QueryFragment, Debug)]
    pub struct Checkpoint {
        pub sequence_number: u64,
    }

    #[derive(cynic::Scalar, Debug, Clone)]
    pub struct Base64(pub String);

    #[derive(cynic::Scalar, Debug, Clone)]
    #[cynic(graphql_type = "UInt53")]
    pub struct Uint53(pub String);
}

// query Objects($keys: [ObjectKey!]!) {
//   multiGetObjects(keys: $keys) {
//     objectBcs
//   }
// }

mod objects_query {
    use super::*;
    #[derive(cynic::QueryVariables, Debug)]
    pub struct ObjectsVariables {
        pub keys: Vec<ObjectKey>,
    }

    #[derive(cynic::QueryFragment, Debug)]
    #[cynic(graphql_type = "Query", variables = "ObjectsVariables")]
    pub struct Objects {
        #[arguments(keys: $keys)]
        pub multi_get_objects: Vec<Option<Object>>,
    }

    #[derive(cynic::QueryFragment, Debug)]
    pub struct Object {
        pub object_bcs: Option<Base64>,
    }

    #[derive(cynic::InputObject, Debug, Clone)]
    pub struct ObjectKey {
        pub address: SuiAddress,
        pub version: Option<u64>,
        pub root_version: Option<u64>,
        pub at_checkpoint: Option<u64>,
    }

    impl ObjectKey {
        pub fn at_checkpoint(obj: ObjectID, ckpt: u64) -> Self {
            Self {
                address: SuiAddress(obj.to_canonical_string(true)),
                version: None,
                root_version: None,
                at_checkpoint: Some(ckpt),
            }
        }

        pub fn latest(obj: ObjectID) -> Self {
            Self {
                address: SuiAddress(obj.to_canonical_string(true)),
                version: None,
                root_version: None,
                at_checkpoint: None,
            }
        }

        pub fn at_version(obj: ObjectID, version: u64) -> Self {
            Self {
                address: SuiAddress(obj.to_canonical_string(true)),
                version: Some(version),
                root_version: None,
                at_checkpoint: None,
            }
        }

        pub fn at_root_version(obj: ObjectID, root_version: u64) -> Self {
            Self {
                address: SuiAddress(obj.to_canonical_string(true)),
                version: None,
                root_version: Some(root_version),
                at_checkpoint: None,
            }
        }
    }

    #[derive(cynic::Scalar, Debug, Clone)]
    pub struct Base64(pub String);

    #[derive(cynic::Scalar, Debug, Clone)]
    pub struct SuiAddress(pub String);

    #[derive(cynic::Scalar, Debug, Clone)]
    #[cynic(graphql_type = "UInt53")]
    pub struct Uint53(pub String);
}

// query EpochesQuery($keys: [UInt53!]!) {
//   multiGetEpochs(keys: $keys) {
//     referenceGasPrice
//     startTimestamp
//     epochId
//     protocolConfigs {
//       protocolVersion
//     }
//   }
// }
mod epoches_query {
    use super::*;
    #[derive(cynic::QueryVariables, Debug)]
    pub struct EpochesQueryVariables {
        pub keys: Vec<u64>,
    }

    #[derive(cynic::QueryFragment, Debug)]
    #[cynic(graphql_type = "Query", variables = "EpochesQueryVariables")]
    pub struct EpochesQuery {
        #[arguments(keys: $keys)]
        pub multi_get_epochs: Vec<Option<Epoch>>,
    }

    #[derive(cynic::QueryFragment, Debug)]
    pub struct Epoch {
        pub reference_gas_price: Option<BigInt>,
        pub start_timestamp: Option<DateTime>,
        pub epoch_id: u64,
        pub protocol_configs: Option<ProtocolConfigs>,
    }

    #[derive(cynic::QueryFragment, Debug)]
    pub struct ProtocolConfigs {
        pub protocol_version: u64,
    }

    #[derive(cynic::Scalar, Debug, Clone)]
    pub struct BigInt(pub String);

    #[derive(cynic::Scalar, Debug, Clone)]
    pub struct DateTime(pub String);

    #[derive(cynic::Scalar, Debug, Clone)]
    #[cynic(graphql_type = "UInt53")]
    pub struct Uint53(pub String);
}

// query ListDynamicFields($first: Int = 10, $after: String = "", $address: SuiAddress!, $checkpoint: UInt53!) {
//   address(address: $address) {
//     dynamicFields(first: $first, after: $after) {
//       nodes {
//         address
//         objectAt(checkpoint: $checkpoint) {
//           version
//           digest
//           defaultSuinsName
//           objectBcs
//           storageRebate
//         }
//       }
//       pageInfo {
//         hasNextPage
//         endCursor
//       }
//     }
//   }
// }
mod dynamic_fields_query {
    use super::*;
    #[derive(cynic::QueryVariables, Debug)]
    pub struct ListDynamicFieldsVariables {
        pub address: SuiAddress,
        pub after: Option<String>,
        pub checkpoint: Uint53,
        pub first: Option<i32>,
    }

    #[derive(cynic::QueryFragment, Debug)]
    #[cynic(graphql_type = "Query", variables = "ListDynamicFieldsVariables")]
    pub struct ListDynamicFields {
        #[arguments(address: $address)]
        pub address: Address,
    }

    #[derive(cynic::QueryFragment, Debug)]
    #[cynic(variables = "ListDynamicFieldsVariables")]
    pub struct Address {
        #[arguments(first: $first, after: $after)]
        pub dynamic_fields: Option<DynamicFieldConnection>,
    }

    #[derive(cynic::QueryFragment, Debug)]
    #[cynic(variables = "ListDynamicFieldsVariables")]
    pub struct DynamicFieldConnection {
        pub nodes: Vec<DynamicField>,
        pub page_info: PageInfo,
    }

    #[derive(cynic::QueryFragment, Debug)]
    pub struct PageInfo {
        pub has_next_page: bool,
        pub end_cursor: Option<String>,
    }

    #[derive(cynic::QueryFragment, Debug)]
    #[cynic(variables = "ListDynamicFieldsVariables")]
    pub struct DynamicField {
        pub address: SuiAddress,
        #[arguments(checkpoint: $checkpoint)]
        pub object_at: Option<Object>,
    }

    #[derive(cynic::QueryFragment, Debug)]
    pub struct Object {
        pub version: Option<Uint53>,
        pub digest: Option<String>,
        pub default_suins_name: Option<String>,
        pub object_bcs: Option<Base64>,
        pub storage_rebate: Option<BigInt>,
    }

    #[derive(cynic::Scalar, Debug, Clone)]
    pub struct Base64(pub String);

    #[derive(cynic::Scalar, Debug, Clone)]
    pub struct BigInt(pub String);

    #[derive(cynic::Scalar, Debug, Clone)]
    pub struct SuiAddress(pub String);

    #[derive(cynic::Scalar, Debug, Clone)]
    #[cynic(graphql_type = "UInt53")]
    pub struct Uint53(pub u64);
}

// query OwnedObjectsAtCheckpoint($first: Int = 10, $after: String = "", $checkpoint: UInt53!, $owner: SuiAddress!, $type: String) {
//   address(address: $owner) {
//     objects(after: $after, first: $first, filter: {type: $type}) {
//       pageInfo {
//         endCursor
//         hasNextPage
//       }
//       nodes {
//         address
//         objectAt(checkpoint: $checkpoint) {
//           version
//           digest
//           defaultSuinsName
//           objectBcs
//           storageRebate
//         }
//       }
//     }
//   }
// }
mod owned_objects {
    use super::*;
    #[derive(cynic::QueryVariables, Debug)]
    pub struct OwnedObjectsAtCheckpointVariables {
        pub after: Option<String>,
        pub checkpoint: Uint53,
        pub first: Option<i32>,
        pub owner: SuiAddress,
        #[cynic(rename = "type")]
        pub type_: Option<String>,
    }

    #[derive(cynic::QueryFragment, Debug)]
    #[cynic(
        graphql_type = "Query",
        variables = "OwnedObjectsAtCheckpointVariables"
    )]
    pub struct OwnedObjectsAtCheckpoint {
        #[arguments(address: $owner)]
        pub address: Address,
    }

    #[derive(cynic::QueryFragment, Debug)]
    #[cynic(variables = "OwnedObjectsAtCheckpointVariables")]
    pub struct Address {
        #[arguments(after: $after, first: $first, filter: { type: $type_ })]
        pub objects: Option<MoveObjectConnection>,
    }

    #[derive(cynic::QueryFragment, Debug)]
    #[cynic(variables = "OwnedObjectsAtCheckpointVariables")]
    pub struct MoveObjectConnection {
        pub page_info: PageInfo,
        pub nodes: Vec<MoveObject>,
    }

    #[derive(cynic::QueryFragment, Debug)]
    #[cynic(variables = "OwnedObjectsAtCheckpointVariables")]
    pub struct MoveObject {
        pub address: SuiAddress,
        #[arguments(checkpoint: $checkpoint)]
        pub object_at: Option<Object>,
    }

    #[derive(cynic::QueryFragment, Debug)]
    pub struct Object {
        pub version: Option<Uint53>,
        pub digest: Option<String>,
        pub default_suins_name: Option<String>,
        pub object_bcs: Option<Base64>,
        pub storage_rebate: Option<BigInt>,
    }

    #[derive(cynic::QueryFragment, Debug)]
    pub struct PageInfo {
        pub end_cursor: Option<String>,
        pub has_next_page: bool,
    }

    #[derive(cynic::Scalar, Debug, Clone)]
    pub struct Base64(pub String);

    #[derive(cynic::Scalar, Debug, Clone)]
    pub struct BigInt(pub String);

    #[derive(cynic::Scalar, Debug, Clone)]
    pub struct SuiAddress(pub String);

    #[derive(cynic::Scalar, Debug, Clone)]
    #[cynic(graphql_type = "UInt53")]
    pub struct Uint53(pub u64);
}

// query CheckpointsQuery($sequenceNumber: UInt53 = "") {
//   checkpoint(sequenceNumber: $sequenceNumber) {
//     contentBcs
//     summaryBcs
//     sequenceNumber
//   }
// }

mod checkpoints_query {
    use super::*;
    #[derive(cynic::QueryVariables, Debug)]
    pub struct CheckpointsQueryVariables {
        pub sequence_number: Option<u64>,
    }

    #[derive(cynic::QueryFragment, Debug)]
    #[cynic(graphql_type = "Query", variables = "CheckpointsQueryVariables")]
    pub struct CheckpointsQuery {
        #[arguments(sequenceNumber: $sequence_number)]
        pub checkpoint: Option<Checkpoint>,
    }

    #[derive(cynic::QueryFragment, Debug)]
    pub struct Checkpoint {
        pub content_bcs: Option<Base64>,
        pub summary_bcs: Option<Base64>,
        pub sequence_number: u64,
    }

    #[derive(cynic::Scalar, Debug, Clone)]
    pub struct Base64(pub String);

    #[derive(cynic::Scalar, Debug, Clone)]
    #[cynic(graphql_type = "UInt53")]
    pub struct Uint53(pub u64);
}

const ASSUMED_GRAPHQL_CONCURRENT: usize = 3;

#[derive(Debug, Clone)]
pub struct TransactionGraphQlResponse {
    pub tx: TransactionData,
    pub effects: TransactionEffects,
    pub checkpoint: u64,
}

#[derive(Debug, Clone)]
pub struct EpochData {
    pub epoch: u64,
    pub rgp: u64,
    pub protocol_version: u64,
    pub start_timestamp: chrono::DateTime<chrono::Utc>,
}

fn base64_to_object<T: DeserializeOwned>(b64: &str) -> Result<T, BelobogError> {
    Ok(bcs::from_bytes(
        &fastcrypto::encoding::Base64::decode(b64).map_err(|e| eyre!("b64: {}", e))?,
    )?)
}

#[derive(Debug, Clone)]
pub struct GraphQlClient {
    pub inner: Arc<GraphQlClientInner>,
}

impl Deref for GraphQlClient {
    type Target = GraphQlClientInner;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl GraphQlClient {
    pub fn new(client: reqwest::Client, url: reqwest::Url, concurrent: usize) -> Self {
        Self {
            inner: Arc::new(GraphQlClientInner::new(client, url, concurrent)),
        }
    }

    pub fn new_mystens() -> Self {
        Self::new(
            reqwest::Client::new(),
            reqwest::Url::parse("https://graphql.mainnet.sui.io/graphql").unwrap(),
            ASSUMED_GRAPHQL_CONCURRENT,
        )
    }
}

#[derive(Debug)]
pub struct GraphQlClientInner {
    pub client: reqwest::Client,
    pub url: reqwest::Url,
    pub permit: Semaphore,
}

impl GraphQlClientInner {
    pub fn new(client: reqwest::Client, url: reqwest::Url, concurrent_limit: usize) -> Self {
        Self {
            client,
            url,
            permit: Semaphore::new(concurrent_limit),
        }
    }

    pub fn new_mystens() -> Self {
        Self::new(
            reqwest::Client::new(),
            reqwest::Url::parse("https://graphql.mainnet.sui.io/graphql").unwrap(),
            ASSUMED_GRAPHQL_CONCURRENT,
        )
    }

    pub(crate) async fn run_query<T, V>(
        &self,
        operation: &Operation<T, V>,
    ) -> Result<GraphQlResponse<T>, BelobogError>
    where
        T: serde::de::DeserializeOwned,
        V: serde::Serialize,
    {
        let _permit = self.permit.acquire().await.expect("acquire permit");
        let mut resp_json: Option<GraphQlResponse<T>> = None;

        for idx in 0..10 {
            let resp = self
                .client
                .post(self.url.clone())
                .header(USER_AGENT, "sui-replay-v2")
                .json(&operation)
                .send()
                .await
                .map_err(|e| eyre!("Failed to send GQL query: {}", e))?;

            let resp_bytes = resp.bytes().await?;
            match serde_json::from_slice(&resp_bytes) {
                Ok(v) => {
                    resp_json = Some(v);
                    break;
                }
                Err(e) => {
                    warn!(
                        "Failed to read response in GQL query: {}, resp: {:?}, sleep {} seconds",
                        e,
                        String::from_utf8_lossy(&resp_bytes),
                        idx
                    );
                    tokio::time::sleep(std::time::Duration::from_secs(idx)).await;
                    let backoff = rand::random_range(0..1000);
                    debug!("backup {} ms", backoff);
                    tokio::time::sleep(std::time::Duration::from_millis(backoff)).await;
                    continue;
                }
            }
        }

        if let Some(resp_json) = resp_json {
            if resp_json.errors.is_some() {
                return Err(eyre!("graphql error: {:?}", &resp_json.errors.unwrap()).into());
            }
            Ok(resp_json)
        } else {
            return Err(eyre!("can not get graphql response...").into());
        }
    }

    pub async fn query_checkpoint(
        &self,
        checkpoint: Option<u64>,
    ) -> Result<Option<(CheckpointContents, CheckpointSummary)>, BelobogError> {
        let query = checkpoints_query::CheckpointsQuery::build(
            checkpoints_query::CheckpointsQueryVariables {
                sequence_number: checkpoint,
            },
        );
        let response = self
            .run_query(&query)
            .await
            .map_err(|e| eyre!("Failed to run transaction query: {}", e))?;

        let checkpoint_resp = response.data.map(|v| v.checkpoint).flatten();
        if checkpoint_resp.is_none() {
            return Ok(None);
        }

        let checkpoint_resp = checkpoint_resp.unwrap();
        let content = base64_to_object(
            &checkpoint_resp
                .content_bcs
                .ok_or_else(|| eyre!("no content for {:?}", checkpoint))?
                .0,
        )?;
        let summary: CheckpointSummary = base64_to_object(
            &checkpoint_resp
                .summary_bcs
                .ok_or_else(|| eyre!("no content for {:?}", checkpoint))?
                .0,
        )?;
        Ok(Some((content, summary)))
    }

    pub async fn owned_objects_at_checkpoint(
        &self,
        checkpoint: u64,
        owner: String,
        ty: Option<String>,
    ) -> Result<Vec<Object>, BelobogError> {
        let mut out = vec![];

        let mut after = None;
        loop {
            let query = owned_objects::OwnedObjectsAtCheckpoint::build(
                owned_objects::OwnedObjectsAtCheckpointVariables {
                    owner: owned_objects::SuiAddress(owner.clone()),
                    after: after,
                    checkpoint: owned_objects::Uint53(checkpoint),
                    first: Some(16),
                    type_: ty.clone(),
                },
            );

            let response = self
                .run_query(&query)
                .await
                .map_err(|e| eyre!("Failed to run transaction query: {}", e))?;

            let objects = response
                .data
                .map(|v| v.address.objects)
                .flatten()
                .ok_or_else(|| eyre!("no fields from request"))?;

            for object in objects.nodes {
                if let Some(at) = &object.object_at {
                    let obj: Object = base64_to_object(
                        &at.object_bcs
                            .as_ref()
                            .ok_or_else(|| eyre!("no object bcs from {:?}", &at))?
                            .0,
                    )?;
                    out.push(obj);
                } else {
                    log::debug!(
                        "We have an object {} but not object at checkpoint {}",
                        &object.address.0,
                        checkpoint
                    );
                }
            }

            if objects.page_info.has_next_page {
                after = Some(
                    objects
                        .page_info
                        .end_cursor
                        .ok_or_else(|| eyre!("has next but not cursor?!"))?,
                );
            } else {
                break;
            }
        }

        Ok(out)
    }

    pub async fn dynamic_fields_at_checkpoint(
        &self,
        checkpoint: u64,
        owner: String,
    ) -> Result<Vec<Object>, BelobogError> {
        let mut out = vec![];

        let mut after = None;
        loop {
            let query = dynamic_fields_query::ListDynamicFields::build(
                dynamic_fields_query::ListDynamicFieldsVariables {
                    address: dynamic_fields_query::SuiAddress(owner.clone()),
                    after: after,
                    checkpoint: dynamic_fields_query::Uint53(checkpoint),
                    first: Some(16),
                },
            );

            let response = self
                .run_query(&query)
                .await
                .map_err(|e| eyre!("Failed to run transaction query: {}", e))?;

            let fields = response
                .data
                .map(|v| v.address.dynamic_fields)
                .flatten()
                .ok_or_else(|| eyre!("no fields from request"))?;

            for field in fields.nodes {
                if let Some(at) = &field.object_at {
                    let obj: Object = base64_to_object(
                        &at.object_bcs
                            .as_ref()
                            .ok_or_else(|| eyre!("no object bcs from {:?}", &at))?
                            .0,
                    )?;
                    out.push(obj);
                } else {
                    log::debug!(
                        "We have a field {} but not object at checkpoint {}",
                        &field.address.0,
                        checkpoint
                    );
                }
            }

            if fields.page_info.has_next_page {
                after = Some(
                    fields
                        .page_info
                        .end_cursor
                        .ok_or_else(|| eyre!("has next but not cursor?!"))?,
                );
            } else {
                break;
            }
        }

        Ok(out)
    }

    pub async fn query_transactions(
        &self,
        digests: Vec<String>,
    ) -> Result<Vec<TransactionGraphQlResponse>, BelobogError> {
        let query = txns_query::QueryTransactions::build(txns_query::QueryTransactionsVariables {
            digests: digests.clone(),
        });
        let response = self
            .run_query(&query)
            .await
            .map_err(|e| eyre!("Failed to run transaction query: {}", e))?;

        let transactions = response
            .data
            .map(|txn| txn.multi_get_transactions)
            .ok_or_else(|| eyre!(format!("Transaction not found for digest: {:?}", digests),))?;

        let mut mp = vec![];
        for (idx, tx_resp) in transactions.into_iter().enumerate() {
            if let Some(tx_resp) = tx_resp {
                let tx_data: TransactionData = base64_to_object(
                    &tx_resp
                        .transaction_bcs
                        .ok_or_else(|| eyre!("no tx data {:?}", digests))?
                        .0,
                )?;
                let effects = tx_resp
                    .effects
                    .ok_or_else(|| eyre!("no effects {:?}", digests))?;
                let tx_effects: TransactionEffects = base64_to_object(
                    &effects
                        .effects_bcs
                        .ok_or_else(|| eyre!("no effects bcs {:?}", digests))?
                        .0,
                )?;
                let tx_checkpoint = effects
                    .checkpoint
                    .ok_or_else(|| eyre!("no tx ckpt {:?}", digests))?
                    .sequence_number;
                mp.push(TransactionGraphQlResponse {
                    tx: tx_data,
                    effects: tx_effects,
                    checkpoint: tx_checkpoint,
                });
            } else {
                log::debug!("Got a none in tx resp, probably {:?}", digests.get(idx));
            }
        }

        Ok(mp)
    }

    pub async fn query_objects(
        &self,
        keys: Vec<objects_query::ObjectKey>,
    ) -> Result<Vec<Object>, BelobogError> {
        let query =
            objects_query::Objects::build(objects_query::ObjectsVariables { keys: keys.clone() });
        let response = self
            .run_query(&query)
            .await
            .map_err(|e| eyre!("Failed to run objects query: {}", e))?;
        let objects = response
            .data
            .map(|txn| txn.multi_get_objects)
            .ok_or_else(|| eyre!(format!("objects not found for digest: {:?}", keys)))?;

        let mut out = vec![];
        for (idx, obj) in objects.into_iter().enumerate() {
            if let Some(obj) = obj {
                let object: Object = base64_to_object(
                    &obj.object_bcs
                        .ok_or_else(|| eyre!("no object bcs: {:?}", keys))?
                        .0,
                )?;
                out.push(object);
            } else {
                log::debug!("Got a none in tx resp, probably {:?}", keys.get(idx));
            }
        }

        Ok(out)
    }

    pub async fn query_epoches(&self, keys: Vec<u64>) -> Result<Vec<EpochData>, BelobogError> {
        let query = epoches_query::EpochesQuery::build(epoches_query::EpochesQueryVariables {
            keys: keys.clone(),
        });
        let response = self
            .run_query(&query)
            .await
            .map_err(|e| eyre!("Failed to run objects query: {}", e))?;
        let epoches = response
            .data
            .map(|txn| txn.multi_get_epochs)
            .ok_or_else(|| eyre!(format!("objects not found for digest: {:?}", keys)))?;

        let mut out = vec![];
        for (idx, ep) in epoches.into_iter().enumerate() {
            if let Some(ep) = ep {
                let epoch_id = ep.epoch_id;
                let start_ts = chrono::DateTime::from_str(
                    &ep.start_timestamp
                        .ok_or_else(|| eyre!("ep {} not ts", epoch_id))?
                        .0,
                )
                .map_err(|e| eyre!("can not parse start timestamp: {}", e))?;
                let protocol_version = ep
                    .protocol_configs
                    .ok_or_else(|| eyre!("no version for {}", epoch_id))?
                    .protocol_version;
                let rgp = u64::from_str(
                    &ep.reference_gas_price
                        .ok_or_else(|| eyre!("no rgp for {}", epoch_id))?
                        .0,
                )
                .map_err(|e| eyre!("can not parse rgp: {}", e))?;
                let data = EpochData {
                    epoch: epoch_id,
                    rgp: rgp,
                    protocol_version: protocol_version,
                    start_timestamp: start_ts,
                };
                out.push(data);
            } else {
                log::debug!("Got a none in tx resp, probably {:?}", keys.get(idx));
            }
        }

        Ok(out)
    }
}

#[derive(Debug, Clone)]
pub struct GraphQlDatabase {
    pub graphql: GraphQlClient,
    pub fork: u64, // inclusive
}

impl GraphQlDatabase {
    pub fn new(client: reqwest::Client, url: reqwest::Url, fork: u64) -> Self {
        Self {
            graphql: GraphQlClient::new(client, url, 3),
            fork,
        }
    }

    pub fn new_mystens(fork: u64) -> Self {
        Self {
            graphql: GraphQlClient::new_mystens(),
            fork,
        }
    }

    pub async fn transaction(
        &self,
        digest: &str,
    ) -> Result<Option<TransactionGraphQlResponse>, BelobogError> {
        debug!("Query transaction {}", digest);
        let mut data = self
            .graphql
            .query_transactions(vec![digest.to_string()])
            .await?;
        debug!("Query transaction succeeds");
        Ok(data.pop())
    }

    pub async fn epoch(&self, epoch_id: u64) -> Result<Option<EpochData>, BelobogError> {
        debug!("Start epoch query for {}", epoch_id);
        let mut data = self.graphql.query_epoches(vec![epoch_id]).await?;
        debug!("Epoch query succeeds");
        Ok(data.pop())
    }

    pub async fn objects(
        &self,
        keys: Vec<objects_query::ObjectKey>,
    ) -> Result<Vec<Object>, BelobogError> {
        debug!("Object query: {:?}", keys);
        let data = self.graphql.query_objects(keys).await?;
        for t in &data {
            trace!(
                "Object result is: {}",
                format!("{}:{} -> {}", t.id(), t.version(), t.digest())
            )
        }
        debug!("Object query succeeds");
        Ok(data)
    }

    pub async fn sinlge_object(
        &self,
        k: objects_query::ObjectKey,
    ) -> Result<Option<Object>, BelobogError> {
        let mut v = self.objects(vec![k.clone()]).await?;

        if v.is_empty() {
            debug!("Get {:?} but no results", &k);
            Ok(None)
        } else {
            let object = v.remove(0);
            debug!("Get {:?} -> {}", &k, object.version());
            Ok(Some(object))
        }
    }

    pub async fn get_object_at_checkpoint(
        &self,
        object: ObjectID,
        checkpoint: u64,
    ) -> Result<Option<Object>, BelobogError> {
        self.sinlge_object(objects_query::ObjectKey::at_checkpoint(object, checkpoint))
            .await
    }
}

impl sui_types::storage::ChildObjectResolver for GraphQlDatabase {
    fn read_child_object(
        &self,
        parent: &sui_types::base_types::ObjectID,
        child: &sui_types::base_types::ObjectID,
        child_version_upper_bound: sui_types::base_types::SequenceNumber,
    ) -> sui_types::error::SuiResult<Option<Object>> {
        debug!(
            "[ChildObjectResolver] read_child_object({}, {}, {})",
            parent, child, child_version_upper_bound,
        );
        let object_key =
            objects_query::ObjectKey::at_root_version(*child, child_version_upper_bound.into());
        let object = tokio::task::block_in_place(move || {
            tokio::runtime::Handle::current()
                .block_on(async move { self.sinlge_object(object_key).await })
        })
        .map_err(|e| SuiError::Storage(e.to_string()))?;
        debug!(
            "[ChildObjectResolver]> read_child_object({}, {}, {}) -> {:?}",
            parent,
            child,
            child_version_upper_bound,
            object
                .as_ref()
                .map(|t| format!("{}:{} -> {}", t.id(), t.version(), t.digest()))
        );
        Ok(object)
    }

    fn get_object_received_at_version(
        &self,
        owner: &sui_types::base_types::ObjectID,
        receiving_object_id: &sui_types::base_types::ObjectID,
        receive_object_at_version: sui_types::base_types::SequenceNumber,
        epoch_id: sui_types::committee::EpochId,
    ) -> sui_types::error::SuiResult<Option<Object>> {
        debug!(
            "[ChildObjectResolver] get_object_received_at_version({}, {}, {}, {})",
            owner, receiving_object_id, receive_object_at_version, epoch_id
        );
        let object_key = objects_query::ObjectKey::at_version(
            *receiving_object_id,
            receive_object_at_version.into(),
        );
        let object = tokio::task::block_in_place(move || {
            tokio::runtime::Handle::current()
                .block_on(async move { self.sinlge_object(object_key).await })
        })
        .map_err(|e| SuiError::Storage(e.to_string()))?;
        debug!(
            "[ChildObjectResolver]> get_object_received_at_version({}:{}) {:?}",
            receiving_object_id,
            receive_object_at_version,
            object
                .as_ref()
                .map(|t| format!("{}:{} -> {}", t.id(), t.version(), t.digest()))
        );
        Ok(object)
    }
}

impl sui_types::storage::ObjectStore for GraphQlDatabase {
    fn get_object(&self, object_id: &sui_types::base_types::ObjectID) -> Option<Object> {
        debug!("[ObjectStore] get_object id={}", object_id);
        match tokio::task::block_in_place(move || {
            tokio::runtime::Handle::current()
                .block_on(async move { self.get_object_at_checkpoint(*object_id, self.fork).await })
        }) {
            Ok(object) => {
                debug!(
                    "[ObjectStore] get_object {} -> {:?}",
                    object_id,
                    object.as_ref().map(|t| t.version())
                );
                object
            }
            Err(e) => {
                warn!("Fail to get object {} due to {}", object_id, e);
                None
            }
        }
    }

    fn get_object_by_key(
        &self,
        object_id: &sui_types::base_types::ObjectID,
        version: sui_types::base_types::VersionNumber,
    ) -> Option<Object> {
        debug!(
            "[ObjectStore] get_object_by_key id={} version={}",
            object_id, version
        );
        let object_key = objects_query::ObjectKey::at_version(*object_id, version.into());
        match tokio::task::block_in_place(move || {
            tokio::runtime::Handle::current()
                .block_on(async move { self.sinlge_object(object_key).await })
        }) {
            Ok(object) => {
                debug!(
                    "[ObjectStore] get_object_by_key {}:{} -> {:?}",
                    object_id,
                    version,
                    object.as_ref().map(|t| t.version())
                );
                object
            }
            Err(e) => {
                warn!("Fail to get object {}:{} due to {}", object_id, version, e);
                None
            }
        }
    }
}

impl ParentSync for GraphQlDatabase {
    fn get_latest_parent_entry_ref_deprecated(
        &self,
        object_id: sui_types::base_types::ObjectID,
    ) -> Option<sui_types::base_types::ObjectRef> {
        unreachable!(
            "unexpected ParentSync::get_latest_parent_entry_ref_deprecated({})",
            object_id,
        )
    }
}

impl BackingPackageStore for GraphQlDatabase {
    fn get_package_object(
        &self,
        package_id: &ObjectID,
    ) -> sui_types::error::SuiResult<Option<sui_types::storage::PackageObject>> {
        debug!("[BackingPackageStore] get_package_object({})", package_id);
        let package = tokio::task::block_in_place(move || {
            tokio::runtime::Handle::current().block_on(async move {
                self.get_object_at_checkpoint(*package_id, self.fork).await
            })
        })
        .map_err(|e| SuiError::Storage(e.to_string()))?;
        if let Some(package) = package {
            debug!(
                "[BackingPackageStore]> get_package_object {}:{} -> {}",
                package.id(),
                package.version(),
                package.digest()
            );
            Ok(Some(PackageObject::new(package)))
        } else {
            debug!(
                "[BackingPackageStore]> get_package_object {} -> None",
                package_id
            );
            Ok(None)
        }
    }
}
