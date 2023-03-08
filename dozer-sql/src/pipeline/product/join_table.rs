use std::hash::{Hash, Hasher};

use ahash::AHasher;
use dozer_core::node::PortHandle;
use dozer_types::types::{Record, Schema};
use multimap::MultiMap;

use crate::pipeline::{
    errors::JoinError,
    product::join::{JoinLookupKey, LookupKey},
};

use super::join::JoinAction;

#[derive(Clone, Debug)]
pub struct JoinTable {
    port: PortHandle,

    pub(crate) schema: Schema,

    record_store: MultiMap<u64, Record>,
}

impl JoinTable {
    pub fn new(port: PortHandle, schema: Schema) -> Self {
        Self {
            port,
            schema,
            record_store: MultiMap::new(),
        }
    }

    pub fn get_source(&self) -> PortHandle {
        self.port
    }

    pub fn execute(
        &self,
        action: JoinAction,
        from_port: PortHandle,
        record: &Record,
    ) -> Result<Vec<(JoinAction, Record, Box<JoinLookupKey>)>, JoinError> {
        debug_assert!(self.port == from_port);

        let mut hasher = AHasher::default();
        record.values.hash(&mut hasher);
        let lookup_key = hasher.finish();

        match action {
            JoinAction::Insert => {
                self.record_store.insert(lookup_key, record.clone());
            }
            JoinAction::Delete => {
                self.record_store.remove(&lookup_key);
            }
        }

        Ok(vec![(
            action,
            record.clone(),
            Box::new(JoinLookupKey::Lookup(LookupKey(lookup_key))),
        )])
    }

    pub fn lookup(&self, join_lookup_key: Box<JoinLookupKey>) -> Result<Vec<Record>, JoinError> {
        let lookup_key = match *join_lookup_key {
            JoinLookupKey::Lookup(LookupKey(key)) => key,
            _ => return Err(JoinError::InvalidLookupKey(*join_lookup_key, self.port)),
        };

        let records = self
            .record_store
            .get_vec(&lookup_key)
            .ok_or(JoinError::HistoryRecordNotFound(lookup_key, self.port))?;
        Ok(*records)
    }
}
