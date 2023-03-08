use dozer_core::node::PortHandle;
use dozer_types::types::{Record, Schema};

use crate::pipeline::errors::JoinError;

use super::{join_operator::JoinOperator, join_table::JoinTable};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum JoinAction {
    Insert,
    Delete,
}

#[derive(Clone, Debug)]
pub struct LookupKey(pub u64);

#[derive(Clone, Debug)]
pub struct CompositeLookupKey {
    pub left: Option<Box<JoinLookupKey>>,
    pub right: Option<Box<JoinLookupKey>>,
}

#[derive(Clone, Debug)]
pub enum JoinLookupKey {
    Lookup(LookupKey),
    Composite(CompositeLookupKey),
}

#[derive(Clone, Debug)]
pub enum JoinSource {
    Table(JoinTable),
    Join(JoinOperator),
}

impl JoinSource {
    pub fn execute(
        &mut self,
        action: JoinAction,
        from_port: PortHandle,
        record: &Record,
    ) -> Result<Vec<(JoinAction, Record, Box<JoinLookupKey>)>, JoinError> {
        match self {
            JoinSource::Table(table) => table.execute(action, from_port, record),
            JoinSource::Join(join) => join.execute(action, from_port, record),
        }
    }

    pub fn lookup(&self, lookup_key: Box<JoinLookupKey>) -> Result<Vec<Record>, JoinError> {
        match self {
            JoinSource::Table(table) => table.lookup(lookup_key),
            JoinSource::Join(join) => join.lookup(lookup_key),
        }
    }

    pub fn get_output_schema(&self) -> Schema {
        match self {
            JoinSource::Table(table) => table.schema.clone(),
            JoinSource::Join(join) => join.schema.clone(),
        }
    }

    pub fn get_sources(&self) -> Vec<PortHandle> {
        match self {
            JoinSource::Table(table) => vec![table.get_source()],
            JoinSource::Join(join) => join.get_sources(),
        }
    }
}
