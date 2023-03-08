use std::hash::{Hash, Hasher};

use ahash::AHasher;
use dozer_core::node::PortHandle;
use dozer_types::types::{Field, Record, Schema};

use multimap::MultiMap;

use crate::pipeline::errors::JoinError;

use super::join::{CompositeLookupKey, JoinAction, JoinLookupKey, JoinSource};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum JoinOperatorType {
    Inner,
    LeftOuter,
    RightOuter,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JoinConstraint {
    pub left_key_index: usize,
    pub right_key_index: usize,
}

#[derive(Clone, Debug)]
pub struct JoinOperator {
    operator: JoinOperatorType,

    left_join_key_indexes: Vec<usize>,
    right_join_key_indexes: Vec<usize>,

    pub(crate) schema: Schema,

    left_source: Box<JoinSource>,
    right_source: Box<JoinSource>,

    left_lookup_index_map: MultiMap<u64, Box<JoinLookupKey>>,
    right_lookup_index_map: MultiMap<u64, Box<JoinLookupKey>>,
}

pub struct JoinBranch {
    pub join_key_indexes: Vec<usize>,
    pub source: Box<JoinSource>,
    // lookup_index_map: MultiMap<Vec<Field>, Vec<Field>>,
}

impl JoinOperator {
    pub fn new(
        operator: JoinOperatorType,
        schema: Schema,
        left_join_branch: JoinBranch,
        right_join_branch: JoinBranch,
    ) -> Self {
        Self {
            operator,
            left_join_key_indexes: left_join_branch.join_key_indexes,
            right_join_key_indexes: right_join_branch.join_key_indexes,
            schema,
            left_source: left_join_branch.source,
            right_source: right_join_branch.source,
            left_lookup_index_map: MultiMap::new(),
            right_lookup_index_map: MultiMap::new(),
        }
    }

    pub fn get_sources(&self) -> Vec<PortHandle> {
        [
            self.left_source.get_sources().as_slice(),
            self.right_source.get_sources().as_slice(),
        ]
        .concat()
    }

    pub fn execute(
        &mut self,
        action: JoinAction,
        from_port: PortHandle,
        record: &Record,
    ) -> Result<Vec<(JoinAction, Record, Box<JoinLookupKey>)>, JoinError> {
        // if the source port is under the left branch of the join
        if self.left_source.get_sources().contains(&from_port) {
            let mut output_records = vec![];

            // forward the record and the current join constraints to the left source
            let mut left_records = self.left_source.execute(action, from_port, record)?;

            // update left join index
            for (join_action, left_record, left_lookup_key) in left_records.iter_mut() {
                // get the join key
                let left_join_key_fields =
                    left_record.get_fields_by_indexes(&self.left_join_key_indexes);
                let mut hasher = AHasher::default();
                left_join_key_fields.hash(&mut hasher);
                let left_join_key = hasher.finish();

                // save the entry to the left join index for future lookups
                self.update_left_index(join_action.clone(), left_join_key, *left_lookup_key);

                // perform the join
                let join_records = match self.operator {
                    JoinOperatorType::Inner => self.inner_join_left(
                        join_action,
                        left_join_key,
                        left_record,
                        left_lookup_key,
                    )?,
                    JoinOperatorType::LeftOuter => {
                        self.left_join(join_action, left_join_key, left_record, left_lookup_key)?
                    }
                    JoinOperatorType::RightOuter => self.right_join_reverse(
                        join_action,
                        left_join_key,
                        left_record,
                        left_lookup_key,
                    )?,
                };

                output_records.extend(join_records);
            }

            Ok(output_records)
        } else if self.right_source.get_sources().contains(&from_port) {
            let mut output_records = vec![];

            // forward the record and the current join constraints to the left source
            let mut right_records = self.right_source.execute(action, from_port, record)?;

            // update right join index
            for (join_action, right_record, right_lookup_key) in right_records.iter_mut() {
                let right_join_key_fields =
                    right_record.get_fields_by_indexes(&self.right_join_key_indexes);
                let mut hasher = AHasher::default();
                right_join_key_fields.hash(&mut hasher);
                let right_join_key = hasher.finish();

                self.update_right_index(join_action.clone(), right_join_key, *right_lookup_key);

                let join_records = match self.operator {
                    JoinOperatorType::Inner => self.inner_join_right(
                        join_action.clone(),
                        right_join_key,
                        right_record,
                        right_lookup_key,
                    )?,
                    JoinOperatorType::RightOuter => self.right_join(
                        join_action.clone(),
                        right_join_key,
                        right_record,
                        right_lookup_key,
                    )?,
                    JoinOperatorType::LeftOuter => self.left_join_reverse(
                        join_action.clone(),
                        right_join_key,
                        right_record,
                        right_lookup_key,
                    )?,
                };
                output_records.extend(join_records);
            }

            return Ok(output_records);
        } else {
            return Err(JoinError::InvalidSource(from_port));
        }
    }

    pub fn lookup(&self, lookup_key: Box<JoinLookupKey>) -> Result<Vec<Record>, JoinError> {
        let mut output_records = vec![];

        let (left_lookup_key, right_lookup_key) = self.split_join_lookup_key(lookup_key)?;

        let mut left_records = self.left_source.lookup(left_lookup_key)?;

        let mut right_records = self.right_source.lookup(right_lookup_key)?;

        for left_record in left_records.iter_mut() {
            for right_record in right_records.iter_mut() {
                let join_record = join_records(left_record, right_record);
                let join_lookup_key =
                    self.compose_join_lookup_key(left_lookup_key, right_lookup_key);

                output_records.push((join_record, join_lookup_key));
            }
        }

        Ok(output_records)
    }

    fn inner_join_left(
        &self,
        action: &JoinAction,
        left_join_key: u64,
        left_record: &mut Record,
        left_lookup_key: &mut Box<JoinLookupKey>,
    ) -> Result<Vec<(JoinAction, Record, Box<JoinLookupKey>)>, JoinError> {
        let right_lookup_keys = self
            .right_lookup_index_map
            .get_vec(&left_join_key)
            .unwrap_or(&vec![])
            .clone();

        let mut output_records = vec![];

        for right_lookup_key in right_lookup_keys.iter() {
            // lookup on the right branch to find matching records
            let mut right_records = self.right_source.lookup(*right_lookup_key)?;

            for right_record in right_records.iter_mut() {
                let join_record = join_records(left_record, right_record);
                let join_lookup_key =
                    self.compose_join_lookup_key(Some(*left_lookup_key), Some(*right_lookup_key));

                output_records.push((action.clone(), join_record, join_lookup_key));
            }
        }
        Ok(output_records)
    }

    fn inner_join_right(
        &self,
        action: JoinAction,
        right_join_key: u64,
        right_record: &mut Record,
        right_lookup_key: &mut Box<JoinLookupKey>,
    ) -> Result<Vec<(JoinAction, Record, Box<JoinLookupKey>)>, JoinError> {
        let left_lookup_keys = self
            .left_lookup_index_map
            .get_vec(&right_join_key)
            .unwrap_or(&vec![])
            .clone();

        let mut output_records = vec![];
        for left_lookup_key in left_lookup_keys.iter() {
            // lookup on the left branch to find matching records
            let mut left_records = self.left_source.lookup(*left_lookup_key)?;

            for left_record in left_records.iter_mut() {
                // join the records
                let join_record = join_records(left_record, right_record);
                let join_lookup_key =
                    self.compose_join_lookup_key(Some(*left_lookup_key), Some(*right_lookup_key));
                output_records.push((action.clone(), join_record, join_lookup_key));
            }
        }
        Ok(output_records)
    }

    fn left_join(
        &self,
        action: &JoinAction,
        left_join_key: u64,
        left_record: &mut Record,
        left_lookup_key: &mut Box<JoinLookupKey>,
    ) -> Result<Vec<(JoinAction, Record, Box<JoinLookupKey>)>, JoinError> {
        let right_lookup_keys = self
            .right_lookup_index_map
            .get_vec(&left_join_key)
            .unwrap_or(&vec![])
            .clone();

        let mut output_records = vec![];

        if right_lookup_keys.is_empty() {
            // no matching records on the right branch
            let right_record = Record::from_schema(&self.right_source.get_output_schema());
            let join_record = join_records(left_record, &right_record);
            let join_lookup_key = self.compose_join_lookup_key(Some(*left_lookup_key), None);
            output_records.push((*action, join_record, join_lookup_key));

            return Ok(output_records);
        }

        for right_lookup_key in right_lookup_keys.iter() {
            // lookup on the right branch to find matching records
            let mut right_records = self.right_source.lookup(*right_lookup_key)?;

            for right_record in right_records.iter_mut() {
                let join_record = join_records(left_record, right_record);
                let join_lookup_key =
                    self.compose_join_lookup_key(Some(*left_lookup_key), Some(*right_lookup_key));

                output_records.push((*action, join_record, join_lookup_key));
            }
        }
        Ok(output_records)
    }

    fn right_join(
        &self,
        action: JoinAction,
        right_join_key: u64,
        right_record: &mut Record,
        right_lookup_key: &mut Box<JoinLookupKey>,
    ) -> Result<Vec<(JoinAction, Record, Box<JoinLookupKey>)>, JoinError> {
        let left_lookup_keys = self
            .left_lookup_index_map
            .get_vec(&right_join_key)
            .unwrap_or(&vec![])
            .clone();

        let mut output_records = vec![];

        if left_lookup_keys.is_empty() {
            // no matching records on the right branch
            let left_record = Record::from_schema(&self.left_source.get_output_schema());
            let join_record = join_records(&left_record, right_record);
            let join_lookup_key = self.compose_join_lookup_key(None, Some(*right_lookup_key));
            output_records.push((action, join_record, join_lookup_key));

            return Ok(output_records);
        }

        for left_lookup_key in left_lookup_keys.iter() {
            // lookup on the left branch to find matching records
            let mut left_records = self.left_source.lookup(*left_lookup_key)?;

            for left_record in left_records.iter_mut() {
                // join the records
                let join_record = join_records(left_record, right_record);
                let join_lookup_key =
                    self.compose_join_lookup_key(Some(*left_lookup_key), Some(*right_lookup_key));
                output_records.push((action.clone(), join_record, join_lookup_key));
            }
        }
        Ok(output_records)
    }

    fn right_join_reverse(
        &self,
        action: &JoinAction,
        left_join_key: u64,
        left_record: &mut Record,
        left_lookup_key: &mut Box<JoinLookupKey>,
    ) -> Result<Vec<(JoinAction, Record, Box<JoinLookupKey>)>, JoinError> {
        let right_lookup_keys = self
            .right_lookup_index_map
            .get_vec(&left_join_key)
            .unwrap_or(&vec![])
            .clone();

        let mut output_records = vec![];

        // if there are no matching records on the left branch, no records will be returned
        if right_lookup_keys.is_empty() {
            return Ok(output_records);
        }

        for right_lookup_key in right_lookup_keys.iter() {
            // lookup on the right branch to find matching records
            let mut right_records = self.right_source.lookup(*right_lookup_key)?;

            for right_record in right_records.iter_mut() {
                let left_matching_count = self.get_left_matching_count(&action, right_record)?;

                let join_record = join_records(left_record, right_record);
                let join_lookup_key =
                    self.compose_join_lookup_key(Some(*left_lookup_key), Some(*right_lookup_key));

                if left_matching_count > 0 {
                    // if there are multiple matching records on the left branch, the right record will be just returned
                    output_records.push((action.clone(), join_record, join_lookup_key));
                } else {
                    match action {
                        JoinAction::Insert => {
                            let old_join_record = join_records(
                                &Record::from_schema(&self.left_source.get_output_schema()),
                                right_record,
                            );
                            let old_join_lookup_key =
                                self.compose_join_lookup_key(Some(*left_lookup_key), None);
                            output_records.push((
                                JoinAction::Delete,
                                old_join_record,
                                old_join_lookup_key,
                            ));

                            output_records.push((JoinAction::Insert, join_record, join_lookup_key));
                        }
                        JoinAction::Delete => {
                            let new_join_record = join_records(
                                &Record::from_schema(&self.left_source.get_output_schema()),
                                right_record,
                            );
                            let new_join_lookup_key =
                                self.compose_join_lookup_key(Some(*left_lookup_key), None);
                            output_records.push((JoinAction::Delete, join_record, join_lookup_key));
                            output_records.push((
                                JoinAction::Insert,
                                new_join_record,
                                new_join_lookup_key,
                            ));
                        }
                    }
                }
            }
        }
        Ok(output_records)
    }

    fn left_join_reverse(
        &self,
        action: JoinAction,
        right_join_key: u64,
        right_record: &mut Record,
        right_lookup_key: &mut Box<JoinLookupKey>,
    ) -> Result<Vec<(JoinAction, Record, Box<JoinLookupKey>)>, JoinError> {
        let left_lookup_keys = self
            .left_lookup_index_map
            .get_vec(&right_join_key)
            .unwrap_or(&vec![])
            .clone();

        let mut output_records = vec![];

        // if there are no matching records on the left branch, no records will be returned
        if left_lookup_keys.is_empty() {
            return Ok(output_records);
        }

        for left_lookup_key in left_lookup_keys.iter() {
            // lookup on the left branch to find matching records
            let mut left_records = self.left_source.lookup(*left_lookup_key)?;

            for left_record in left_records.iter_mut() {
                let right_matching_count = self.get_right_matching_count(&action, left_record)?;

                let join_record = join_records(left_record, right_record);
                let join_lookup_key =
                    self.compose_join_lookup_key(Some(*left_lookup_key), Some(*right_lookup_key));

                if right_matching_count > 0 {
                    // if there are multiple matching records on the right branch, the left record will be just returned
                    output_records.push((action.clone(), join_record, join_lookup_key));
                } else {
                    match action {
                        JoinAction::Insert => {
                            let old_join_record = join_records(
                                left_record,
                                &Record::from_schema(&self.right_source.get_output_schema()),
                            );
                            let old_join_lookup_key =
                                self.compose_join_lookup_key(Some(*left_lookup_key), None);

                            // delete the "first left join" record
                            output_records.push((
                                JoinAction::Delete,
                                old_join_record,
                                old_join_lookup_key,
                            ));
                            // insert the new left join record
                            output_records.push((action.clone(), join_record, join_lookup_key));
                        }
                        JoinAction::Delete => {
                            let new_join_record = join_records(
                                left_record,
                                &Record::from_schema(&self.right_source.get_output_schema()),
                            );
                            let new_join_lookup_key =
                                self.compose_join_lookup_key(Some(*left_lookup_key), None);
                            output_records.push((action.clone(), join_record, join_lookup_key));
                            output_records.push((
                                JoinAction::Insert,
                                new_join_record,
                                new_join_lookup_key,
                            ));
                        }
                    }
                }
            }
        }
        Ok(output_records)
    }

    fn get_right_matching_count(
        &self,
        action: &JoinAction,
        left_record: &mut Record,
    ) -> Result<usize, JoinError> {
        let left_join_key_fields: Vec<Field> =
            left_record.get_fields_by_indexes(&self.left_join_key_indexes);
        let mut hasher = AHasher::default();
        left_join_key_fields.hash(&mut hasher);
        let left_join_key = hasher.finish();

        let right_lookup_keys = self
            .right_lookup_index_map
            .get_vec(&left_join_key)
            .unwrap_or(&vec![])
            .clone();

        let mut records_count = right_lookup_keys.len();
        if action == &JoinAction::Insert {
            records_count -= 1;
        }
        Ok(records_count)
    }

    fn get_left_matching_count(
        &self,
        action: &JoinAction,
        right_record: &mut Record,
    ) -> Result<usize, JoinError> {
        let right_join_key_fields =
            right_record.get_fields_by_indexes(&self.right_join_key_indexes);
        let mut hasher = AHasher::default();
        right_join_key_fields.hash(&mut hasher);
        let right_join_key = hasher.finish();

        let left_lookup_keys = self
            .left_lookup_index_map
            .get_vec(&right_join_key)
            .unwrap_or(&vec![])
            .clone();

        let mut records_count = left_lookup_keys.len();
        if action == &JoinAction::Insert {
            records_count -= 1;
        }
        Ok(records_count)
    }

    pub fn update_left_index(&mut self, action: JoinAction, key: u64, value: Box<JoinLookupKey>) {
        match action {
            JoinAction::Insert => {
                self.left_lookup_index_map.insert(key, value);
            }
            JoinAction::Delete => {
                self.left_lookup_index_map.remove(&key);
            }
        }
    }

    pub fn update_right_index(&mut self, action: JoinAction, key: u64, value: Box<JoinLookupKey>) {
        match action {
            JoinAction::Insert => {
                self.right_lookup_index_map.insert(key, value);
            }
            JoinAction::Delete => {
                self.right_lookup_index_map.remove(&key);
            }
        }
    }

    fn compose_join_lookup_key(
        &self,
        left_lookup_key: Option<Box<JoinLookupKey>>,
        right_lookup_key: Option<Box<JoinLookupKey>>,
    ) -> Box<JoinLookupKey> {
        Box::new(JoinLookupKey::Composite(CompositeLookupKey {
            left: left_lookup_key,
            right: right_lookup_key,
        }))
    }

    fn split_join_lookup_key(
        &self,
        join_lookup_key: Box<JoinLookupKey>,
    ) -> Result<(Option<Box<JoinLookupKey>>, Option<Box<JoinLookupKey>>), JoinError> {
        match *join_lookup_key {
            JoinLookupKey::Lookup(_) => return Err(JoinError::InvalidJoinKey(*join_lookup_key)),
            JoinLookupKey::Composite(key) => Ok((key.left, key.right)),
        }
    }
}

fn join_records(left_record: &Record, right_record: &Record) -> Record {
    let concat_values = [left_record.values.clone(), right_record.values.clone()].concat();
    Record::new(None, concat_values, None)
}
