#![allow(clippy::too_many_arguments)]

use crate::pipeline::errors::PipelineError;
use crate::pipeline::expression::execution::ExpressionExecutor;
use crate::pipeline::{aggregation::aggregator::Aggregator, expression::execution::Expression};
use dozer_core::channels::ProcessorChannelForwarder;
use dozer_core::errors::ExecutionError;
use dozer_core::errors::ExecutionError::InternalError;
use dozer_core::node::{PortHandle, Processor};
use dozer_core::storage::lmdb_storage::{LmdbExclusiveTransaction, SharedTransaction};
use dozer_core::DEFAULT_PORT_HANDLE;
use dozer_types::errors::types::TypeError;
use dozer_types::types::{Field, FieldType, Operation, Record, Schema};
use std::cell::RefCell;
use std::hash::{Hash, Hasher};

use crate::pipeline::aggregation::aggregator::{
    get_aggregator_from_aggregator_type, get_aggregator_type_from_aggregation_expression,
    AggregatorType,
};
use ahash::AHasher;
use dozer_core::epoch::Epoch;
use dozer_core::storage::common::Database;
use dozer_core::storage::prefix_transaction::PrefixTransaction;
use hashbrown::HashMap;
use lmdb::DatabaseFlags;
use std::mem::size_of_val;
use std::ptr::hash;
use std::rc::Rc;

const DEFAULT_SEGMENT_KEY: &str = "DOZER_DEFAULT_SEGMENT_KEY";

enum DimensionAggregationDataType {}

#[derive(Debug)]
struct AggregationState {
    count: usize,
    states: Vec<Box<dyn Aggregator>>,
    values: Option<Vec<Field>>,
}

impl AggregationState {
    pub fn new(types: &Vec<AggregatorType>, ret_types: &Vec<FieldType>) -> Self {
        let mut states: Vec<Box<dyn Aggregator>> = Vec::new();
        for (idx, typ) in types.iter().enumerate() {
            let mut aggr = get_aggregator_from_aggregator_type(*typ);
            aggr.init(ret_types[idx]);
            states.push(aggr);
        }

        Self {
            count: 0,
            states,
            values: None,
        }
    }
}

#[derive(Debug)]
pub struct AggregationProcessor {
    dimensions: Vec<Expression>,
    measures: Vec<Vec<Expression>>,
    measures_types: Vec<AggregatorType>,
    measures_return_types: Vec<FieldType>,
    projections: Vec<Expression>,
    input_schema: Schema,
    aggregation_schema: Schema,
    states: HashMap<u64, AggregationState>,
    default_segment_key: u64,
}

enum AggregatorOperation {
    Insert,
    Delete,
    Update,
}

impl AggregationProcessor {
    pub fn new(
        dimensions: Vec<Expression>,
        measures: Vec<Expression>,
        projections: Vec<Expression>,
        input_schema: Schema,
        aggregation_schema: Schema,
    ) -> Result<Self, PipelineError> {
        let mut aggr_types = Vec::new();
        let mut aggr_measures = Vec::new();
        let mut aggr_measures_ret_types = Vec::new();

        for measure in measures {
            let (aggr_measure, aggr_type) =
                get_aggregator_type_from_aggregation_expression(&measure, &input_schema)?;
            aggr_measures.push(aggr_measure);
            aggr_types.push(aggr_type);
            aggr_measures_ret_types.push(measure.get_type(&input_schema)?.return_type)
        }

        let mut hasher = AHasher::default();
        DEFAULT_SEGMENT_KEY.hash(&mut hasher);

        Ok(Self {
            dimensions,
            projections,
            input_schema,
            aggregation_schema,
            states: HashMap::new(),
            measures: aggr_measures,
            measures_types: aggr_types,
            measures_return_types: aggr_measures_ret_types,
            default_segment_key: hasher.finish(),
        })
    }

    fn calc_and_fill_measures(
        curr_state: &mut AggregationState,
        deleted_record: Option<&Record>,
        inserted_record: Option<&Record>,
        out_rec_delete: &mut Vec<Field>,
        out_rec_insert: &mut Vec<Field>,
        op: AggregatorOperation,
        measures: &Vec<Vec<Expression>>,
        input_schema: &Schema,
    ) -> Result<Vec<Field>, PipelineError> {
        //

        let mut new_fields: Vec<Field> = Vec::with_capacity(measures.len());

        for (idx, measure) in measures.iter().enumerate() {
            let mut curr_aggr = &mut curr_state.states[idx];
            let curr_val_opt: Option<&Field> = curr_state.values.as_ref().map(|e| &e[idx]);

            let new_val = match op {
                AggregatorOperation::Insert => {
                    let mut inserted_fields = Vec::with_capacity(measure.len());
                    for m in measure {
                        inserted_fields.push(m.evaluate(inserted_record.unwrap(), &input_schema)?);
                    }
                    if let Some(curr_val) = curr_val_opt {
                        out_rec_delete.push(curr_val.clone());
                    }
                    curr_aggr.insert(&inserted_fields)?
                }
                AggregatorOperation::Delete => {
                    let mut deleted_fields = Vec::with_capacity(measure.len());
                    for m in measure {
                        deleted_fields.push(m.evaluate(deleted_record.unwrap(), &input_schema)?);
                    }
                    if let Some(curr_val) = curr_val_opt {
                        out_rec_delete.push(curr_val.clone());
                    }
                    curr_aggr.delete(&deleted_fields)?
                }
                AggregatorOperation::Update => {
                    let mut deleted_fields = Vec::with_capacity(measure.len());
                    for m in measure {
                        deleted_fields.push(m.evaluate(deleted_record.unwrap(), &input_schema)?);
                    }
                    let mut inserted_fields = Vec::with_capacity(measure.len());
                    for m in measure {
                        inserted_fields.push(m.evaluate(inserted_record.unwrap(), &input_schema)?);
                    }
                    if let Some(curr_val) = curr_val_opt {
                        out_rec_delete.push(curr_val.clone());
                    }
                    curr_aggr.update(&deleted_fields, &inserted_fields)?
                }
            };
            out_rec_insert.push(new_val.clone());
            new_fields.push(new_val);
        }
        Ok(new_fields)
    }

    fn agg_delete(&mut self, old: &mut Record) -> Result<Operation, PipelineError> {
        let mut out_rec_delete: Vec<Field> = Vec::with_capacity(self.measures.len());
        let mut out_rec_insert: Vec<Field> = Vec::with_capacity(self.measures.len());

        let key = if !self.dimensions.is_empty() {
            get_key(&self.input_schema, old, &self.dimensions)?
        } else {
            self.default_segment_key
        };

        let mut curr_state_opt = self.states.get_mut(&key);
        assert!(
            curr_state_opt.is_some(),
            "Unable to find aggregator state during DELETE operation"
        );
        let mut curr_state = curr_state_opt.unwrap();

        let new_values = Self::calc_and_fill_measures(
            curr_state,
            Some(old),
            None,
            &mut out_rec_delete,
            &mut out_rec_insert,
            AggregatorOperation::Delete,
            &self.measures,
            &self.input_schema,
        )?;

        let res = if curr_state.count == 1 {
            self.states.remove(&key);
            Operation::Delete {
                old: Self::build_projection(
                    old,
                    out_rec_delete,
                    &self.projections,
                    &self.aggregation_schema,
                )?,
            }
        } else {
            curr_state.count -= 1;
            curr_state.values = Some(new_values);
            Operation::Update {
                new: Self::build_projection(
                    old,
                    out_rec_insert,
                    &self.projections,
                    &self.aggregation_schema,
                )?,
                old: Self::build_projection(
                    old,
                    out_rec_delete,
                    &self.projections,
                    &self.aggregation_schema,
                )?,
            }
        };

        Ok(res)
    }

    fn agg_insert(&mut self, new: &mut Record) -> Result<Operation, PipelineError> {
        let mut out_rec_delete: Vec<Field> = Vec::with_capacity(self.measures.len());
        let mut out_rec_insert: Vec<Field> = Vec::with_capacity(self.measures.len());

        let key = if !self.dimensions.is_empty() {
            get_key(&self.input_schema, new, &self.dimensions)?
        } else {
            self.default_segment_key
        };

        let curr_state = self.states.entry(key).or_insert(AggregationState::new(
            &self.measures_types,
            &self.measures_return_types,
        ));

        let new_values = Self::calc_and_fill_measures(
            curr_state,
            None,
            Some(new),
            &mut out_rec_delete,
            &mut out_rec_insert,
            AggregatorOperation::Insert,
            &self.measures,
            &self.input_schema,
        )?;

        let res = if curr_state.count == 0 {
            Operation::Insert {
                new: Self::build_projection(
                    new,
                    out_rec_insert,
                    &self.projections,
                    &self.aggregation_schema,
                )?,
            }
        } else {
            Operation::Update {
                new: Self::build_projection(
                    new,
                    out_rec_insert,
                    &self.projections,
                    &self.aggregation_schema,
                )?,
                old: Self::build_projection(
                    new,
                    out_rec_delete,
                    &self.projections,
                    &self.aggregation_schema,
                )?,
            }
        };

        curr_state.count += 1;
        curr_state.values = Some(new_values);

        Ok(res)
    }

    fn agg_update(
        &mut self,
        old: &mut Record,
        new: &mut Record,
        key: u64,
    ) -> Result<Operation, PipelineError> {
        let mut out_rec_delete: Vec<Field> = Vec::with_capacity(self.measures.len());
        let mut out_rec_insert: Vec<Field> = Vec::with_capacity(self.measures.len());

        let mut curr_state_opt = self.states.get_mut(&key);
        assert!(
            curr_state_opt.is_some(),
            "Unable to find aggregator state during UPDATE operation"
        );
        let mut curr_state = curr_state_opt.unwrap();

        let new_values = Self::calc_and_fill_measures(
            curr_state,
            Some(old),
            Some(new),
            &mut out_rec_delete,
            &mut out_rec_insert,
            AggregatorOperation::Update,
            &self.measures,
            &self.input_schema,
        )?;

        let res = Operation::Update {
            new: Self::build_projection(
                new,
                out_rec_insert,
                &self.projections,
                &self.aggregation_schema,
            )?,
            old: Self::build_projection(
                old,
                out_rec_delete,
                &self.projections,
                &self.aggregation_schema,
            )?,
        };

        curr_state.values = Some(new_values);
        Ok(res)
    }

    pub fn build_projection(
        original: &mut Record,
        measures: Vec<Field>,
        projections: &Vec<Expression>,
        aggregation_schema: &Schema,
    ) -> Result<Record, PipelineError> {
        let original_len = original.values.len();
        original.values.extend(measures);
        let mut output = Vec::<Field>::with_capacity(projections.len());
        for exp in projections {
            output.push(exp.evaluate(original, aggregation_schema)?);
        }
        original.values.drain(original_len..);
        Ok(Record::new(None, output, None))
    }

    pub fn aggregate(&mut self, mut op: Operation) -> Result<Vec<Operation>, PipelineError> {
        match op {
            Operation::Insert { ref mut new } => Ok(vec![self.agg_insert(new)?]),
            Operation::Delete { ref mut old } => Ok(vec![self.agg_delete(old)?]),
            Operation::Update {
                ref mut old,
                ref mut new,
            } => {
                let (old_record_hash, new_record_hash) = if self.dimensions.is_empty() {
                    (self.default_segment_key, self.default_segment_key)
                } else {
                    (
                        get_key(&self.input_schema, old, &self.dimensions)?,
                        get_key(&self.input_schema, new, &self.dimensions)?,
                    )
                };

                if old_record_hash == new_record_hash {
                    Ok(vec![self.agg_update(old, new, old_record_hash)?])
                } else {
                    Ok(vec![self.agg_delete(old)?, self.agg_insert(new)?])
                }
            }
        }
    }
}

fn get_key(
    schema: &Schema,
    record: &Record,
    dimensions: &[Expression],
) -> Result<u64, PipelineError> {
    let mut key = Vec::<Field>::with_capacity(dimensions.len());
    for dimension in dimensions.iter() {
        key.push(dimension.evaluate(record, schema)?);
    }
    let mut hasher = AHasher::default();
    key.hash(&mut hasher);
    Ok(hasher.finish())
}

impl Processor for AggregationProcessor {
    fn commit(&self, _epoch: &Epoch, _tx: &SharedTransaction) -> Result<(), ExecutionError> {
        Ok(())
    }

    fn process(
        &mut self,
        _from_port: PortHandle,
        op: Operation,
        fw: &mut dyn ProcessorChannelForwarder,
        txn: &SharedTransaction,
    ) -> Result<(), ExecutionError> {
        let ops = self.aggregate(op).map_err(|e| InternalError(Box::new(e)))?;
        for fop in ops {
            fw.send(fop, DEFAULT_PORT_HANDLE)?;
        }
        Ok(())
    }
}
