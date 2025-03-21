#pragma once

#include <string>
#include <valarray>
#include <vector>

#include "catalog/catalog.h"
#include "catalog/schema.h"
#include "common/rid.h"
#include "concurrency/transaction.h"
#include "storage/table/tuple.h"

namespace bustub {

auto CreateUndoLog(Tuple &new_tuple, Tuple &old_tuple, const Schema &schema, const UndoLog &before = UndoLog())
    -> UndoLog;

auto CheckConflict(TupleMeta &meta, RID rid, Transaction *txn) -> bool;

auto GetUndoSchema(const Schema *schema, const UndoLog &log) -> Schema;

auto SetUndoColum(const Schema &undo_schema, std::vector<Value> &res, const UndoLog &log) -> void;

auto ReconstructTuple(const Schema *schema, const Tuple &base_tuple, const TupleMeta &base_meta,
                      const std::vector<UndoLog> &undo_logs) -> std::optional<Tuple>;

void TxnMgrDbg(const std::string &info, TransactionManager *txn_mgr, const TableInfo *table_info,
               TableHeap *table_heap);

// Add new functions as needed... You are likely need to define some more functions.
//
// To give you a sense of what can be shared across executors / transaction manager, here are the
// list of helper function names that we defined in the reference solution. You should come up with
// your own when you go through the process.
// * CollectUndoLogs
// * WalkUndoLogs
// * Modify
// * IsWriteWriteConflict
// * GenerateDiffLog
// * GenerateNullTupleForSchema
// * GetUndoLogSchema
//
// We do not provide the signatures for these functions because it depends on the your implementation
// of other parts of the system. You do not need to define the same set of helper functions in
// your implementation. Please add your own ones as necessary so that you do not need to write
// the same code everywhere.

}  // namespace bustub
