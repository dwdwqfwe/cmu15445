#include "execution/execution_common.h"
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <mutex>
#include <optional>
#include <vector>
#include "catalog/catalog.h"
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/config.h"
#include "common/logger.h"
#include "common/macros.h"
#include "common/rid.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "execution/executors/insert_executor.h"
#include "fmt/core.h"
#include "storage/table/table_heap.h"
#include "storage/table/tuple.h"
#include "type/type.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

auto CreateUndoLog(Tuple &new_tuple, Tuple &old_tuple, const Schema &schema, const UndoLog &before) -> UndoLog {
  std::vector<Value> res;
  std::vector<Column> res_type;
  std::vector<bool> before_change = before.modified_fields_;
  if (before_change.empty()) {
    for (uint32_t i = 0; i < schema.GetColumnCount(); i++) {
      before_change.push_back(false);
    }
  }
  UndoLog undo_log;
  undo_log.is_deleted_ = false;
  auto before_schema = GetUndoSchema(&schema, before);
  uint32_t col = 0;
  for (uint32_t i = 0; i < schema.GetColumnCount(); i++) {
    if (before_change[i]) {
      res.push_back(before.tuple_.GetValue(&before_schema, col));
      res_type.emplace_back(schema.GetColumn(i).GetName(), schema.GetColumn(i).GetType());
      undo_log.modified_fields_.emplace_back(true);
      col++;
    } else if (!new_tuple.GetValue(&schema, i).CompareExactlyEquals(old_tuple.GetValue(&schema, i))) {
      undo_log.modified_fields_.push_back(true);
      res.push_back(old_tuple.GetValue(&schema, i));
      res_type.emplace_back(schema.GetColumn(i).GetName(), schema.GetColumn(i).GetType());
    } else {
      undo_log.modified_fields_.push_back(false);
    }
  }
  Schema res_schema = Schema(res_type);
  undo_log.tuple_ = Tuple(res, &res_schema);
  return undo_log;
}

// check wirte-write conflict
auto CheckConflict(TupleMeta &meta, RID rid, Transaction *txn) -> bool {
  if (meta.ts_ >= TXN_START_ID && meta.ts_ != txn->GetTransactionId()) {
    return false;
  }
  if (meta.ts_ < TXN_START_ID && meta.ts_ > txn->GetReadTs()) {
    return false;
  }
  return true;
}
// get the schema of undolog
auto GetUndoSchema(const Schema *schema, const UndoLog &log) -> Schema {
  std::vector<Column> undo_colum;
  for (uint32_t i = 0; i < log.modified_fields_.size(); i++) {
    if (log.modified_fields_[i]) {
      undo_colum.push_back(schema->GetColumn(i));
    }
  }
  Schema res(undo_colum);
  return res;
}

auto SetUndoColum(const Schema &undo_schema, std::vector<Value> &res, const UndoLog &log) -> void {
  uint32_t col = 0;
  for (size_t i = 0; i < log.modified_fields_.size(); i++) {
    if (log.modified_fields_[i]) {
      res[i] = log.tuple_.GetValue(&undo_schema, col);
      col++;
    }
  }
}

auto ReconstructTuple(const Schema *schema, const Tuple &base_tuple, const TupleMeta &base_meta,
                      const std::vector<UndoLog> &undo_logs) -> std::optional<Tuple> {
  bool deter_delete_base = base_meta.is_deleted_;
  bool deter_delete_undo = false;
  std::vector<Value> res;
  res.reserve(schema->GetColumnCount());
  for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
    res.push_back(base_tuple.GetValue(schema, i));
  }
  for (auto &log : undo_logs) {
    if (log.is_deleted_) {
      deter_delete_undo = true;
      continue;
    }
    deter_delete_undo = false;
    auto undo_schema = GetUndoSchema(schema, log);
    SetUndoColum(undo_schema, res, log);
  }

  if ((deter_delete_undo) || (undo_logs.empty() && deter_delete_base)) {
    return std::nullopt;
  }
  return Tuple(res, schema);
}

void TxnMgrDbg(const std::string &info, TransactionManager *txn_mgr, const TableInfo *table_info,
               TableHeap *table_heap) {
  fmt::println(stderr, "\033[36mTxnMgrDbg start...");
  // always use stderr for printing logs...
  fmt::println(stderr, "debug_hook: {}", info);
  // RID: page_id/slot
  std::ostringstream debug;
  auto str_timestamp = [](const timestamp_t &ts) -> std::string {
    return ts >= TXN_START_ID ? "txn" + std::to_string(ts - TXN_START_ID) : std::to_string(ts);
  };
  auto t_itr = table_heap->MakeIterator();
  while (!t_itr.IsEnd()) {
    auto [meta, tuple] = t_itr.GetTuple();
    auto rid = t_itr.GetRID();
    ++t_itr;
    // table heap中最新数据
    if (meta.is_deleted_) {
      debug << fmt::format("\nRID={}/{} ts={} <del> tuple={}\n", rid.GetPageId(), rid.GetSlotNum(),
                           str_timestamp(meta.ts_), tuple.ToString(&table_info->schema_));
    } else {
      debug << fmt::format("\nRID={}/{} ts={} tuple={}\n", rid.GetPageId(), rid.GetSlotNum(), str_timestamp(meta.ts_),
                           tuple.ToString(&table_info->schema_));
    }
    // 打印版本链
    auto undo_link = txn_mgr->GetUndoLink(rid);
    std::vector<UndoLog> undo_logs;
    unsigned count = 0;

    if (!undo_link.has_value()) {
      continue;
    }
    while (undo_link->IsValid()) {
      auto tmp_undo_log_opt = txn_mgr->GetUndoLogOptional(undo_link.value());
      if (!tmp_undo_log_opt.has_value()) {
        break;
      }
      auto tmp_undo_log = tmp_undo_log_opt.value();
      undo_logs.push_back(tmp_undo_log);
      auto rebuild_tuple = ReconstructTuple(&table_info->schema_, tuple, meta, undo_logs);
      if (rebuild_tuple.has_value()) {
        debug << fmt::format("  {}:txn{} tuple={} ts={}\n", count, undo_link->prev_txn_ ^ TXN_START_ID,
                             rebuild_tuple->ToString(&table_info->schema_), tmp_undo_log.ts_);
      } else {
        // tmp_undo_log is delete flag
        debug << fmt::format("  {}:txn{} <del> ts={}\n", count, undo_link->prev_txn_ ^ TXN_START_ID, tmp_undo_log.ts_);
      }
      undo_link = tmp_undo_log.prev_version_;
      count += 1;
    }
  }

  fmt::println(stderr, "{}", debug.str());
  fmt::println(stderr, "\033[36mTxnMgrDbg end\033[0m");
}

}  // namespace bustub
