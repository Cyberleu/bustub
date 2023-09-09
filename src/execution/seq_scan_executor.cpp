//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  table_oid_t table_oid = plan_->GetTableOid();
  Catalog *catalog = exec_ctx_->GetCatalog();
  TableInfo *tableinfo = catalog->GetTable(table_oid);
  iter_ = std::make_unique<TableIterator>(tableinfo->table_->MakeIterator());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (!iter_->IsEnd()) {
    TupleMeta meta = iter_->GetTuple().first;
    if (meta.is_deleted_) {
      ++(*iter_);
      continue;
    }
    *tuple = iter_->GetTuple().second;
    *rid = iter_->GetRID();
    ++(*iter_);
    return true;
  }
  return false;
}

}  // namespace bustub
