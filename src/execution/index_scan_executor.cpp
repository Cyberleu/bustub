//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() {
  Catalog *catalog = exec_ctx_->GetCatalog();
  index_oid_t index_oid = plan_->GetIndexOid();
  index_info_ = catalog->GetIndex(index_oid);
  table_info_ = catalog->GetTable(index_info_->table_name_);
  tree_ = dynamic_cast<BPlusTreeIndexForTwoIntegerColumn *>(index_info_->index_.get());
  iter_ = tree_->GetBeginIterator();
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  std::cout << "i'm here2" << '\n';
  while (iter_ != tree_->GetEndIterator()) {
    *rid = (*iter_).second;
    *tuple = table_info_->table_->GetTuple(*rid).second;
    ++iter_;
    return true;
  }
  return false;
}

}  // namespace bustub
