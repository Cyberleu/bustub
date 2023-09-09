//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  child_executor_->Init();
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  index_infoes_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (no_next_) {
    return false;
  }
  while (child_executor_->Next(tuple, rid)) {
    auto meta = TupleMeta();
    meta.is_deleted_ = false;
    *rid = table_info_->table_->InsertTuple(meta, *tuple).value();
    for (auto index_info : index_infoes_) {
      std::vector<uint32_t> key_attrs;
      for (auto &column : index_info->key_schema_.GetColumns()) {
        key_attrs.push_back(table_info_->schema_.GetColIdx(column.GetName()));
      }
      Tuple new_key = tuple->KeyFromTuple(child_executor_->GetOutputSchema(), index_info->key_schema_, key_attrs);
      index_info->index_->InsertEntry(new_key, *rid, exec_ctx_->GetTransaction());
    }
    count_++;
  }
  no_next_ = true;
  *tuple = Tuple(std::vector<Value>{Value(INTEGER, count_)}, &plan_->OutputSchema());
  return true;
}

}  // namespace bustub
