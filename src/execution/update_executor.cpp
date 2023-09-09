//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  // As of Fall 2022, you DON'T need to implement update executor to have perfect score in project 3 / project 4.
}

void UpdateExecutor::Init() {
  child_executor_->Init();
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  index_infoes_ = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
}

auto UpdateExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (no_next_) {
    return false;
  }
  while (child_executor_->Next(tuple, rid)) {
    // Construct the new tuple to be inserted.
    std::vector<Value> values{};
    values.reserve(child_executor_->GetOutputSchema().GetColumnCount());
    for (const auto &exp_ref : plan_->target_expressions_) {
      Value value = exp_ref->Evaluate(tuple, child_executor_->GetOutputSchema());
      values.emplace_back(value);
    }
    Tuple new_tuple = Tuple(values, &child_executor_->GetOutputSchema());
    // Set the meta of old tuple to deleted.
    auto meta = table_info_->table_->GetTupleMeta(*rid);
    meta.is_deleted_ = true;
    table_info_->table_->UpdateTupleMeta(meta, *rid);
    // Insert the new tuple.
    TupleMeta new_meta = TupleMeta();
    new_meta.delete_txn_id_ = INVALID_TXN_ID;
    new_meta.insert_txn_id_ = INVALID_TXN_ID;
    new_meta.is_deleted_ = false;
    *rid = table_info_->table_->InsertTuple(new_meta, new_tuple).value();
    // Update related indexes.
    for (auto index_info : index_infoes_) {
      std::vector<uint32_t> key_attrs;
      for (auto &column : index_info->key_schema_.GetColumns()) {
        key_attrs.push_back(table_info_->schema_.GetColIdx(column.GetName()));
      }
      Tuple old_key = tuple->KeyFromTuple(child_executor_->GetOutputSchema(), index_info->key_schema_, key_attrs);
      index_info->index_->DeleteEntry(old_key, *rid, exec_ctx_->GetTransaction());
      Tuple new_key = new_tuple.KeyFromTuple(child_executor_->GetOutputSchema(), index_info->key_schema_, key_attrs);
      index_info->index_->InsertEntry(new_key, *rid, exec_ctx_->GetTransaction());
    }
    count_++;
  }
  no_next_ = true;
  *tuple = Tuple(std::vector<Value>{Value(INTEGER, count_)}, &plan_->OutputSchema());
  return true;
}

}  // namespace bustub
