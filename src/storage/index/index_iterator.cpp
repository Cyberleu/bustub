/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(page_id_t leaf_page_id, int index, BufferPoolManager *bpm)
    : leaf_page_id_(leaf_page_id), bpm_(bpm), index_(index) {
  if (leaf_page_id_ != INVALID_PAGE_ID) {
    ReadPageGuard read_guard = bpm_->FetchPageRead(leaf_page_id);
    leaf_page_ = read_guard.As<B_PLUS_TREE_LEAF_PAGE_TYPE>();
    pair_ = MappingType(leaf_page_->KeyAt(index_), leaf_page_->ValueAt(index_));
  }
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool {
  return (index_ == leaf_page_->GetSize()) && (leaf_page_->GetNextPageId() == INVALID_PAGE_ID);
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & { return pair_; }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  index_++;
  if (IsEnd()) {
    return *this;
  }
  if (index_ == leaf_page_->GetSize() && leaf_page_->GetNextPageId() != INVALID_PAGE_ID) {
    // The last key-value pair of this leaf page.
    leaf_page_id_ = leaf_page_->GetNextPageId();
    ReadPageGuard read_guard = bpm_->FetchPageRead(leaf_page_id_);
    leaf_page_ = read_guard.As<B_PLUS_TREE_LEAF_PAGE_TYPE>();
    index_ = 0;
  }
  pair_ = MappingType(leaf_page_->KeyAt(index_), leaf_page_->ValueAt(index_));
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
