#include <iostream>
#include <sstream>
#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"

namespace bustub {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                          const KeyComparator &comparator, int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      bpm_(buffer_pool_manager),
      comparator_(std::move(comparator)),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size),
      header_page_id_(header_page_id) {
  WritePageGuard guard = bpm_->FetchPageWrite(header_page_id_);
  auto root_page = guard.AsMut<BPlusTreeHeaderPage>();
  root_page->root_page_id_ = INVALID_PAGE_ID;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::BinarySearch(const KeyType &key, const InternalPage *internal_page) -> int {  // upper_bound
  int left = 1;
  int right = internal_page->GetSize();
  while (left < right) {
    int mid = left + (right - left) / 2;
    if (comparator_(internal_page->KeyAt(mid), key) != 1) {
      left = mid + 1;
    } else {
      right = mid;
    }
  }
  return left;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::BinarySearch(const KeyType &key, const LeafPage *leaf_page) -> int {  // lower_bound
  int left = 0;
  int right = leaf_page->GetSize();
  while (left < right) {
    int mid = left + (right - left) / 2;
    if (comparator_(leaf_page->KeyAt(mid), key) == -1) {
      left = mid + 1;
    } else {
      right = mid;
    }
  }
  return left;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::SplitLeaf(MappingType insert_value, page_id_t &right_page_id, KeyType &new_key, Context &ctx) {
  // std::cout << std::this_thread::get_id() << "Start Split Leaf!" << '\n';
  auto leaf_page = ctx.write_set_.back().AsMut<LeafPage>();
  int insert_idx = BinarySearch(insert_value.first, leaf_page);
  // std::cout << "leaf insert index:" << insert_idx << std::endl;
  BasicPageGuard basic_guard = bpm_->NewPageGuarded(&right_page_id);
  auto right_page = basic_guard.AsMut<LeafPage>();
  right_page->Init(leaf_max_size_);
  // std::cout << "i'm here1 !" << '\n';
  for (int i = leaf_page->GetSize(); i > insert_idx; i--) {
    leaf_page->SetAt(i, leaf_page->KeyAt(i - 1), leaf_page->ValueAt(i - 1));
  }
  // std::cout << "i'm here2 !" << '\n';
  // std::cout << "leaf page size:" << LEAF_PAGE_SIZE << '\n';
  // std::cout << "insert_idx" << insert_idx << "leaf max size" << leaf_page->GetMaxSize() << '\n';
  leaf_page->SetAt(insert_idx, insert_value.first, insert_value.second);
  // std::cout << "i'm here3 !" << '\n';
  int split_idx = (leaf_page->GetMaxSize() + 1) / 2;
  for (int i = split_idx; i <= leaf_page->GetMaxSize(); i++) {  // Copy data from original page to the new page
    right_page->SetAt(i - split_idx, leaf_page->KeyAt(i), leaf_page->ValueAt(i));
  }
  new_key = right_page->KeyAt(0);
  leaf_page->SetSize(split_idx);
  right_page->SetSize(leaf_page->GetMaxSize() - split_idx + 1);
  // std::cout << "i'm here5 !" << '\n';
  right_page->SetNextPageId(leaf_page->GetNextPageId());
  leaf_page->SetNextPageId(right_page_id);
  right_page->SetParentPageId(leaf_page->GetParentPageId());
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::SplitInternal(page_id_t &right_page_id, KeyType &new_key, Context &ctx) {
  // std::cout << std::this_thread::get_id() << "Start Split Internal!" << '\n';
  auto internal_page = ctx.write_set_.back().AsMut<InternalPage>();
  int split_idx = internal_page->GetMaxSize() / 2 + 1;
  new_key = internal_page->KeyAt(split_idx);
  // std::cout << "split_idx" << split_idx << "new_key" << new_key << '\n';
  BasicPageGuard basic_guard = bpm_->NewPageGuarded(&right_page_id);
  auto right_page = basic_guard.AsMut<InternalPage>();
  right_page->Init(internal_max_size_);
  // std:: cout << "right_page_id:" << right_page_id << '\n';
  for (int i = split_idx; i < internal_page->GetMaxSize() + 1; i++) {
    right_page->SetAt(i - split_idx, internal_page->KeyAt(i), internal_page->ValueAt(i));
  }
  internal_page->SetSize(split_idx);
  right_page->SetSize(internal_page->GetMaxSize() - split_idx + 1);
  // std::cout << "Reset the parent page id of each child node!" << '\n';
  for (int i = 0; i < right_page->GetSize(); i++) {  // Reset the parent page id of each child node.
    // std::cout << right_page->ValueAt(i) << ' ';
    WritePageGuard write_guard = bpm_->FetchPageWrite(right_page->ValueAt(i));
    auto tree_page = write_guard.AsMut<BPlusTreePage>();
    tree_page->SetParentPageId(right_page_id);
  }
  // std::cout << "Finish reset the parent page id of each child node!" << '\n';
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoInternal(KeyType key, page_id_t left_page_id, page_id_t right_page_id, Context &ctx) {
  // std::cout << std::this_thread::get_id() << "start insert into internal" << std::endl;
  // std::cout << "size:" << ctx.write_set_.size() << '\n';
  page_id_t internal_page_id = ctx.write_set_.back().PageId();
  // std::cout << internal_page_id << std::endl;
  if (internal_page_id == header_page_id_) {  // The internal page not exsisted
    // std::cout << std::this_thread::get_id() << "The internal page not exsisted" << std::endl;
    page_id_t root_page_id = INVALID_PAGE_ID;
    BasicPageGuard basic_guard = bpm_->NewPageGuarded(&root_page_id);
    // std::cout << "root_page_id:" << root_page_id << '\n';
    auto root_page = basic_guard.AsMut<InternalPage>();
    root_page->Init(internal_max_size_);
    root_page->SetAt(0, key, left_page_id);
    root_page->SetAt(1, key, right_page_id);
    root_page->SetSize(2);
    // std:: cout << "header_page_id:" << header_page_id_ << '\n';
    auto header_page = ctx.write_set_.back().AsMut<BPlusTreeHeaderPage>();
    header_page->root_page_id_ = root_page_id;
    // std:: cout << "left_page_id:" << left_page_id << '\n';
    WritePageGuard write_guard = bpm_->FetchPageWrite(left_page_id);
    auto tree_page = write_guard.AsMut<BPlusTreePage>();
    tree_page->SetParentPageId(root_page_id);
    write_guard = bpm_->FetchPageWrite(right_page_id);
    tree_page = write_guard.AsMut<BPlusTreePage>();
    tree_page->SetParentPageId(root_page_id);
    return;
  }
  auto internal_page = ctx.write_set_.back().AsMut<InternalPage>();
  int insert_idx = BinarySearch(key, internal_page);
  // std::cout << "insert_idx:" << insert_idx << '\n';
  // std::cout << "size:" << internal_page->GetSize() << '\n';
  for (int i = internal_page->GetSize(); i > insert_idx; i--) {  // Remove the value before insertion
    internal_page->SetAt(i, internal_page->KeyAt(i - 1), internal_page->ValueAt(i - 1));
  }
  // std::cout << "key:" << key << '\n';
  internal_page->SetAt(insert_idx, key, right_page_id);
  internal_page->SetAt(insert_idx - 1, internal_page->KeyAt(insert_idx - 1),
                       left_page_id);  // Update the page_id of splitted pages
  internal_page->IncreaseSize(1);
  WritePageGuard write_guard = bpm_->FetchPageWrite(right_page_id);
  auto tree_page = write_guard.AsMut<BPlusTreePage>();  // Set child node's parent page id
  tree_page->SetParentPageId(internal_page_id);
  write_guard.Drop();
  right_page_id = INVALID_PAGE_ID;
  KeyType new_key;
  if (internal_page->GetSize() > internal_page->GetMaxSize()) {  // The internal page is full
    // std::cout << std::this_thread::get_id() << "The internal page is full" << '\n';
    SplitInternal(right_page_id, new_key, ctx);
    ctx.write_set_.pop_back();
    InsertIntoInternal(new_key, internal_page_id, right_page_id, ctx);
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::RemoveFromLeaf(const KeyType &key, int &key_idx, Context &ctx) -> bool {
  // std::cout << std::this_thread::get_id() << "Start remove from leaf!" << '\n';
  auto leaf_page = ctx.write_set_.back().AsMut<LeafPage>();
  key_idx = BinarySearch(key, leaf_page);
  if (key_idx == leaf_page->GetSize() || comparator_(leaf_page->KeyAt(key_idx), key)) {
    return false;
  }  // key not found
  for (int i = key_idx + 1; i < leaf_page->GetSize(); i++) {
    leaf_page->SetAt(i - 1, leaf_page->KeyAt(i), leaf_page->ValueAt(i));
  }
  leaf_page->IncreaseSize(-1);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromInternal(const int &removed_idx, Context &ctx) {
  page_id_t internal_page_id = ctx.write_set_.back().PageId();
  // std::cout << std::this_thread::get_id() << "Start Remove from the Internal!"
  //           << "internal_page_id:" << internal_page_id << '\n';
  auto internal_page = ctx.write_set_.back().AsMut<InternalPage>();
  KeyType internal_key = internal_page->KeyAt(1);
  for (int i = removed_idx + 1; i < internal_page->GetSize(); i++) {
    internal_page->SetAt(i - 1, internal_page->KeyAt(i), internal_page->ValueAt(i));
  }
  internal_page->IncreaseSize(-1);
  if (internal_page->GetSize() < internal_page->GetMinSize()) {
    page_id_t parent_page_id = internal_page->GetParentPageId();
    page_id_t left_page_id = INVALID_PAGE_ID;
    page_id_t right_page_id = INVALID_PAGE_ID;

    if (parent_page_id == INVALID_PAGE_ID && internal_page->GetSize() > 1) {
      std::cout << std::this_thread::get_id() << "It is the root page, it has the minsize one" << '\n';
    } else if (parent_page_id == INVALID_PAGE_ID && internal_page->GetSize() == 1) {
      // std::cout << std::this_thread::get_id() << "Root page has to be changed!" << '\n';
      page_id_t new_root_page_id = internal_page->ValueAt(0);
      auto header_page = ctx.write_set_.front().AsMut<BPlusTreeHeaderPage>();
      header_page->root_page_id_ = new_root_page_id;
      WritePageGuard write_guard = bpm_->FetchPageWrite(new_root_page_id);
      auto root_page = write_guard.AsMut<BPlusTreePage>();
      root_page->SetParentPageId(INVALID_PAGE_ID);
      ctx.write_set_.pop_back();
      bpm_->DeletePage(internal_page_id);
    } else {  // Fetch page ids of its bros.
      // std::cout << std::this_thread::get_id() << "Search page ids of its bros." << '\n';
      auto parent_page = ctx.write_set_[ctx.write_set_.size() - 2].AsMut<InternalPage>();
      int key_idx = BinarySearch(internal_key, parent_page) - 1;
      if (key_idx - 1 >= 0) {
        left_page_id = parent_page->ValueAt(key_idx - 1);
      }
      if (key_idx + 1 < parent_page->GetSize()) {
        right_page_id = parent_page->ValueAt(key_idx + 1);
      }
      // std::cout << "parent_page_id:" << parent_page_id << "left:" <<
      // left_page_id << "right:" << right_page_id << '\n';
      InternalPage *left_internal_page = nullptr;
      InternalPage *right_internal_page = nullptr;
      if (left_page_id != INVALID_PAGE_ID) {
        // std::cout << left_page_id << '\n';
        ctx.write_sibling_set_.push_front(bpm_->FetchPageWrite(left_page_id));
        left_internal_page = ctx.write_sibling_set_.front().AsMut<InternalPage>();
      }
      if (right_page_id != INVALID_PAGE_ID) {
        ctx.write_sibling_set_.push_back(bpm_->FetchPageWrite(right_page_id));
        right_internal_page = ctx.write_sibling_set_.back().AsMut<InternalPage>();
      }
      if (left_page_id != INVALID_PAGE_ID && left_internal_page->GetSize() > left_internal_page->GetMinSize()) {
        // std::cout << std::this_thread::get_id() << "Borrow from the left internal page!" << '\n';
        // Prepare for the insertion.
        for (int i = internal_page->GetSize() - 1; i >= 0; i--) {
          internal_page->SetAt(i + 1, internal_page->KeyAt(i), internal_page->ValueAt(i));
        }
        // Move the key from the parent page down to the operating internal page.
        internal_page->SetAt(1, parent_page->KeyAt(key_idx), internal_page->ValueAt(1));
        // Replace the key in the parent page with the last key in tne left page.
        parent_page->SetAt(key_idx, left_internal_page->KeyAt(left_internal_page->GetSize() - 1),
                           parent_page->ValueAt(key_idx));
        // After insert the left key, update the value at index of 0 to the value of the inserted key.
        internal_page->SetAt(0, internal_page->KeyAt(0),
                             left_internal_page->ValueAt(left_internal_page->GetSize() - 1));
        // std::cout << left_internal_page->ValueAt(left_internal_page->GetSize() - 1) << '\n';
        WritePageGuard write_guard_temp =
            bpm_->FetchPageWrite(left_internal_page->ValueAt(left_internal_page->GetSize() - 1));
        auto page_temp = write_guard_temp.AsMut<BPlusTreePage>();
        page_temp->SetParentPageId(internal_page_id);
        internal_page->IncreaseSize(1);
        left_internal_page->IncreaseSize(-1);
      } else if (right_page_id != INVALID_PAGE_ID &&
                 right_internal_page->GetSize() > right_internal_page->GetMinSize()) {
        // std::cout << "Borrow from the right internal page!" << '\n';
        internal_page->SetAt(internal_page->GetSize(), parent_page->KeyAt(key_idx + 1),
                             right_internal_page->ValueAt(0));
        WritePageGuard write_guard_temp = bpm_->FetchPageWrite(right_internal_page->ValueAt(0));
        auto page_temp = write_guard_temp.AsMut<BPlusTreePage>();
        page_temp->SetParentPageId(internal_page_id);
        parent_page->SetAt(key_idx + 1, right_internal_page->KeyAt(1), parent_page->ValueAt(key_idx + 1));
        for (int i = 1; i < right_internal_page->GetSize(); i++) {
          right_internal_page->SetAt(i - 1, right_internal_page->KeyAt(i), right_internal_page->ValueAt(i));
        }
        internal_page->IncreaseSize(1);
        right_internal_page->IncreaseSize(-1);
      } else if (left_page_id != INVALID_PAGE_ID) {
        // std::cout << std::this_thread::get_id() << "Merge left internal page!" << '\n';
        KeyType parent_key = parent_page->KeyAt(key_idx);
        MergeInternalNode(left_internal_page, internal_page, parent_key, left_page_id, right_page_id);
        ctx.write_set_.pop_back();
        ctx.write_sibling_set_.clear();
        RemoveFromInternal(key_idx, ctx);
      } else {
        // std::cout << std::this_thread::get_id() << "Merge right internal page!" << '\n';
        KeyType parent_key = parent_page->KeyAt(key_idx + 1);
        MergeInternalNode(internal_page, right_internal_page, parent_key, internal_page_id, right_page_id);
        ctx.write_set_.pop_back();
        ctx.write_sibling_set_.clear();
        RemoveFromInternal(key_idx + 1, ctx);
      }
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::MergeLeafNode(LeafPage *page1, LeafPage *page2, page_id_t left_page_id, page_id_t right_page_id) {
  // ReadPageGuard read_guard = bpm_->FetchPageRead(right_page_id);
  // const LeafPage *right_page = read_guard.As<LeafPage>();
  // WritePageGuard write_guard = bpm_->FetchPageWrite(left_page_id);
  // LeafPage *left_page = write_guard.AsMut<LeafPage>();
  // std::cout << std::this_thread::get_id() << "Start merge leaf node" << '\n';
  for (int i = 0; i < page2->GetSize(); i++) {
    page1->SetAt(page1->GetSize() + i, page2->KeyAt(i), page2->ValueAt(i));
  }
  page1->IncreaseSize(page2->GetSize());
  page1->SetNextPageId(page2->GetNextPageId());
  bpm_->DeletePage(right_page_id);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::MergeInternalNode(InternalPage *left_page, InternalPage *right_page, const KeyType &parent_key,
                                       page_id_t left_page_id, page_id_t right_page_id) {
  // std::cout << std::this_thread::get_id() << "Start merge internal node" << '\n';
  left_page->SetAt(left_page->GetSize(), parent_key, right_page->ValueAt(0));
  for (int i = 1; i < right_page->GetSize(); i++) {
    left_page->SetAt(left_page->GetSize() + i, right_page->KeyAt(i), right_page->ValueAt(i));
  }
  left_page->IncreaseSize(right_page->GetSize());
  for (int i = 0; i < right_page->GetSize(); i++) {
    WritePageGuard child_write_guard = bpm_->FetchPageWrite(right_page->ValueAt(i));
    auto child_page = child_write_guard.AsMut<BPlusTreePage>();
    child_page->SetParentPageId(left_page_id);
  }
  bpm_->DeletePage(right_page_id);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveParentReadLock(Context &ctx, page_id_t pos_page_id) {
  while (true) {
    if (ctx.read_set_.front().PageId() != pos_page_id) {
      ctx.read_set_.pop_front();
    } else {
      break;
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveParentWriteLock(Context &ctx, page_id_t pos_page_id) {
  while (true) {
    if (ctx.write_set_.front().PageId() != pos_page_id) {
      ctx.write_set_.pop_front();
    } else {
      break;
    }
  }
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool {
  page_id_t root_page_id = GetRootPageId();
  return root_page_id == INVALID_PAGE_ID;
}
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *txn) -> bool {
  // Declaration of context instance.
  Context ctx;
  (void)ctx;
  ctx.read_set_.emplace_back(bpm_->FetchPageRead(header_page_id_));
  page_id_t root_page_id = GetRootPageId();
  page_id_t pos_page_id = root_page_id;
  ctx.root_page_id_ = root_page_id;
  while (true) {  // Find the leafnode first
    ctx.read_set_.emplace_back(bpm_->FetchPageRead(pos_page_id));
    auto page = ctx.read_set_.back().As<BPlusTreePage>();
    if (!ctx.IsRootPage(pos_page_id)) {
      ctx.read_set_.pop_front();
    }
    if (page->IsLeafPage()) {
      const auto leaf_page = ctx.read_set_.back().As<LeafPage>();
      int idx = BinarySearch(key, leaf_page);
      if (idx == leaf_page->GetSize() || comparator_(key, leaf_page->KeyAt(idx))) {
        return false;
      }
      for (int i = idx; i < leaf_page->GetSize(); i++) {
        if (comparator_(leaf_page->KeyAt(i), key) == 0) {
          result->push_back(leaf_page->ValueAt(i));
        } else {
          break;
        }
      }
      return true;
    }
    const auto internal_page = ctx.read_set_.back().As<InternalPage>();
    int idx = BinarySearch(key, internal_page);
    pos_page_id = internal_page->ValueAt(idx - 1);
  }
  return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *txn) -> bool {
  // Declaration of context instance.
  Context ctx;
  (void)ctx;
  // std::cout << std::this_thread::get_id() << "Start insert!"
  //           << "Insert key:" << key << '\n';
  ctx.write_set_.emplace_back(bpm_->FetchPageWrite(header_page_id_));
  auto header_page = ctx.write_set_.back().AsMut<BPlusTreeHeaderPage>();
  page_id_t root_page_id = header_page->root_page_id_;
  ctx.root_page_id_ = root_page_id;
  if (root_page_id == INVALID_PAGE_ID) {
    // std::cout << "creat root page" << std::endl;
    BasicPageGuard basic_guard = bpm_->NewPageGuarded(&root_page_id);
    // Fetching a newpage as the rootpage failed
    auto leaf_page = basic_guard.AsMut<LeafPage>();
    leaf_page->Init(leaf_max_size_);
    auto header_page = ctx.write_set_.back().AsMut<BPlusTreeHeaderPage>();
    header_page->root_page_id_ = root_page_id;
  }
  // std::cout<<"i'm here"<<std::endl;
  page_id_t pos_page_id = root_page_id;
  page_id_t leaf_page_id = INVALID_PAGE_ID;
  while (true) {  // Find the leafnode first
    ctx.write_set_.emplace_back(bpm_->FetchPageWrite(pos_page_id));
    auto page = ctx.write_set_.back().As<BPlusTreePage>();
    // std::cout << std::this_thread::get_id() << "Start insert leaf searching!" << '\n';
    if (page->IsLeafPage()) {
      // std::cout << "is leaf page!" << '\n';
      // std::cout << "leaf page size:" << page->GetSize() << std::endl;
      leaf_page_id = ctx.write_set_.back().PageId();
      if (page->GetSize() < page->GetMaxSize()) {
        RemoveParentWriteLock(ctx, pos_page_id);
      }
      break;
    }
    // std::cout << std::this_thread::get_id() << "is internal page!" << '\n';
    auto internal_page = ctx.write_set_.back().AsMut<InternalPage>();
    // If pos page will not be splitted,
    // then all the later operations will not influnence parent pages, so we can unlock them from the ctx.
    if (internal_page->GetSize() < internal_page->GetMaxSize()) {
      RemoveParentWriteLock(ctx, pos_page_id);
    }
    int idx = BinarySearch(key, internal_page);
    pos_page_id = internal_page->ValueAt(idx - 1);
  }
  // std::cout << std::this_thread::get_id() << "Leaf Searching finished!" << '\n';
  auto leaf_page = ctx.write_set_.back().AsMut<LeafPage>();
  int insert_idx = BinarySearch(key, leaf_page);
  // std::cout << "leaf search finished" << std::endl;
  MappingType insert_value = MappingType(key, value);
  if (insert_idx < leaf_page->GetSize() && comparator_(leaf_page->KeyAt(insert_idx), key) == 0) {
    return false;  // Already have the same key
  }
  if (leaf_page->GetSize() < leaf_page->GetMaxSize()) {  // case 1:no need to split the leaf page
    for (int i = leaf_page->GetSize() - 1; i >= insert_idx; i--) {
      leaf_page->SetAt(i + 1, leaf_page->KeyAt(i), leaf_page->ValueAt(i));
    }
    leaf_page->SetAt(insert_idx, insert_value.first, insert_value.second);
    leaf_page->IncreaseSize(1);
  } else {
    // std::cout << std::this_thread::get_id() << "split leaf" << std::endl;
    KeyType new_key;
    page_id_t right_page_id = INVALID_PAGE_ID;
    SplitLeaf(insert_value, right_page_id, new_key, ctx);  // case 2 :split the leaf page
    // std::cout << std::this_thread::get_id() << "split leaf finished" << std::endl;
    ctx.write_set_.pop_back();
    InsertIntoInternal(new_key, leaf_page_id, right_page_id, ctx);
  }
  return true;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.< __val; }
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *txn) {
  // Declaration of context instance.
  Context ctx;
  (void)ctx;
  // std::cout << std::this_thread::get_id() << DrawBPlusTree();
  // std::cout << std::this_thread::get_id() << "Start Remove!"
  //           << "Removed key:" << key << '\n';

  ctx.write_set_.emplace_back(bpm_->FetchPageWrite(header_page_id_));
  const auto root_page = ctx.write_set_.back().As<BPlusTreeHeaderPage>();
  page_id_t root_page_id = root_page->root_page_id_;
  ctx.root_page_id_ = root_page_id;
  if (root_page_id == INVALID_PAGE_ID) {
    return;
  }
  // std::cout<<"i'm here"<<std::endl;
  page_id_t pos_page_id = root_page_id;
  page_id_t leaf_page_id = INVALID_PAGE_ID;
  while (true) {  // Find the leafnode first
    ctx.write_set_.emplace_back(bpm_->FetchPageWrite(pos_page_id));
    auto page = ctx.write_set_.back().AsMut<BPlusTreePage>();
    if (page->IsLeafPage()) {
      // std::cout << std::this_thread::get_id() << "is leaf page!" << '\n';
      // std::cout << "leaf page size:" << page->GetSize() << std::endl;
      leaf_page_id = pos_page_id;
      if (page->GetSize() > page->GetMinSize()) {
        RemoveParentWriteLock(ctx, pos_page_id);
      }
      break;
    }
    // std::cout << std::this_thread::get_id() << "is internal page!" << '\n';
    auto internal_page = ctx.write_set_.back().AsMut<InternalPage>();
    if (internal_page->GetSize() > internal_page->GetMinSize()) {
      RemoveParentWriteLock(ctx, pos_page_id);
    }
    int idx = BinarySearch(key, internal_page);
    pos_page_id = internal_page->ValueAt(idx - 1);
  }
  auto leaf_page = ctx.write_set_.back().AsMut<LeafPage>();
  // std::cout << "i'm here1!" << '\n';
  if (leaf_page->GetSize() > leaf_page->GetMinSize() ||
      leaf_page->GetParentPageId() == INVALID_PAGE_ID) {  // Just delete directly.
    // std::cout << "delete directly!" << '\n';
    int leaf_key_idx = -1;
    RemoveFromLeaf(key, leaf_key_idx, ctx);
    // if (has_removed && leaf_key_idx == 0 &&
    //     parent_page_id != INVALID_PAGE_ID) {  // replace the key in parent node with the new one.
    //   WritePageGuard write_guard = bpm_->FetchPageWrite(parent_page_id);
    //   InternalPage *internal_page = write_guard.AsMut<InternalPage>();
    //   read_guard = bpm_->FetchPageRead(leaf_page_id);
    //   leaf_page = read_guard.As<LeafPage>();
    //   int internal_key_idx = BinarySearch(key, internal_page);
    //   internal_page->SetAt(internal_key_idx - 1, leaf_page->KeyAt(0), internal_page->ValueAt(internal_key_idx -1));
    // }
  } else {
    // std::cout << "i'm here2!" << '\n';
    page_id_t pre_page_id = INVALID_PAGE_ID;
    page_id_t next_page_id = INVALID_PAGE_ID;
    page_id_t parent_page_id = leaf_page->GetParentPageId();
    const auto parent_page = ctx.write_set_[ctx.write_set_.size() - 2].As<InternalPage>();
    int parent_key_idx = BinarySearch(leaf_page->KeyAt(0), parent_page);
    if (parent_key_idx > 1) {
      pre_page_id = parent_page->ValueAt(parent_key_idx - 2);
    }
    if (parent_key_idx < parent_page->GetSize()) {
      next_page_id = parent_page->ValueAt(parent_key_idx);
    }
    bool has_removed = false;
    if (pre_page_id != INVALID_PAGE_ID) {  // adopt pessimistic lock here.
      // std::cout << "i'm here3!" << '\n';
      ctx.write_sibling_set_.push_front(bpm_->FetchPageWrite(pre_page_id));
      auto pre_leaf_page = ctx.write_sibling_set_.front().AsMut<LeafPage>();
      if (pre_leaf_page->GetSize() > pre_leaf_page->GetMinSize()) {
        // std::cout << std::this_thread::get_id() << "borrow from left bro!" << '\n';
        KeyType pre_key = pre_leaf_page->KeyAt(pre_leaf_page->GetSize() - 1);
        ValueType pre_value = pre_leaf_page->ValueAt(pre_leaf_page->GetSize() - 1);
        int key_idx = -1;
        has_removed = RemoveFromLeaf(key, key_idx, ctx);
        if (!has_removed) {
          return;
        }
        auto pos_leaf_page = ctx.write_set_.back().AsMut<LeafPage>();
        for (int i = pos_leaf_page->GetSize() - 1; i >= 0; i--) {
          pos_leaf_page->SetAt(i + 1, pos_leaf_page->KeyAt(i), pos_leaf_page->ValueAt(i));
        }
        pos_leaf_page->SetAt(0, pre_key, pre_value);
        pos_leaf_page->IncreaseSize(1);
        pre_leaf_page->IncreaseSize(-1);
        ctx.write_set_.pop_back();
        auto internal_page = ctx.write_set_.back().AsMut<InternalPage>();
        key_idx = BinarySearch(pre_key, internal_page);
        internal_page->SetAt(key_idx, pre_key, internal_page->ValueAt(key_idx));
      }
      ctx.write_sibling_set_.pop_front();
    }
    if (!has_removed && next_page_id != INVALID_PAGE_ID) {
      // std::cout << "i'm here4!" << '\n';
      ctx.write_sibling_set_.push_back(bpm_->FetchPageWrite(next_page_id));
      auto next_leaf_page = ctx.write_sibling_set_.back().AsMut<LeafPage>();
      if (next_leaf_page->GetSize() > next_leaf_page->GetMinSize()) {
        // std::cout << std::this_thread::get_id() << "borrow from right bro!" << '\n';
        KeyType key_temp = leaf_page->KeyAt(0);
        KeyType next_key = next_leaf_page->KeyAt(0);
        ValueType next_value = next_leaf_page->ValueAt(0);
        int key_idx = -1;
        has_removed = RemoveFromLeaf(key, key_idx, ctx);
        if (!has_removed) {
          return;
        }
        for (int i = 1; i < next_leaf_page->GetSize(); i++) {
          next_leaf_page->SetAt(i - 1, next_leaf_page->KeyAt(i), next_leaf_page->ValueAt(i));
        }
        next_leaf_page->IncreaseSize(-1);
        KeyType new_key = next_leaf_page->KeyAt(0);
        auto pos_leaf_page = ctx.write_set_.back().AsMut<LeafPage>();
        pos_leaf_page->SetAt(pos_leaf_page->GetMinSize() - 1, next_key, next_value);
        pos_leaf_page->IncreaseSize(1);
        ctx.write_set_.pop_back();
        auto internal_page = ctx.write_set_.back().AsMut<InternalPage>();
        key_idx = BinarySearch(key_temp, internal_page);
        internal_page->SetAt(key_idx, new_key, internal_page->ValueAt(key_idx));
      }
      ctx.write_sibling_set_.pop_back();
    }
    if (!has_removed && pre_page_id != INVALID_PAGE_ID) {
      ctx.write_sibling_set_.push_front(bpm_->FetchPageWrite(pre_page_id));
      auto pre_leaf_page = ctx.write_sibling_set_.front().AsMut<LeafPage>();
      if (pre_leaf_page->GetParentPageId() == parent_page_id) {
        // std::cout << std::this_thread::get_id() << "Fine,have to merge left bro!" << '\n';
        int key_idx = -1;
        has_removed = RemoveFromLeaf(key, key_idx, ctx);
        if (!has_removed) {
          return;
        }
        MergeLeafNode(pre_leaf_page, leaf_page, pre_page_id, leaf_page_id);

        auto pre_page = ctx.write_sibling_set_.front().AsMut<LeafPage>();
        ctx.write_set_.pop_back();
        ctx.write_sibling_set_.pop_front();
        auto parent_page = ctx.write_set_.back().AsMut<InternalPage>();
        key_idx = BinarySearch(pre_page->KeyAt(0), parent_page);
        RemoveFromInternal(key_idx, ctx);
      } else {
        ctx.write_sibling_set_.pop_front();
      }
    }
    if (!has_removed && next_page_id != INVALID_PAGE_ID) {
      // std::cout << std::this_thread::get_id() << "Fine,have to merge right bro!" << '\n';
      ctx.write_sibling_set_.emplace_back(bpm_->FetchPageWrite(next_page_id));
      auto right_page = ctx.write_sibling_set_.back().AsMut<LeafPage>();
      int key_idx = -1;
      KeyType leaf_key = leaf_page->KeyAt(0);
      has_removed = RemoveFromLeaf(key, key_idx, ctx);
      if (!has_removed) {
        return;
      }
      MergeLeafNode(leaf_page, right_page, leaf_page_id, next_page_id);
      ctx.write_set_.pop_back();
      auto parent_page = ctx.write_set_.back().AsMut<InternalPage>();
      key_idx = BinarySearch(leaf_key, parent_page);
      RemoveFromInternal(key_idx, ctx);
    }
  }
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  std::cout << "i'm here1" << '\n';
  page_id_t root_page_id = GetRootPageId();
  std::cout << root_page_id << '\n';
  page_id_t pos_page_id = root_page_id;
  page_id_t leaf_page_id = INVALID_PAGE_ID;
  while (true) {  // Find the leafnode first
    if (pos_page_id == INVALID_PAGE_ID) {
      break;
    }
    ReadPageGuard read_guard = bpm_->FetchPageRead(pos_page_id);
    const auto page = read_guard.As<BPlusTreePage>();
    if (page->IsLeafPage()) {
      leaf_page_id = read_guard.PageId();
      return INDEXITERATOR_TYPE(leaf_page_id, 0, bpm_);
    }
    const auto internal_page = read_guard.As<InternalPage>();
    pos_page_id = internal_page->ValueAt(0);
  }
  return INDEXITERATOR_TYPE(-1, 0, nullptr);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  page_id_t root_page_id = GetRootPageId();
  page_id_t pos_page_id = root_page_id;
  page_id_t leaf_page_id = INVALID_PAGE_ID;
  while (true) {  // Find the leafnode first
    ReadPageGuard read_guard = bpm_->FetchPageRead(pos_page_id);
    auto page = read_guard.As<BPlusTreePage>();
    // std::cout << "Start insert leaf searching!" << '\n';
    if (page->IsLeafPage()) {
      // std::cout << "is leaf page!" << '\n';
      // std::cout << "leaf page size:" << page->GetSize() << std::endl;
      leaf_page_id = read_guard.PageId();
      break;
    }
    // std::cout << "is internal page!" << '\n';
    auto internal_page = read_guard.As<InternalPage>();
    int idx = BinarySearch(key, internal_page);
    // std::cout << idx << '\n';
    pos_page_id = internal_page->ValueAt(idx - 1);
    // std::cout << "Leaf Searching finished!" << '\n';
  }
  ReadPageGuard read_guard = bpm_->FetchPageRead(leaf_page_id);
  const auto leaf_page = read_guard.As<LeafPage>();
  int key_index = BinarySearch(key, leaf_page);
  return INDEXITERATOR_TYPE(leaf_page_id, key_index, bpm_);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE {
  page_id_t root_page_id = GetRootPageId();
  page_id_t pos_page_id = root_page_id;
  page_id_t leaf_page_id = INVALID_PAGE_ID;
  int leaf_size = -1;
  while (true) {  // Find the leafnode first
    if (root_page_id == INVALID_PAGE_ID) {
      break;
    }
    ReadPageGuard read_guard = bpm_->FetchPageRead(pos_page_id);
    const auto page = read_guard.As<BPlusTreePage>();
    if (page->IsLeafPage()) {
      leaf_page_id = read_guard.PageId();
      leaf_size = page->GetSize();
      return INDEXITERATOR_TYPE(leaf_page_id, leaf_size, bpm_);
    }
    const auto internal_page = read_guard.As<InternalPage>();
    pos_page_id = internal_page->ValueAt(internal_page->GetSize() - 1);
  }
  return INDEXITERATOR_TYPE(-1, 0, nullptr);
}

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() const -> page_id_t {
  ReadPageGuard read_guard = bpm_->FetchPageRead(header_page_id_);
  auto root_page = read_guard.As<BPlusTreeHeaderPage>();
  return root_page->root_page_id_;
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, txn);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, txn);
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::BatchOpsFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  char instruction;
  std::ifstream input(file_name);
  while (input) {
    input >> instruction >> key;
    RID rid(key);
    KeyType index_key;
    index_key.SetFromInteger(key);
    switch (instruction) {
      case 'i':
        Insert(index_key, rid, txn);
        break;
      case 'd':
        Remove(index_key, txn);
        break;
      default:
        break;
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::PrintTree(page_id_t page_id, const BPlusTreePage *page) {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    std::cout << "Leaf Page: " << page_id << "\tNext: " << leaf->GetNextPageId() << std::endl;

    // Print the contents of the leaf page.
    std::cout << "Contents: ";
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i);
      if ((i + 1) < leaf->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;

  } else {
    auto *internal = reinterpret_cast<const InternalPage *>(page);
    std::cout << "Internal Page: " << page_id << std::endl;

    // Print the contents of the internal page.
    std::cout << "Contents: ";
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i);
      if ((i + 1) < internal->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      auto guard = bpm_->FetchPageBasic(internal->ValueAt(i));
      PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
    }
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Drawing an empty tree");
    return;
  }

  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  ToGraph(guard.PageId(), guard.template As<BPlusTreePage>(), out);
  out << "}" << std::endl;
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(page_id_t page_id, const BPlusTreePage *page, std::ofstream &out) {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    // Print node name
    out << leaf_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << page_id << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << page_id << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }
  } else {
    auto *inner = reinterpret_cast<const InternalPage *>(page);
    // Print node name
    out << internal_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_guard = bpm_->FetchPageBasic(inner->ValueAt(i));
      auto child_page = child_guard.template As<BPlusTreePage>();
      ToGraph(child_guard.PageId(), child_page, out);
      if (i > 0) {
        auto sibling_guard = bpm_->FetchPageBasic(inner->ValueAt(i - 1));
        auto sibling_page = sibling_guard.template As<BPlusTreePage>();
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_guard.PageId() << " " << internal_prefix
              << child_guard.PageId() << "};\n";
        }
      }
      out << internal_prefix << page_id << ":p" << child_guard.PageId() << " -> ";
      if (child_page->IsLeafPage()) {
        out << leaf_prefix << child_guard.PageId() << ";\n";
      } else {
        out << internal_prefix << child_guard.PageId() << ";\n";
      }
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::DrawBPlusTree() -> std::string {
  if (IsEmpty()) {
    return "()";
  }

  PrintableBPlusTree p_root = ToPrintableBPlusTree(GetRootPageId());
  std::ostringstream out_buf;
  p_root.Print(out_buf);

  return out_buf.str();
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::ToPrintableBPlusTree(page_id_t root_id) -> PrintableBPlusTree {
  auto root_page_guard = bpm_->FetchPageBasic(root_id);
  auto root_page = root_page_guard.template As<BPlusTreePage>();
  PrintableBPlusTree proot;

  if (root_page->IsLeafPage()) {
    auto leaf_page = root_page_guard.template As<LeafPage>();
    proot.keys_ = leaf_page->ToString();
    proot.size_ = proot.keys_.size() + 4;  // 4 more spaces for indent

    return proot;
  }

  // draw internal page
  auto internal_page = root_page_guard.template As<InternalPage>();
  proot.keys_ = internal_page->ToString();
  proot.size_ = 0;
  for (int i = 0; i < internal_page->GetSize(); i++) {
    page_id_t child_id = internal_page->ValueAt(i);
    PrintableBPlusTree child_node = ToPrintableBPlusTree(child_id);
    proot.size_ += child_node.size_;
    proot.children_.push_back(child_node);
  }

  return proot;
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;

template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
