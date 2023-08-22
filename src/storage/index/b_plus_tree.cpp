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
auto BPLUSTREE_TYPE::BinarySearch(const KeyType &key, const InternalPage *internal_page) -> int {  //upper_bound
  int left = 1, right = internal_page->GetSize();
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
auto BPLUSTREE_TYPE::BinarySearch(const KeyType &key, const LeafPage *leaf_page) -> int { //lower_bound
  int left = 0, right = leaf_page->GetSize();
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
void BPLUSTREE_TYPE::SplitLeaf(page_id_t leaf_page_id, MappingType insert_value, page_id_t &right_page_id,
                               KeyType &new_key) {
  std::cout << "Start Split Leaf!" << '\n';
  WritePageGuard write_guard = bpm_->FetchPageWrite(leaf_page_id);
  LeafPage* leaf_page = write_guard.AsMut<LeafPage>();
  int insert_idx = BinarySearch(insert_value.first, leaf_page);
  std::cout << "leaf insert index:" << insert_idx << std::endl;
  BasicPageGuard basic_guard = bpm_->NewPageGuarded(&right_page_id);
  LeafPage *right_page = basic_guard.AsMut<LeafPage>();
  right_page->Init(leaf_max_size_);
  for (int i = leaf_page->GetSize(); i > insert_idx; i--) {
    leaf_page->SetAt(i, leaf_page->KeyAt(i - 1), leaf_page->ValueAt(i - 1));
  }
  leaf_page->SetAt(insert_idx, insert_value.first, insert_value.second);
  int split_idx = (leaf_page->GetMaxSize() + 1) / 2;
  for (int i = split_idx; i <= leaf_page->GetMaxSize(); i++) {  // Copy data from original page to the new page
    right_page->SetAt(i - split_idx, leaf_page->KeyAt(i), leaf_page->ValueAt(i));
  }
  new_key = right_page->KeyAt(0);
  leaf_page->SetSize(split_idx);
  right_page->SetSize(leaf_page->GetMaxSize() - split_idx + 1);
  right_page->SetNextPageId(leaf_page->GetNextPageId());
  leaf_page->SetNextPageId(right_page_id);
  right_page->SetParentPageId(leaf_page->GetParentPageId());
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::SplitInternal(page_id_t internal_page_id, page_id_t &right_page_id, KeyType &new_key) {
  std::cout << "Start Split Internal!" << '\n';
  WritePageGuard write_guard = bpm_->FetchPageWrite(internal_page_id);
  InternalPage* internal_page = write_guard.AsMut<InternalPage>();
  int split_idx = internal_page->GetMaxSize() / 2 + 1;
  new_key = internal_page->KeyAt(split_idx);
  // std::cout << "split_idx" << split_idx << "new_key" << new_key << '\n';
  BasicPageGuard basic_guard = bpm_->NewPageGuarded(&right_page_id);
  InternalPage *right_page = basic_guard.AsMut<InternalPage>();
  right_page->Init(internal_max_size_);
  // std:: cout << "right_page_id:" << right_page_id << '\n';
  for (int i = split_idx ; i < internal_page->GetMaxSize() + 1; i++) {
    right_page->SetAt(i - split_idx, internal_page->KeyAt(i), internal_page->ValueAt(i));
  }
  internal_page->SetSize(split_idx);
  right_page->SetSize(internal_page->GetMaxSize() - split_idx + 1);
  for (int i = 0; i < right_page->GetSize(); i++) {  // Reset the parent page id of each child node.
    WritePageGuard write_guard = bpm_->FetchPageWrite(right_page->ValueAt(i));
    BPlusTreePage *tree_page = write_guard.AsMut<BPlusTreePage>();
    tree_page->SetParentPageId(right_page_id);
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoInternal(KeyType key, page_id_t internal_page_id, page_id_t left_page_id,
                                        page_id_t right_page_id) {
  std::cout << "start insert into internal" << std::endl;
  std::cout << internal_page_id << std::endl;
  if (internal_page_id == INVALID_PAGE_ID) {  // The internal page not exsisted
    std::cout << "The internal page not exsisted" << std::endl;
    page_id_t root_page_id = INVALID_PAGE_ID;
    BasicPageGuard basic_guard = bpm_->NewPageGuarded(&root_page_id);
    // std::cout << "root_page_id:" << root_page_id << '\n';
    InternalPage *root_page = basic_guard.AsMut<InternalPage>();
    root_page->Init(internal_max_size_);
    root_page->SetAt(0, key, left_page_id);
    root_page->SetAt(1, key, right_page_id);
    root_page->SetSize(2);
    WritePageGuard write_guard = bpm_->FetchPageWrite(header_page_id_);
    // std:: cout << "header_page_id:" << header_page_id_ << '\n';
    BPlusTreeHeaderPage *header_page = write_guard.AsMut<BPlusTreeHeaderPage>();
    header_page->root_page_id_ = root_page_id;
    // std:: cout << "left_page_id:" << left_page_id << '\n';
    write_guard = bpm_->FetchPageWrite(left_page_id);
    BPlusTreePage *tree_page = write_guard.AsMut<BPlusTreePage>();
    tree_page->SetParentPageId(root_page_id);
    write_guard = bpm_->FetchPageWrite(right_page_id);
    tree_page = write_guard.AsMut<BPlusTreePage>();
    tree_page->SetParentPageId(root_page_id);
    return;
  }
  WritePageGuard write_guard = bpm_->FetchPageWrite(internal_page_id);
  InternalPage *internal_page = write_guard.AsMut<InternalPage>();
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
  write_guard = bpm_->FetchPageWrite(right_page_id);
  BPlusTreePage *tree_page = write_guard.AsMut<BPlusTreePage>();  // Set child node's parent page id
  tree_page->SetParentPageId(internal_page_id);
  right_page_id = INVALID_PAGE_ID;
  KeyType new_key;
  write_guard.Drop();
  if (internal_page->GetSize() > internal_page->GetMaxSize()) {  // The internal page is full
    std::cout << "The internal page is full" << '\n';
    SplitInternal(internal_page_id, right_page_id, new_key);
    InsertIntoInternal(new_key, internal_page->GetParentPageId(), internal_page_id, right_page_id);
  }
}
/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool {
  page_id_t root_page_id = GetRootPageId();
  if (root_page_id == INVALID_PAGE_ID) return true;
  return false;
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
  page_id_t root_page_id = GetRootPageId();
  page_id_t pos_page_id = root_page_id;
  while (1) {  // Find the leafnode first
    ReadPageGuard read_guard = bpm_->FetchPageRead(pos_page_id);
    auto page = read_guard.As<BPlusTreePage>();
    if (page->IsLeafPage()) {
      const LeafPage *leaf_page = read_guard.As<LeafPage>();
      int idx = BinarySearch(key, leaf_page);
      if (idx == leaf_page->GetSize() || comparator_(key, leaf_page->KeyAt(idx))) {
        return false;
      } else {
        for (int i = idx; i < leaf_page->GetSize(); i++) {
          if (comparator_(leaf_page->KeyAt(i), key) == 0) {
            result->push_back(leaf_page->ValueAt(i));
          } else {
            break;
          }
        }
        return true;
      }
      break;
    } else {
      const InternalPage *internal_page = read_guard.As<InternalPage>();
      int idx = BinarySearch(key, internal_page);
      pos_page_id = internal_page->ValueAt(idx - 1);
    }
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
  page_id_t root_page_id = GetRootPageId();
  if (root_page_id == INVALID_PAGE_ID) {
    std::cout << "creat root page" << std::endl;
    BasicPageGuard basic_guard = bpm_->NewPageGuarded(&root_page_id);
    // Fetching a newpage as the rootpage failed
    auto leaf_page = basic_guard.AsMut<LeafPage>();
    leaf_page->Init(leaf_max_size_);
    WritePageGuard write_guard = bpm_->FetchPageWrite(header_page_id_);
    auto header_page = write_guard.AsMut<BPlusTreeHeaderPage>();
    header_page->root_page_id_ = root_page_id;
  }
  // std::cout<<"i'm here"<<std::endl;
  page_id_t pos_page_id = root_page_id;
  page_id_t leaf_page_id = INVALID_PAGE_ID;
  while (1) {  // Find the leafnode first
    ReadPageGuard read_guard = bpm_->FetchPageRead(pos_page_id);
    auto page = read_guard.As<BPlusTreePage>();
    if (page->IsLeafPage()) {
      std::cout << "is leaf page!" << '\n';
      std::cout << "leaf page size:" << page->GetSize() << std::endl;
      leaf_page_id = read_guard.PageId();
      break;
    } else {
      std::cout << "is internal page!" << '\n';
      auto internal_page = read_guard.As<InternalPage>();
      int idx = BinarySearch(key, internal_page);
      std::cout <<idx << '\n';
      pos_page_id = internal_page->ValueAt(idx-1);
    }
  }
  WritePageGuard leaf_guard = bpm_->FetchPageWrite(leaf_page_id);
  LeafPage *leaf_page = leaf_guard.AsMut<LeafPage>();
  int insert_idx = BinarySearch(key, leaf_page);
  std::cout << "leaf search finished" << std::endl;
  MappingType insert_value = MappingType(key, value);
  if (insert_idx < leaf_page->GetSize() && comparator_(leaf_page->KeyAt(insert_idx), key) == 0)
    return false;                                        // Already have the same key
  if (leaf_page->GetSize() < leaf_page->GetMaxSize()) {  // case 1:no need to split the leaf page
    for (int i = leaf_page->GetSize() - 1; i >= insert_idx; i--) {
      leaf_page->SetAt(i + 1, leaf_page->KeyAt(i), leaf_page->ValueAt(i));
    }
    leaf_page->SetAt(insert_idx, insert_value.first, insert_value.second);
    leaf_page->IncreaseSize(1);
  } else {
    std::cout << "split leaf" << std::endl;
    KeyType new_key;
    page_id_t right_page_id = INVALID_PAGE_ID;
    leaf_guard.Drop();
    SplitLeaf(leaf_page_id, insert_value, right_page_id, new_key);  // case 2 :split the leaf page
    std::cout << "split leaf finished" << std::endl;
    InsertIntoInternal(new_key, leaf_page->GetParentPageId(), pos_page_id, right_page_id);
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
  // if(IsEmpty()) return;
  // page_id_t root_page_id = GetRootPageId();
  // if(root_page_id == INVALID_PAGE_ID){
  //   BasicPageGuard basic_guard = bpm_->NewPageGuarded(&root_page_id);
  //     //Fetching a newpage as the rootpage failed
  //   auto leaf_page = basic_guard.AsMut<LeafPage>();
  //   leaf_page->Init(leaf_max_size_);
  //   WritePageGuard write_guard = bpm_->FetchPageWrite(header_page_id_);
  //   auto header_page = write_guard.AsMut<BPlusTreeHeaderPage>();
  //   header_page->root_page_id_ = root_page_id;
  // }
  // page_id_t pos_page_id = root_page_id;
  // page_id_t leaf_page_id = INVALID_PAGE_ID;
  // while(1){    //Find the leafnode first
  //   ReadPageGuard read_guard = bpm_->FetchPageRead(leaf_page_id);
  //   auto page = read_guard.As<BPlusTreePage>();
  //   if(page->IsLeafPage()) {
  //     leaf_page_id = read_guard.PageId();
  //     break;
  //   }
  //   else{
  //     auto internal_page = read_guard.As<InternalPage>();
  //     int idx = BinarySearch(key,internal_page);
  //     pos_page_id = internal_page->ValueAt(idx);
  //   }
  // }
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
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() const -> page_id_t {
  ReadPageGuard read_guard = bpm_->FetchPageRead(HEADER_PAGE_ID);
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
