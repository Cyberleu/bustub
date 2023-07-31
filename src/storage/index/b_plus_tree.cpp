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
auto BPLUSTREE_TYPE::key_cmp(const MappingType & left,const MappingType & right) -> bool{
  return comparator_(left.first,right.first) == -1;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::BinarySearch(const KeyType &key,const InternalPage * internal_page) -> int{
  auto p = std::upper_bound( internal_page->array_+1,internal_page->array_+internal_page->GetSize(),key);
  int offset = p-(internal_page->array_+1);
  return offset;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::BinarySearch(const KeyType &key,const LeafPage * leaf_page) -> int{
  auto p = std::lower_bound(leaf_page->array_,leaf_page->array_+leaf_page->GetSize(),key);
  int offset = p-leaf_page->array_;
  return offset;
}
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::SplitLeaf(LeafPage * leaf_page,int insert_idx,MappingType insert_value,page_id_t &left_page_id,page_id_t &right_page_id){
  int split_point = (leaf_page->GetSize()+1)/2;
  WritePageGuard left_guard = bpm_->FetchPageWrite(&left_page_id);
  LeafPage * left_page = left_guard.AsMut<LeafPage>();
  left_page->Init(leaf_max_size_);
  WritePageGuard right_guard = bpm_->FetchPageWrite(&right_page_id);
  LeafPage * right_page = right_guard.AsMut<LeafPage>();
  right_page->Init(leaf_max_size_);
  bool flag = true;
  int idx = 0;         //Point to the index of the array
  int count = 0;       
  while(true){          //Divide the leaf page into two pages
    LeafPage * insert_page = nullptr;
    int next_idx = -1;
    if(count<split_point) {
      insert_page = left_page;
      next_idx = count;
    }
    else {
      insert_page = right_page;
      next_idx = count-split_point;
    }
    if(flag && idx == insert_idx) {
      insert_page->SetAt(next_idx,insert_value);
      flag = false;
    }
    else{
      insert_page->SetAt(next_idx,leaf_page->array_[idx]);
      idx++;
    }
    count++;
    if(count == leaf_page->GetSize()+1) break;
  }
  left_page->SetSize(split_point);
  right_page->SetSize(count-split_point);
  left_page->SetNextPageId(right_guard.PageId());
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoInternal(KeyType key,page_id_t internal_page_id,page_id_t left_page_id,page_id_t right_page_id){
  WritePageGuard write_guard = bpm_->FetchPageWrite(internal_page_id);
  InternalPage * internal_page = write_guard.AsMut<InternalPage>();
  if(internal_page->GetSize()<internal_page->GetMaxSize()){     //Case 1:the internal page is not full
    int insert_idx = BinarySearch(key,internal_page);
    for(int i = internal_page->GetSize()-1;i>=insert_idx;i--){  //Remove the value before insertion
      internal_page->array_[i] = internal_page->array_[i-1];
    }
    internal_page->array_[insert_idx].first = key;              //Update the page_id of splitted pages
    internal_page->array_[insert_idx-1].second = left_page_id;
    internal_page->array_[insert_idx].second = right_page_id;
  }
  else{

  }
}
/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { 
  page_id_t root_page_id = GetRootPageId();
  if(root_page_id == INVALID_PAGE_ID) return true;
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
  page_id_t leaf_page_id = INVALID_PAGE_ID;
  while(1){    //Find the leafnode first
    ReadPageGuard read_guard = bpm_->FetchPageRead(pos_page_id);
    auto page = read_guard.As<BPlusTreePage>();
    if(page->IsLeafPage()) {
      leaf_page_id = read_guard.PageId();
      LeafPage * leaf_page = read_guard<BPlusTreeLeafPage>();
      int idx = BinarySearch(key,leaf_page);
      if(idx == leaf_page->GetSize() || key != leaf_page->KeyAt(idx)) return false;
      else{
        for(int i = idx;i<leaf_page->GetSize();i++){
          if(comparator_(leaf_page->KeyAt(i),key) == 0) result->push_back(leaf_page->array_[i].second);
          else break;
        }
        return true;
      }
      break;
    }
    else{
      InternalPage * internal_page = read_guard.As<BPlusTreeInternalPage>();
      int idx = BinarySearch(key,internal_page);
      pos_page_id = internal_page->ValueAt(idx-1);
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
  if(root_page_id == INVALID_PAGE_ID){
    BasicPageGuard basic_guard = bpm_->NewPageGuarded(&root_page_id); 
    if(basic_guard.page_ == nullptr) return false;    //Fetching a newpage as the rootpage failed
    auto leaf_page = basic_guard.AsMut<BPlusTreeLeafPage>();
    leaf_page->Init(leaf_max_size_);
    WritePageGuard write_guard = bpm_->FetchPageWrite(header_page_id_);
    auto header_page = write_guard.AsMut<BPlusTreeHeaderPage>();
    header_page->root_page_id_ = root_page_id;
  }
  page_id_t pos_page_id = root_page_id;
  page_id_t leaf_page_id = INVALID_PAGE_ID;
  while(1){    //Find the leafnode first
    ReadPageGuard read_guard = bpm_->FetchPageRead(pos_page_id);
    auto page = read_guard.As<BPlusTreePage>();
    if(page->IsLeafPage()) {
      leaf_page_id = read_guard.PageId();
      break;
    }
    else{
      InternalPage * internal_page = read_guard.As<BPlusTreeInternalPage>();
      int idx = BinarySearch(key,internal_page);
      pos_page_id = internal_page->ValueAt(idx);
    }
  }
  WritePageGuard leaf_guard = bpm_->FetchPageWrite(root_page_id);
  LeafPage* leaf_page = leaf_guard.AsMut<LeafPage>();
  int insert_idx = BinarySearch(key,leaf_page);
  if(insert_idx < leaf_page->GetSize() && leaf_page->KeyAt(insert_idx) == key) return false;   //Already have the same key 
  if(leaf_page->GetSize() < leaf_page->GetMaxSize()){   //case 1:no need to split the leaf page
    for(int i = leaf_page->GetSize()-1;i>=insert_idx;i--){
      leaf_page->SetAt(i+1,array_[i]);
    }
    leaf_page->SetAt(insert_idx,MappingType(key,value));
    leaf_page->IncreaseSize();
  }
  else{                                                    //case 2 :split the leaf page
    page_id_t left_page_id = INVALID_PAGE_ID;
    page_id_t right_page_id = INVALID_PAGE_ID;
    WritePageGuard left_guard = bpm_->FetchPageWrite(&left_page_id);
    LeafPage * left_page = left_guard.AsMut<LeafPage>();
    left_page->Init(leaf_max_size_);
    WritePageGuard right_guard = bpm_->FetchPageWrite(&right_page_id);
    LeafPage * right_page = right_guard.AsMut<LeafPage>();
    right_page->Init(leaf_max_size_);
    int split_point = (leaf_page->GetSize()+1)/2;
    bool flag = true;
    int idx = 0;         //Point to the index of the array
    int count = 0;       
    while(true){          //Divide the leaf page into two pages
      LeafPage * insert_page = nullptr;
      int next_idx = -1;
      if(count<split_point) {
        insert_page = left_page;
        next_idx = count;
      }
      else {
        insert_page = right_page;
        next_idx = count-split_point;
      }
      if(flag && idx == insert_idx) {
        insert_page->SetAt(next_idx,MappingType(key,value));
        flag = false;
      }
      else{
        insert_page->SetAt(next_idx,leaf_page->array_[idx]);
        idx++;
      }
      count++;
      if(count == leaf_page->GetSize()+1) break;
    }
    left_page->SetSize(split_point);
    right_page->SetSize(count-split_point);
    right_page->SetNextPageId(leaf_page->GetNextPageId());
    left_page->SetNextPageId(right_guard.PageId());
    leaf_guard.Drop();
    bpm_->DeletePage(leaf_page_id);
    KeyType insert_key = right_page->KeyAt(0);
    
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
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *txn) {
  // Declaration of context instance.
  Context ctx;
  (void)ctx;
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
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t {
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
