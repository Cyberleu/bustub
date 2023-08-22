//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "buffer/buffer_pool_manager.h"
#include <iostream>

#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // TODO(students): remove this line after you have implemented the buffer pool manager
  // std::cout << "BUfferPoolManager Constructor!" << '\n';
  // std::cout << "replacer_k:" << replacer_k << '\n';
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);
  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  std::lock_guard<std::mutex> my_lock(latch_);
  // std::cout << "Start NewPage!" << '\n';
  if (free_list_.empty() && replacer_->Size() == 0) {
    return nullptr;
  }
  // all places are occupied and non-evictable
  frame_id_t frame_id = -1;
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
    replacer_->Evict(&frame_id);
    if (pages_[frame_id].is_dirty_) {
      FlushPage(pages_[frame_id].page_id_);  // if has been modified,flushing to the disk first.
    }
    page_id_t evict_page_id = pages_[frame_id].page_id_;  // remove the evicted page from page_table_,this must be done
                                                          // after the evcted page has been flushed to the disk!
    auto iter = page_table_.find(evict_page_id);
    page_table_.erase(iter);
  }
  page_table_[next_page_id_] = frame_id;
  pages_[frame_id].ResetMemory();
  pages_[frame_id].page_id_ = next_page_id_;
  pages_[frame_id].pin_count_ = 1;
  *page_id = next_page_id_;
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);
  next_page_id_++;
  // std::cout << "frame_id:" << frame_id << "page_id:" << *page_id << '\n';
  return &pages_[frame_id];
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  std::lock_guard<std::mutex> my_lock(latch_);
  // std::cout << "Start FetchPage!" << '\n';
  // std::cout << "page_id" << page_id << '\n';
  // Print();
  auto iter = page_table_.find(page_id);
  Page *page = nullptr;
  if (iter != page_table_.end()) {  // This page has already exsisted in the buffer pool
    frame_id_t frame_id = iter->second;
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    pages_[frame_id].pin_count_++;
    page = &pages_[frame_id];
  } else {
    if (free_list_.empty() && replacer_->Size() == 0) {
      return nullptr;
    }
    // all places are occupied and non-evictable
    frame_id_t frame_id = -1;
    if (!free_list_.empty()) {
      frame_id = free_list_.front();
      free_list_.pop_front();
    } else {
      replacer_->Evict(&frame_id);
      if (pages_[frame_id].is_dirty_) {
        FlushPage(pages_[frame_id].page_id_);
      }
      // if has been modified,flushing to the disk first.
      page_id_t evict_page_id = pages_[frame_id].page_id_;
      // remove the evicted page from page_table_,this must be
      // done after the evcted page has been flushed to the disk!
      auto iter = page_table_.find(evict_page_id);
      page_table_.erase(iter);
    }
    page_table_[page_id] = frame_id;
    pages_[frame_id].ResetMemory();
    disk_manager_->ReadPage(page_id, pages_[frame_id].GetData());
    pages_[frame_id].page_id_ = page_id;
    pages_[frame_id].pin_count_ = 1;
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    page = &pages_[frame_id];
  }

  return page;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  std::lock_guard<std::mutex> my_lock(latch_);
  // std::cout << "Start UnpinPage!" << '\n';
  // std::cout << "page_id:" << page_id << "is_dirty:" << is_dirty << '\n';
  auto iter = page_table_.find(page_id);
  if (iter == page_table_.end() || pages_[iter->second].pin_count_ == 0) {
    return false;
  }
  frame_id_t frame_id = iter->second;
  pages_[frame_id].pin_count_--;
  if (pages_[frame_id].pin_count_ == 0) {
    replacer_->SetEvictable(frame_id, true);
  }
  pages_[frame_id].is_dirty_ |= is_dirty;
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  // std::cout << "Start FlushPage!" << '\n';
  // std::cout << "page_id" << page_id << '\n';
  auto iter = page_table_.find(page_id);
  if (iter == page_table_.end()) {
    return false;
  }
  frame_id_t frame_id = iter->second;
  disk_manager_->WritePage(page_id, pages_[frame_id].data_);
  pages_[frame_id].is_dirty_ = false;
  return true;
}

void BufferPoolManager::FlushAllPages() {
  std::lock_guard<std::mutex> my_lock(latch_);
  for (auto &iter : page_table_) {
    FlushPage(iter.first);
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> my_lock(latch_);
  // std::cout << "Start DeletePage!" << '\n';
  // std::cout << "page_id:" << page_id << '\n';
  auto iter = page_table_.find(page_id);
  if (iter == page_table_.end()) {
    return true;
  }
  frame_id_t frame_id = page_table_[page_id];
  if (pages_[frame_id].pin_count_ != 0) {
    return false;
  }
  pages_[frame_id].ResetMemory();
  pages_[frame_id].page_id_ = INVALID_PAGE_ID;
  pages_[frame_id].pin_count_ = 0;
  pages_[frame_id].is_dirty_ = false;
  replacer_->Remove(frame_id);
  free_list_.push_back(frame_id);
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard {
  Page *page = FetchPage(page_id);
  if (page != nullptr) {
    return BasicPageGuard{this, page};
  }
  return BasicPageGuard{this, nullptr};
}

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  Page *page = FetchPage(page_id);
  if (page != nullptr) {
    page->RLatch();
    return ReadPageGuard{this, page};
  }
  return ReadPageGuard{this, nullptr};
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  Page *page = FetchPage(page_id);
  if (page != nullptr) {
    page->WLatch();
    return WritePageGuard{this, page};
  }
  return WritePageGuard{this, nullptr};
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard {
  Page *page = NewPage(page_id);
  return BasicPageGuard{this, page};
}

// void BufferPoolManager::Print(){
//   replacer_->Print();
//   for(auto & a : page_table_){
//     std::cout << "frame_id:" << a.second << "page_id:" << a.first << '\n';
//   }
// }

}  // namespace bustub
