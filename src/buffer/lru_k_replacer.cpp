//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "buffer/lru_k_replacer.h"
#include <iostream>
#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {
  // std::cout << "LRUKReplacer Constructor!" << '\n';
  // std::cout << "replacer_size_:" << replacer_size_ << "k_:" << k_ << '\n';
  head1_ = new LRUKNode();
  head2_ = new LRUKNode();
  tail1_ = new LRUKNode();
  tail2_ = new LRUKNode();
  head1_->next_ = tail1_;
  tail1_->pre_ = head1_;
  head2_->next_ = tail2_;
  tail2_->pre_ = head2_;
}

LRUKReplacer::~LRUKReplacer() {
  // std::cout << "deconstrctor called";
  for (auto &[k, v] : node_store_) {
    delete (v);
  }
  delete head1_;
  delete head2_;
  delete tail1_;
  delete tail2_;
}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::lock_guard<std::mutex> my_lock(latch_);
  // std::cout << "Start Eivict!" << '\n';
  // Print();
  bool has_evict = false;
  LRUKNode *evict_node = nullptr;
  for (auto p = head1_->next_; p != tail1_; p = p->next_) {
    if (p->is_evictable_) {
      *frame_id = p->fid_;
      has_evict = true;
      evict_node = p;
      break;
    }
  }
  if (!has_evict) {
    for (auto p = head2_->next_; p != tail2_; p = p->next_) {
      if (p->is_evictable_) {
        *frame_id = p->fid_;
        has_evict = true;
        evict_node = p;
        break;
      }
    }
  }
  if (has_evict) {
    RemoveNode(evict_node);
    node_store_.erase(*frame_id);
    delete evict_node;
    evictable_size_--;
  }
  // std::cout << "has_evict:" << has_evict << "frame_id:" << *frame_id << '\n';
  // std::cout << "cursize:" << curr_size_ << "evictable size:" << evictable_size_ << '\n';
  return has_evict;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  std::lock_guard<std::mutex> my_lock(latch_);
  // std::cout << "i'm here2" << '\n';
  // std::cout << "Start RecordAccess!" << '\n';
  // Print();
  // std::cout << "frame_id:" << frame_id << '\n';
  assert(frame_id < static_cast<int>(replacer_size_));
  auto iter = node_store_.find(frame_id);
  if (iter != node_store_.end()) {
    LRUKNode *node = iter->second;
    node->history_.push_back(current_timestamp_++);
    node->pinned_count_++;
    if (node->pinned_count_ == k_) {
      RemoveNode(node);
      InsertNode(tail2_->pre_, node);
    } else if (node->pinned_count_ > k_) {
      node->history_.pop_front();
      RemoveNode(node);
      LRUKNode *p = head2_->next_;
      for (; p != tail2_; p = p->next_) {
        if (node->history_.front() < p->history_.front()) {
          break;
        }
      }
      InsertNode(p->pre_, node);
    }
  } else {
    auto node = new LRUKNode(k_, frame_id);
    node->history_.push_back(current_timestamp_++);
    node->pinned_count_++;
    InsertNode(tail1_->pre_, node);
    node_store_[frame_id] = node;
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> my_lock(latch_);
  // std::cout << "Start SetEivictable!" << '\n';
  // Print();
  // std::cout << "frame_id:" << frame_id << "set_evictable:" << set_evictable << '\n';
  assert(frame_id < static_cast<int>(replacer_size_));
  if (node_store_.find(frame_id) == node_store_.end()) {
    return;
  }
  LRUKNode *node = node_store_[frame_id];
  if (node->is_evictable_ && !set_evictable) {
    node->is_evictable_ = set_evictable;
    evictable_size_--;
  } else if (!node->is_evictable_ && set_evictable) {
    node->is_evictable_ = set_evictable;
    evictable_size_++;
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> my_lock(latch_);
  // std::cout << "Start Remove!" << '\n';
  // std::cout << "frame_id:" << frame_id << '\n';
  assert(frame_id < static_cast<int>(replacer_size_));
  auto iter = node_store_.find(frame_id);
  LRUKNode *node = nullptr;
  if (iter != node_store_.end()) {
    node = iter->second;
    RemoveNode(node);
    node_store_.erase(frame_id);
    if (node->is_evictable_) {
      evictable_size_--;
    }
    delete node;
  }
}

auto LRUKReplacer::Size() -> size_t {
  std::lock_guard<std::mutex> my_lock(latch_);
  return evictable_size_;
}

// void LRUKReplacer::Print() {
// std::cout << "evictable frame:" << evictable_size_ << '\n';
// std::cout << "total frame ids:";
// for (auto &node : node_store_) {
//   std::cout << node.first << ' ';
// }
// std::cout << '\n';
// std::cout << "evictable frame ids:";
// for (auto &node : node_store_) {
//   if (node.second->is_evictable_) {
//     std::cout << node.first << ' ';
//   }
// }
// std::cout << '\n';
//   std::cout<<"list1:";
//   for(auto p = head1->next_;p!=tail1;p = p->next_){
//     std::cout << p->fid_ << ' ';
//   }
//   std::cout << '\n';
//     std::cout<<"list2:";
//   for(auto p = head2->next_;p!=tail2;p = p->next_){
//     std::cout << p->fid_ << ' '<<p->history_.front()<<' ';
//   }
//   std::cout << '\n';
// }

// void LRUKReplacer::AdjustList(frame_id_t frame_id) {
//   std::list<frame_id_t>::iterator iter;
//   const std::list<frame_id_t> *queue;
//   iter = second_q_.begin();
//   queue = &second_q_;
//   while (std::next(iter, 1) != queue->end()) {
//     frame_id_t next_frame_id = *next(iter, 1);
//     if (node_store_[*iter].history_.front() >= node_store_[next_frame_id].history_.front()) {
//       std::swap(*iter, *next(iter, 1));
//       iter++;
//     } else {
//       break;
//     }
//   }
// }

void LRUKReplacer::RemoveNode(const LRUKNode *node) {
  node->pre_->next_ = node->next_;
  node->next_->pre_ = node->pre_;
}

void LRUKReplacer::InsertNode(LRUKNode *left, LRUKNode *right) {
  right->next_ = left->next_;
  right->pre_ = left;
  left->next_ = right;
  right->next_->pre_ = right;
}

}  // namespace bustub
