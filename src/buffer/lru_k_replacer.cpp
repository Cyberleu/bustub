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
#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  bool HAS_EVICT = false;
  if (evictable_size == 0) return false;
  for (auto iter = first_q.begin(); iter != first_q.end(); iter++) {
    if (node_store_[*iter].is_evictable_) {
      *frame_id = node_store_[*iter].fid_;
      first_q.erase(iter);
      HAS_EVICT = true;
      break;
    }
  }
  if (!HAS_EVICT) {
    for (auto iter = second_q.begin(); iter != second_q.end(); iter++) {
      if (node_store_[*iter].is_evictable_) {
        *frame_id = node_store_[*iter].fid_;
        second_q.erase(iter);
        HAS_EVICT = true;
        break;
      }
    }
  }
  if (HAS_EVICT) {
    node_store_.erase(*frame_id);
    curr_size_--;
    evictable_size--;
  }
  return HAS_EVICT;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
  assert(frame_id < static_cast<int>(replacer_size_));                     //ҳid�����������С
  if (node_store_.find(frame_id) != node_store_.end()) {  //ҳ��������
    LRUKNode *node = &node_store_[frame_id];
    node->pinned_count++;
    if (node->pinned_count < k_) {  //��һ������ȫ��+inf������ֱ�ӱ�ɶ�β
      return;
    } else if (node->pinned_count > k_) {  //�ڶ��������ǰ����ʴ�������
      AdjustList(frame_id);
    } else {
      first_q.remove(frame_id);
      second_q.push_front(frame_id);
      AdjustList(frame_id);
    }
  } else {
    LRUKNode node = LRUKNode(k_, frame_id);
    node.pinned_count++;
    node_store_[frame_id] = node;
    first_q.push_back(frame_id);
    curr_size_++;
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  assert(frame_id < static_cast<int>(replacer_size_));
  if (node_store_.find(frame_id) == node_store_.end()) return;
  LRUKNode *node = &node_store_[frame_id];
  if (node->is_evictable_ && !set_evictable) {
    node->is_evictable_ = set_evictable;
    evictable_size--;
  } else if (!node->is_evictable_ && set_evictable) {
    node->is_evictable_ = set_evictable;
    evictable_size++;
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  assert(frame_id < static_cast<int>(replacer_size_));
  auto iter = node_store_.find(frame_id);
  if (iter == node_store_.end()) return;
  assert(iter->second.is_evictable_);
  curr_size_--;
  evictable_size--;
}

auto LRUKReplacer::Size() -> size_t { return evictable_size; }

auto LRUKReplacer::GetCurSize() -> size_t { return curr_size_; }

void LRUKReplacer::AdjustList(frame_id_t frame_id) {  //����frame_id�ڶ����е�λ��
  size_t pinned_count = node_store_[frame_id].pinned_count;
  std::list<frame_id_t>::iterator iter;
  const std::list<frame_id_t> *queue;
  if (pinned_count < k_) {  //˵���ڵ�һ������
    iter = first_q.begin();
    queue = &first_q;
  } else {
    iter = second_q.begin();
    queue = &second_q;
  }
  for (; iter != queue->end(); iter++) {
    if (*iter == frame_id) {
      while (std::next(iter, 1) != queue->end()) {
        frame_id_t next_frame_id = *next(iter, 1);
        if (node_store_[*iter].pinned_count >= node_store_[next_frame_id].pinned_count) {
          std::swap(*iter, *next(iter, 1));
          break;
        }
      }
    }
  }
}

}  // namespace bustub
