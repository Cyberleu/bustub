//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_table_bucket_page.cpp
//
// Identification: src/storage/page/hash_table_bucket_page.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/hash_table_bucket_page.h"
#include "common/logger.h"
#include "common/util/hash_util.h"
#include "storage/index/generic_key.h"
#include "storage/index/hash_comparator.h"
#include "storage/table/tmp_tuple.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::GetValue(KeyType key, KeyComparator cmp, std::vector<ValueType> *result) -> bool {
  bool flag = false;
  for (uint32_t i = 0; i < BUCKET_ARRAY_SIZE; i++) {
    if (IsReadable(i) && cmp(key, this->array_[i].first) == 0){
	  result->push_back(this->array_[i].second);
      flag = true;
	}
  }
	return flag;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::Insert(KeyType key, ValueType value, KeyComparator cmp) -> bool {
  uint32_t idx = -1;
  for (uint32_t i = 0; i < BUCKET_ARRAY_SIZE; i++) {
    if (IsReadable(i) && !cmp(key, KeyAt(i)) && value == ValueAt(i)) {  //�������ͬ��key-value�ԣ���ֱ������
      idx = -1;
      break; 
	}
    else if (!IsReadable(i)) { 
	  idx = i;
    break;
	}
  }
  if (static_cast<int>(idx) != -1) {
    this->array_[idx] = MappingType(key, value);
    SetOccupied(idx);
    SetReadable(idx);
    return true;
  }
	return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::Remove(KeyType key, ValueType value, KeyComparator cmp) -> bool {
  for (uint32_t i = 0; i < BUCKET_ARRAY_SIZE; i++) {
    if (IsReadable(i) && !cmp(key, this->array_[i].first) && value == this->array_[i].second) {
      RemoveAt(i);
      return true;
	}
  }
  return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::KeyAt(uint32_t bucket_idx) const -> KeyType {
  return {this->array_[bucket_idx].first};
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::ValueAt(uint32_t bucket_idx) const -> ValueType {
  return {this->array_[bucket_idx].second};
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::RemoveAt(uint32_t bucket_idx) {
  uint32_t group = bucket_idx / (8 * sizeof(char));
  uint32_t offset = bucket_idx % (8 * sizeof(char));
  uint8_t mask = 1 << (7 - offset);
  uint8_t pos = mask ^ static_cast<uint8_t>(this->readable_[group]);//�����벢����������1��0
  this->readable_[group] = static_cast<char>(pos);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::IsOccupied(uint32_t bucket_idx) const -> bool {
  uint32_t group = bucket_idx / (8 * sizeof(char));
  uint32_t offset = bucket_idx % (8 * sizeof(char));
  uint8_t mask = 1 << (7 - offset);
  return (static_cast<uint8_t>(this->occupied_[group]) & mask) > 0;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetOccupied(uint32_t bucket_idx) {
  uint32_t group = bucket_idx / (8 * sizeof(char));
  uint32_t offset = bucket_idx % (8 * sizeof(char));
  uint8_t mask = 1 << (7 - offset);
  uint8_t update = mask | static_cast<uint8_t>(this->occupied_[group]);
  this->occupied_[group] = static_cast<char>(update);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::IsReadable(uint32_t bucket_idx) const -> bool {
  uint32_t group = bucket_idx / (8 * sizeof(char));
  uint32_t offset = bucket_idx % (8 * sizeof(char));
  uint8_t mask = 1 << (7 - offset);
  return (static_cast<uint8_t>(this->readable_[group]) & mask) > 0;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetReadable(uint32_t bucket_idx) {
  uint32_t group = bucket_idx / (8 * sizeof(char));
  uint32_t offset = bucket_idx % (8 * sizeof(char));
  uint8_t mask = 1 << (7 - offset);
  uint8_t update = mask | static_cast<uint8_t>(this->readable_[group]);
  this->readable_[group] = static_cast<char>(update);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::IsFull() -> bool {
  for (uint32_t i = 0; i < BUCKET_ARRAY_SIZE; i++) {
    if (!IsReadable(i)) return false;
  }
	return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::NumReadable() -> uint32_t {
  uint32_t count = 0;
  for (uint32_t i = 0; i < BUCKET_ARRAY_SIZE; i++) {
    if (!IsOccupied(i)) break;
    else {
      if (IsReadable(i)) count++;    
	}
  }
	return count;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_BUCKET_TYPE::IsEmpty() -> bool {
  return !IsOccupied(0);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::PrintBucket() {
  uint32_t size = 0;
  uint32_t taken = 0;
  uint32_t free = 0;
  for (size_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; bucket_idx++) {
    if (!IsOccupied(bucket_idx)) {
      break;
    }

    size++;

    if (IsReadable(bucket_idx)) {
      taken++;
    } else {
      free++;
    }
  }

  LOG_INFO("Bucket Capacity: %lu, Size: %u, Taken: %u, Free: %u", BUCKET_ARRAY_SIZE, size, taken, free);
}

// DO NOT REMOVE ANYTHING BELOW THIS LINE
template class HashTableBucketPage<int, int, IntComparator>;

template class HashTableBucketPage<GenericKey<4>, RID, GenericComparator<4>>;
template class HashTableBucketPage<GenericKey<8>, RID, GenericComparator<8>>;
template class HashTableBucketPage<GenericKey<16>, RID, GenericComparator<16>>;
template class HashTableBucketPage<GenericKey<32>, RID, GenericComparator<32>>;
template class HashTableBucketPage<GenericKey<64>, RID, GenericComparator<64>>;

// template class HashTableBucketPage<hash_t, TmpTuple, HashComparator>;

}  // namespace bustub
