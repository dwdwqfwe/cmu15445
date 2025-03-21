//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_extendible_hash_table.cpp
//
// Identification: src/container/disk/hash/disk_extendible_hash_table.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstdint>
#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include <sys/types.h>
#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"
#include "common/rid.h"
#include "common/util/hash_util.h"
#include "container/disk/hash/disk_extendible_hash_table.h"
#include "storage/index/hash_comparator.h"
#include "storage/page/extendible_htable_bucket_page.h"
#include "storage/page/extendible_htable_directory_page.h"
#include "storage/page/extendible_htable_header_page.h"
#include "storage/page/page.h"
#include "storage/page/page_guard.h"
#include "type/type.h"

namespace bustub {

template <typename K, typename V, typename KC>
DiskExtendibleHashTable<K, V, KC>::DiskExtendibleHashTable(const std::string &name, BufferPoolManager *bpm,
                                                           const KC &cmp, const HashFunction<K> &hash_fn,
                                                           uint32_t header_max_depth, uint32_t directory_max_depth,
                                                           uint32_t bucket_max_size)
    : bpm_(bpm),
      cmp_(cmp),
      hash_fn_(std::move(hash_fn)),
      header_max_depth_(header_max_depth),
      directory_max_depth_(directory_max_depth),
      bucket_max_size_(bucket_max_size) {
  index_name_ = name;
  auto head_guard = bpm->NewPageGuarded(&header_page_id_);
  auto head_page = head_guard.AsMut<ExtendibleHTableHeaderPage>();
  head_page->Init(header_max_depth);
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::GetValue(const K &key, std::vector<V> *result, Transaction *transaction) const
    -> bool {
  uint32_t hash = Hash(key);
  auto header_page = bpm_->FetchPageRead(header_page_id_);
  auto htable_header = reinterpret_cast<const ExtendibleHTableHeaderPage *>(header_page.GetData());
  uint32_t dir_idx = htable_header->HashToDirectoryIndex(hash);
  page_id_t dir_page_id = htable_header->GetDirectoryPageId(dir_idx);
  if (dir_page_id == -1) {
    return false;
  }
  header_page.Drop();
  auto dir_page = bpm_->FetchPageRead(dir_page_id);
  auto htable_dir = reinterpret_cast<const ExtendibleHTableDirectoryPage *>(dir_page.GetData());
  uint32_t bucket_idx = htable_dir->HashToBucketIndex(hash);
  page_id_t bukcet_page_id = htable_dir->GetBucketPageId(bucket_idx);
  if (bukcet_page_id == -1) {
    return false;
  }
  dir_page.Drop();
  auto bucket_page = bpm_->FetchPageRead(bukcet_page_id);
  auto htable_bucket = reinterpret_cast<const ExtendibleHTableBucketPage<K, V, KC> *>(bucket_page.GetData());
  V res;
  if (htable_bucket->Lookup(key, res, cmp_)) {
    result->push_back(res);
    return true;
  }
  return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Insert(const K &key, const V &value, Transaction *transaction) -> bool {
  std::vector<V> temp;
  if (GetValue(key, &temp)) {
    return false;
  }
  auto header_guard = bpm_->FetchPageWrite(header_page_id_);
  auto header_page = header_guard.AsMut<ExtendibleHTableHeaderPage>();
  uint64_t cast_hash_num = Hash(key);
  uint32_t dir_page_index = header_page->HashToDirectoryIndex(cast_hash_num);
  page_id_t dir_page_id = header_page->GetDirectoryPageId(dir_page_index);
  if (dir_page_id == INVALID_PAGE_ID) {
    return InsertToNewDirectory(header_page, dir_page_index, cast_hash_num, key, value);
  }
  header_guard.Drop();

  WritePageGuard dir_page_guard = bpm_->FetchPageWrite(dir_page_id);
  auto dir_page = dir_page_guard.AsMut<ExtendibleHTableDirectoryPage>();
  uint32_t bucket_page_index = dir_page->HashToBucketIndex(cast_hash_num);
  page_id_t bucket_page_id = dir_page->GetBucketPageId(bucket_page_index);
  if (bucket_page_id == INVALID_PAGE_ID) {
    return InsertToNewBucket(dir_page, bucket_page_index, key, value);
  }

  WritePageGuard bucket_page_guard = bpm_->FetchPageWrite(bucket_page_id);
  auto *bucket_page = bucket_page_guard.template AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  if (bucket_page->Insert(key, value, cmp_)) {
    LOG_DEBUG("bucket_page_id %ud insert succeed", bucket_page_id);
    return true;
  }
  if (dir_page->GetLocalDepth(bucket_page_index) == dir_page->GetGlobalDepth()) {
    if (dir_page->GetGlobalDepth() == directory_max_depth_) {
      LOG_DEBUG("dir_depth reach the maxsize, insert false");
      return false;
    }
    dir_page->IncrGlobalDepth();
  }
  dir_page->IncrLocalDepth(bucket_page_index);
  dir_page->IncrLocalDepth(bucket_page_index + (1U << (dir_page->GetGlobalDepth() - 1)));

  if (!SplitHash(dir_page, bucket_page, bucket_page_index)) {
    LOG_DEBUG("split false in insert");
    return false;
  }
  bucket_page_guard.Drop();
  dir_page_guard.Drop();
  return Insert(key, value);
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::SplitHash(ExtendibleHTableDirectoryPage *dir_page,
                                                  ExtendibleHTableBucketPage<K, V, KC> *bucket_page,
                                                  uint32_t bucket_index) -> bool {
  page_id_t new_page_id;
  auto new_page_guard_basic = bpm_->NewPageGuarded(&new_page_id);
  if (new_page_id == INVALID_PAGE_ID) {
    LOG_DEBUG("get a invald id in spilithash");
    return false;
  }
  auto new_page_guard_write = new_page_guard_basic.UpgradeWrite();
  auto new_page = new_page_guard_write.template AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  new_page->Init(bucket_max_size_);
  uint32_t new_index = dir_page->GetSplitImageIndex(bucket_index);
  dir_page->SetBucketPageId(new_index, new_page_id);
  uint32_t size = bucket_page->Size();
  for (uint32_t i = 0; i < size; i++) {
    K key = bucket_page->KeyAt(i);
    V value = bucket_page->ValueAt(i);
    uint64_t hash_num = Hash(key);
    auto temp_index = dir_page->HashToBucketIndex(hash_num);
    if (temp_index != bucket_index) {
      new_page->Insert(key, value, cmp_);
      bucket_page->Remove(key, cmp_);
    }
  }
  return true;
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewDirectory(ExtendibleHTableHeaderPage *header, uint32_t directory_idx,
                                                             uint32_t hash, const K &key, const V &value) -> bool {
  page_id_t new_dir_id;
  auto dir_guard_basic = bpm_->NewPageGuarded(&new_dir_id);
  if (new_dir_id == INVALID_PAGE_ID) {
    return false;
  }
  auto dir_guard_write = dir_guard_basic.UpgradeWrite();
  auto dir_page = dir_guard_write.AsMut<ExtendibleHTableDirectoryPage>();
  dir_page->Init(directory_max_depth_);
  header->SetDirectoryPageId(directory_idx, new_dir_id);
  uint32_t bucket_index = dir_page->HashToBucketIndex(hash);
  return InsertToNewBucket(dir_page, bucket_index, key, value);
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewBucket(ExtendibleHTableDirectoryPage *directory, uint32_t bucket_idx,
                                                          const K &key, const V &value) -> bool {
  page_id_t bucket_page_id;
  auto bucket_guard_basic = bpm_->NewPageGuarded(&bucket_page_id);
  auto bucket_guard_write = bucket_guard_basic.UpgradeWrite();
  auto bucket_page = bucket_guard_write.template AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  bucket_page->Init(bucket_max_size_);
  directory->SetBucketPageId(bucket_idx, bucket_page_id);
  return bucket_page->Insert(key, value, cmp_);
}

template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::UpdateDirectoryMapping(ExtendibleHTableDirectoryPage *directory,
                                                               uint32_t new_bucket_idx, page_id_t new_bucket_page_id,
                                                               uint32_t new_local_depth, uint32_t local_depth_mask) {
  local_depth_mask = (1 << new_local_depth) - 1;
  directory->SetBucketPageId(new_bucket_idx, new_bucket_page_id);
  for (int i = 0; i < (1 << directory->GetGlobalDepth()); i++) {
    if (directory->GetLocalDepth(i) == (new_local_depth + 1) && (i & local_depth_mask) == new_bucket_idx) {
      directory->SetBucketPageId(i, new_bucket_page_id);
      directory->SetLocalDepth(i, new_local_depth);
    }
  }
}

template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::MigrateEntries(ExtendibleHTableBucketPage<K, V, KC> *old_bucket,
                                                       ExtendibleHTableBucketPage<K, V, KC> *new_bucket,
                                                       uint32_t new_bucket_idx, uint32_t local_depth_mask) {
  local_depth_mask++;
  for (uint32_t i = 0; i < old_bucket->Size(); i++) {
    auto entry = old_bucket->EntryAt(i);
    uint32_t hash = Hash(entry.first);
    if ((hash & local_depth_mask) != 0) {
      new_bucket->Insert(entry.first, entry.second, cmp_);
      old_bucket->RemoveAt(i);
    }
  }
}
/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Remove(const K &key, Transaction *transaction) -> bool {
  std::vector<V> temp;
  if (!GetValue(key, &temp)) {
    return false;
  }
  auto header_guard = bpm_->FetchPageWrite(header_page_id_);
  auto header_page = header_guard.AsMut<ExtendibleHTableHeaderPage>();
  auto cast_hash_num = Hash(key);
  auto dir_index = header_page->HashToDirectoryIndex(cast_hash_num);
  page_id_t dir_page_id = header_page->GetDirectoryPageId(dir_index);
  if (dir_page_id == INVALID_PAGE_ID) {
    return false;
  }
  header_guard.Drop();
  auto dir_guard = bpm_->FetchPageWrite(dir_page_id);
  auto dir_page = dir_guard.AsMut<ExtendibleHTableDirectoryPage>();
  auto bucket_index = dir_page->HashToBucketIndex(cast_hash_num);
  auto bucket_page_id = dir_page->GetBucketPageId(bucket_index);
  if (bucket_page_id == INVALID_PAGE_ID) {
    return false;
  }

  auto bucket_guard = bpm_->FetchPageWrite(bucket_page_id);
  auto *bucket_page = bucket_guard.template AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  auto remove_res = bucket_page->Remove(key, cmp_);
  bucket_guard.Drop();
  if (!remove_res) {
    return false;
  }
  uint32_t local_depth = dir_page->GetLocalDepth(bucket_index);
  // auto global_depth=dir_page->GetGlobalDepth();
  auto pre_page_id = bucket_page_id;
  auto pre_bucket_guard = bpm_->FetchPageRead(pre_page_id);
  auto pre_bucket_page = pre_bucket_guard.template As<ExtendibleHTableBucketPage<K, V, KC>>();
  auto pre_bucket_index = bucket_index;
  while (local_depth > 0) {
    auto local_mask = 1 << (local_depth - 1);
    auto next_bucket_index = pre_bucket_index ^ local_mask;
    auto next_page_id = dir_page->GetBucketPageId(next_bucket_index);
    auto next_bucket_guard = bpm_->FetchPageRead(next_page_id);
    auto next_bucket_page = next_bucket_guard.template As<ExtendibleHTableBucketPage<K, V, KC>>();
    if ((local_depth != dir_page->GetLocalDepth(next_bucket_index)) ||
        (!pre_bucket_page->IsEmpty() && !next_bucket_page->IsEmpty())) {
      break;
    }
    if (pre_bucket_page->IsEmpty()) {
      bpm_->DeletePage(pre_page_id);
      pre_bucket_page = next_bucket_page;
      pre_bucket_guard = std::move(next_bucket_guard);
      pre_page_id = next_page_id;
    } else {
      bpm_->DeletePage(next_page_id);
    }
    dir_page->DecrLocalDepth(pre_bucket_index);
    local_depth = dir_page->GetLocalDepth(pre_bucket_index);
    // uint32_t use_count=1<<(global_depth-local_depth);
    uint32_t update_mask = dir_page->GetLocalDepthMask(pre_bucket_index);
    auto update_index = pre_bucket_index & update_mask;
    UpdateDirectoryMapping(dir_page, update_index, pre_page_id, local_depth, 0);
    while (dir_page->CanShrink()) {
      dir_page->DecrGlobalDepth();
    }
  }
  return true;
}

template class DiskExtendibleHashTable<int, int, IntComparator>;
template class DiskExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class DiskExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class DiskExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class DiskExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class DiskExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
