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
#include <cstddef>
#include <memory>
#include <mutex>
#include <utility>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"
#include "storage/index/index.h"
#include "storage/page/page.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_scheduler_(std::make_unique<DiskScheduler>(disk_manager)), log_manager_(log_manager) {
  // TODO(students): remove this line after you have implemented the buffer pool manager
  // throw NotImplementedException(
  //     "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
  //     "exception line in `buffer_pool_manager.cpp`.");

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
  // LOG_DEBUG("new_page--------------------------------- ");
  std::unique_lock<std::mutex> lock(rw_lock_);
  if (free_list_.empty()) {
    frame_id_t index;
    if (!replacer_->Evict(&index)) {
      return nullptr;
    }
    Page *page = &pages_[index];
    if (pages_[index].IsDirty()) {
      if (page->GetPageId() != INVALID_PAGE_ID) {
        auto write_promise = disk_scheduler_->CreatePromise();
        auto future = write_promise.get_future();
        page_id_t old_page_id = page->GetPageId();
        disk_scheduler_->Schedule({true, page->GetData(), old_page_id, std::move(write_promise)});
        future.get();
      }
    }
    page_table_.erase(page->GetPageId());
    *page_id = AllocatePage();
    page_table_.insert(std::make_pair(*page_id, index));
    replacer_->RecordAccess(index);
    replacer_->SetEvictable(index, false);
    page->ResetMemory();
    page->is_dirty_ = false;
    page->page_id_ = *page_id;
    page->pin_count_ = 1;
    // LOG_DEBUG("page_id=%d--------------------------------- ", *page_id);
    return page;
  }

  int frame_id = free_list_.front();
  free_list_.pop_front();
  *page_id = AllocatePage();
  page_table_.insert(std::make_pair(*page_id, frame_id));
  auto index = frame_id;
  replacer_->RecordAccess(index);
  replacer_->SetEvictable(index, false);
  pages_[index].pin_count_ = 1;
  pages_[index].page_id_ = *page_id;
  pages_[index].is_dirty_ = false;
  return &pages_[index];
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  // LOG_DEBUG("fetch_page--------------------------------- page_id= %d ", page_id);
  // rw_lock_.lock();
  std::unique_lock<std::mutex> lock(rw_lock_);
  frame_id_t frame_id;
  if (page_table_.find(page_id) != page_table_.end()) {
    // LOG_DEBUG("fetch_page_exsit---------------------------------");
    pages_[page_table_[page_id]].pin_count_++;
    // pages_[page_table_[page_id]].is_dirty_ = true;
    replacer_->RecordAccess(page_table_[page_id]);
    replacer_->SetEvictable(page_table_[page_id], false);
    return &pages_[page_table_[page_id]];
  }
  if (!free_list_.empty()) {
    // LOG_DEBUG("fetch_page_read--------------------------------");
    frame_id_t frame_id = free_list_.front();
    free_list_.pop_front();
    Page *page = &pages_[frame_id];
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    page_table_[page_id] = frame_id;
    // page_table_.insert(std::make_pair(page_id, frame_id));
    auto read_promise = disk_scheduler_->CreatePromise();
    auto future = read_promise.get_future();
    disk_scheduler_->Schedule({false, page->GetData(), page_id, std::move(read_promise)});
    future.get();
    page->page_id_ = page_id;
    pages_[frame_id].pin_count_ = 1;
    pages_->is_dirty_ = false;
    return page;
  }
  if (replacer_->Evict(&frame_id)) {
    Page *page = &pages_[frame_id];
    page_id_t old_page_id = page->GetPageId();
    // LOG_DEBUG("fetch_page_evict %d---------------------------------", old_page_id);
    if (page->IsDirty() && old_page_id != INVALID_PAGE_ID && page_table_.find(old_page_id) != page_table_.end()) {
      auto write_promise = disk_scheduler_->CreatePromise();
      auto future = write_promise.get_future();
      disk_scheduler_->Schedule({true, page->GetData(), old_page_id, std::move(write_promise)});
      future.get();
    }
    page_table_.erase(old_page_id);
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    page_table_[page_id] = frame_id;
    auto read_promise = disk_scheduler_->CreatePromise();
    auto future = read_promise.get_future();
    disk_scheduler_->Schedule({false, page->GetData(), page_id, std::move(read_promise)});
    future.get();
    page->is_dirty_ = false;
    page->page_id_ = page_id;
    page->pin_count_ = 1;
    return page;
  }
  // LOG_DEBUG("fetch false");
  return nullptr;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  // LOG_DEBUG("upin_page---------------------------------page_id=%d", page_id);
  if (page_id == INVALID_PAGE_ID) {
    return false;
  }
  std::unique_lock<std::mutex> lock(rw_lock_);
  if (page_table_.find(page_id) == page_table_.end()) {
    // LOG_DEBUG("upin_page_false---------------------------------");
    return false;
  }
  frame_id_t frame_id = page_table_[page_id];
  Page *page = &pages_[frame_id];
  page->is_dirty_ = is_dirty || page->is_dirty_;
  if (page->pin_count_ <= 0) {
    return false;
  }
  page->pin_count_--;
  if (page->GetPinCount() == 0) {
    replacer_->SetEvictable(frame_id, true);
  }
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  // LOG_DEBUG("flush_begin----------------------");
  if (page_table_.find(page_id) == page_table_.end() || page_id == INVALID_PAGE_ID) {
    return false;
  }
  Page *page = &pages_[page_table_[page_id]];
  auto write_promise = disk_scheduler_->CreatePromise();
  auto future = write_promise.get_future();

  disk_scheduler_->Schedule({true, page->GetData(), page_id, std::move(write_promise)});
  future.get();
  page->is_dirty_ = false;
  // rw_lock_.unlock();
  // LOG_DEBUG("flush_end----------------------");
  return true;
}

void BufferPoolManager::FlushAllPages() {
  // LOG_DEBUG("flushall_page---------------------------------");
  for (auto &page_temp : page_table_) {
    auto page_id = page_temp.first;
    if (page_id == INVALID_PAGE_ID) {
      continue;
    }
    Page *page = &pages_[page_table_[page_id]];
    auto write_promise = disk_scheduler_->CreatePromise();
    auto future = write_promise.get_future();

    disk_scheduler_->Schedule({true, page->GetData(), page_id, std::move(write_promise)});
    future.get();
    page->is_dirty_ = false;
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  // LOG_DEBUG("del_page---------------------------------");
  std::unique_lock<std::mutex> lock(rw_lock_);
  if (page_table_.find(page_id) == page_table_.end()) {
    return true;
  }
  Page *page = &pages_[page_table_[page_id]];
  if (page->GetPinCount() > 0) {
    return false;
  }
  if (page->is_dirty_) {
    FlushPage(page->GetPageId());
  }
  frame_id_t frame_id = page_table_[page_id];
  replacer_->SetEvictable(frame_id, true);
  replacer_->Remove(frame_id);
  free_list_.push_back(frame_id);
  page_table_.erase(page_id);
  page->ResetMemory();
  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard {
  LOG_DEBUG("%d", page_id);
  Page *gurd_page = FetchPage(page_id);
  return {this, gurd_page};
}

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
  // LOG_DEBUG("%d", page_id);
  Page *gurd_page = FetchPage(page_id);
  if (gurd_page != nullptr) {
    gurd_page->RLatch();
  }
  return {this, gurd_page};
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
  // LOG_DEBUG("%d", page_id);
  Page *gurd_page = FetchPage(page_id);
  if (gurd_page != nullptr) {
    gurd_page->WLatch();
  }
  return {this, gurd_page};
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard {
  Page *gurd_page = NewPage(page_id);
  // LOG_DEBUG("%d", *page_id);
  return {this, gurd_page};
}
}  // namespace bustub
// //===----------------------------------------------------------------------===//
// //
// //                         BusTub
// //
// // buffer_pool_manager.cpp
// //
// // Identification: src/buffer/buffer_pool_manager.cpp
// //
// // Copyright (c) 2015-2021, Carnegie Mellon University Database Group
// //
// //===----------------------------------------------------------------------===//

// #include "buffer/buffer_pool_manager.h"

// #include "common/exception.h"
// #include "common/macros.h"
// #include "storage/page/page_guard.h"

// namespace bustub {

// BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
//                                      LogManager *log_manager)
//     : pool_size_(pool_size), disk_scheduler_(std::make_unique<DiskScheduler>(disk_manager)),
//     log_manager_(log_manager) {
//   // we allocate a consecutive memory space for the buffer pool
//   pages_ = new Page[pool_size_];
//   replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

//   // Initially, every page is in the free list.
//   for (size_t i = 0; i < pool_size_; ++i) {
//     free_list_.emplace_back(static_cast<int>(i));
//   }
// }

// BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

// // NewPage correct
// auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
//   std::scoped_lock latch(latch_);
//   frame_id_t frame_id;
//   //如果所有的Page都被使用了
//   if (free_list_.empty()) {
//     if (!replacer_->Evict(&frame_id)) {
//       //如果所有的Page都被锁定了
//       return nullptr;
//     }
//     //如果Page是脏的，将其写回磁盘
//     if (pages_[frame_id].IsDirty()) {
//       auto promise = disk_scheduler_->CreatePromise();
//       auto future = promise.get_future();
//       disk_scheduler_->Schedule({true, pages_[frame_id].GetData(), pages_[frame_id].GetPageId(),
//       std::move(promise)}); future.get();
//     }
//     auto old_page_id = pages_[frame_id].GetPageId();
//     page_table_.erase(old_page_id);
//   } else {
//     frame_id = free_list_.front();
//     free_list_.pop_front();
//   }
//   *page_id = AllocatePage();         //获取新的页面id
//   page_table_[*page_id] = frame_id;  //将新的页面id和frame_id映射
//   //重置新页面的内存和元数据
//   auto page = &pages_[frame_id];
//   page->is_dirty_ = false;
//   page->page_id_ = *page_id;
//   page->pin_count_ = 1;
//   page->ResetMemory();
//   replacer_->RecordAccess(frame_id);         //记录帧的访问历史，注意要在设置页面不可替换之前
//   replacer_->SetEvictable(frame_id, false);  //设置页面不可替换
//   return page;
// }

// auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
//   if (page_id == INVALID_PAGE_ID) {
//     return nullptr;
//   }
//   //如果页面在页面表中，直接返回页面
//   std::scoped_lock latch(latch_);
//   Page *page = nullptr;
//   if (page_table_.find(page_id) != page_table_.end()) {
//     auto frame_id = page_table_[page_id];
//     page = &pages_[frame_id];
//     page->pin_count_++;
//     replacer_->RecordAccess(frame_id);
//     replacer_->SetEvictable(frame_id, false);
//     return page;
//   }
//   //如果页面不在页面表中，需要从磁盘中读取，即驱逐一个页面，将新页面读入
//   frame_id_t frame_id;
//   if (!free_list_.empty()) {
//     frame_id = free_list_.front();
//     free_list_.pop_front();
//     page = &pages_[frame_id];
//   } else {
//     if (!replacer_->Evict(&frame_id)) {
//       return nullptr;
//     }
//     page = &pages_[frame_id];
//   }
//   if (page->IsDirty()) {
//     auto promise = disk_scheduler_->CreatePromise();
//     auto future = promise.get_future();
//     disk_scheduler_->Schedule({true, page->GetData(), page->GetPageId(), std::move(promise)});
//     future.get();
//   }
//   page->is_dirty_ = false;
//   page_table_.erase(page->GetPageId());      //从页面表中删除旧页面
//   page_table_[page_id] = frame_id;           //将新的页面id和frame_id映射
//   page->page_id_ = page_id;                  //设置新页面的id
//   page->pin_count_ = 1;                      //设置新页面的固定计数
//   page->ResetMemory();                       //重置新页面的内存
//   replacer_->RecordAccess(frame_id);         //记录帧的访问历史
//   replacer_->SetEvictable(frame_id, false);  //设置页面不可替换
//   auto promise = disk_scheduler_->CreatePromise();
//   //将页面读入
//   auto future = promise.get_future();
//   disk_scheduler_->Schedule({false, page->GetData(), page_id, std::move(promise)});
//   future.get();
//   return page;
// }

// // UnpinPage correct
// auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool
// {
//   if (page_id == INVALID_PAGE_ID) {
//     return false;
//   }
//   //如果页面不在页面表中或其固定计数在此调用之前小于等于0，则返回false
//   std::scoped_lock latch(latch_);
//   if (page_table_.find(page_id) == page_table_.end()) {
//     return false;
//   }
//   //如果页面的固定计数在此调用之前小于等于0，说明页面已经被释放，返回false
//   auto frame_id = page_table_[page_id];
//   auto page = &pages_[frame_id];
//   page->is_dirty_ = (is_dirty || page->is_dirty_);
//   if (page->GetPinCount() <= 0) {
//     return false;
//   }
//   page->pin_count_--;
//   //如果页面的固定计数在此调用之后等于0，说明页面刚刚被释放，可以被替换器替换
//   if (page->GetPinCount() == 0) {
//     replacer_->SetEvictable(frame_id, true);
//   }
//   return true;
// }

// auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
//   if (page_id == INVALID_PAGE_ID) {
//     return false;
//   }
//   std::scoped_lock latch(latch_);
//   //如果页面在页面表中找不到，则返回false
//   if (page_table_.find(page_id) == page_table_.end()) {
//     return false;
//   }
//   auto frame_id = page_table_[page_id];
//   auto page = &pages_[frame_id];
//   //使用Scheduler::ProcessRequest方法将页面刷新到磁盘，无论脏标志如何,并取消页面的脏标志
//   auto promise = disk_scheduler_->CreatePromise();
//   auto future = promise.get_future();
//   disk_scheduler_->Schedule({true, page->GetData(), page_id, std::move(promise)});
//   future.get();
//   page->is_dirty_ = false;
//   //记录帧的访问历史
//   replacer_->RecordAccess(frame_id);
//   return true;
// }

// void BufferPoolManager::FlushAllPages() {
//   //将缓冲池中的所有页面刷新到磁盘
//   for (auto &iter : page_table_) {
//     FlushPage(iter.first);
//   }
// }

// auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
//   std::scoped_lock latch(latch_);
//   //如果页面不存在，则返回true
//   if (page_table_.find(page_id) == page_table_.end()) {
//     return true;
//   }
//   auto frame_id = page_table_[page_id];
//   auto page = &pages_[frame_id];
//   //如果页面被固定且无法删除，则立即返回false
//   if (page->GetPinCount() > 0) {
//     return false;
//   }
//   //从页面表中删除页面
//   page_table_.erase(page_id);
//   //停止在替换器中跟踪该帧
//   replacer_->Remove(frame_id);
//   //将该帧添加回空闲列表中
//   free_list_.push_back(frame_id);
//   //重置页面的内存和元数据
//   page->is_dirty_ = false;
//   page->pin_count_ = 0;
//   page->page_id_ = INVALID_PAGE_ID;  //将页面id设置为无效
//   page->ResetMemory();
//   //模拟在磁盘上释放页面
//   DeallocatePage(page_id);
//   return true;
// }

// auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

// auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, FetchPage(page_id)}; }

// auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
//   auto page = FetchPage(page_id);
//   if (page != nullptr) {
//     page->RLatch();
//   }
//   return {this, page};
// }

// auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
//   auto page = FetchPage(page_id);
//   if (page != nullptr) {
//     page->WLatch();
//   }
//   return {this, page};
// }

// auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard {
//   auto page = NewPage(page_id);
//   return {this, page};
// }

// }  // namespace bustub
