// #include "storage/page/page_guard.h"
// #include <cstddef>
// #include <utility>
// #include "buffer/buffer_pool_manager.h"
// #include "common/logger.h"
// #include "nodes/parsenodes.hpp"
// #include "storage/page/page.h"

// namespace bustub {

// auto BasicPageGuard::UpgradeRead() -> ReadPageGuard {
//   if (page_ != nullptr) {
//     LOG_DEBUG("page_id= %d", page_->GetPageId());
//   } else {
//     LOG_DEBUG("page is nullptr");
//   }
//   if (page_ != nullptr) {
//     page_->RLatch();
//   }
//   auto read_guard = ReadPageGuard(bpm_, page_);
//   bpm_ = nullptr;
//   page_ = nullptr;
//   return read_guard;
// }

// auto BasicPageGuard::UpgradeWrite() -> WritePageGuard {
//   if (page_ != nullptr) {
//     LOG_DEBUG("page_id= %d", page_->GetPageId());
//   } else {
//     LOG_DEBUG("page is nullptr");
//   }
//   if (page_ != nullptr) {
//     page_->WLatch();
//   }
//   auto write_guard = WritePageGuard(bpm_, page_);
//   bpm_ = nullptr;
//   page_ = nullptr;
//   return write_guard;
// }

// BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept
//     : bpm_(that.bpm_), page_(that.page_), is_dirty_(that.is_dirty_) {
//   if (page_ != nullptr) {
//     LOG_DEBUG("page_id= %d", page_->GetPageId());
//   } else {
//     LOG_DEBUG("page is nullptr");
//   }
//   that.bpm_ = nullptr;
//   that.page_ = nullptr;
// }

// void BasicPageGuard::Drop() {
//   if (page_ != nullptr) {
//     LOG_DEBUG("page_id= %d", page_->GetPageId());
//   } else {
//     LOG_DEBUG("page is nullptr");
//   }
//   if (bpm_ != nullptr && page_ != nullptr) {
//     bpm_->UnpinPage(page_->GetPageId(), is_dirty_);
//   }
//   bpm_ = nullptr;
//   page_ = nullptr;
// }

// auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
//   Drop();
//   bpm_ = that.bpm_;
//   page_ = that.page_;
//   that.bpm_ = nullptr;
//   that.page_ = nullptr;
//   is_dirty_ = that.is_dirty_;
//   if (page_ != nullptr) {
//     LOG_DEBUG("page_id= %d", page_->GetPageId());
//   } else {
//     LOG_DEBUG("page is nullptr");
//   }
//   return *this;
// }

// BasicPageGuard::~BasicPageGuard() {
//   if (page_ != nullptr) {
//     LOG_DEBUG("page_id= %d", page_->GetPageId());
//   } else {
//     LOG_DEBUG("page is nullptr");
//   }
//   Drop();
// };  // NOLINT

// ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept {
//   if (that.guard_.page_ != nullptr) {
//     LOG_DEBUG("page_id= %d", that.guard_.page_->GetPageId());
//   } else {
//     LOG_DEBUG("page is nullptr");
//   }
//   guard_ = std::move(that.guard_);
// }

// auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
//   if (guard_.page_ != nullptr) {
//     LOG_DEBUG("this page_id= %d", guard_.page_->GetPageId());
//   } else {
//     LOG_DEBUG("this page is nullptr");
//   }
//   Drop();
//   if (that.guard_.page_ != nullptr) {
//     LOG_DEBUG("that page_id= %d", that.guard_.page_->GetPageId());
//   } else {
//     LOG_DEBUG("that page is nullptr");
//   }
//   guard_ = std::move(that.guard_);
//   return *this;
// }

// void ReadPageGuard::Drop() {
//   if (guard_.page_ != nullptr) {
//     LOG_DEBUG("page_id= %d", guard_.page_->GetPageId());
//   } else {
//     LOG_DEBUG("page is nullptr");
//   }
//   if (guard_.page_ != nullptr) {
//     guard_.page_->RUnlatch();
//   }
//   guard_.Drop();
// }

// ReadPageGuard::~ReadPageGuard() {
//   if (guard_.page_ != nullptr) {
//     LOG_DEBUG("page_id= %d", guard_.page_->GetPageId());
//   } else {
//     LOG_DEBUG("page is nullptr");
//   }
//   Drop();
// }  // NOLINT

// WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept {
//   if (guard_.page_ != nullptr) {
//     LOG_DEBUG("this page_id= %d", guard_.page_->GetPageId());
//   } else {
//     LOG_DEBUG("this page is nullptr");
//   }
//   if (that.guard_.page_ != nullptr) {
//     LOG_DEBUG("that page_id= %d", that.guard_.page_->GetPageId());
//   } else {
//     LOG_DEBUG("that page is nullptr");
//   }
//   guard_ = std::move(that.guard_);
// }

// auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
//   if (that.guard_.page_ != nullptr) {
//     LOG_DEBUG("this page_id= %d", that.guard_.page_->GetPageId());
//   } else {
//     LOG_DEBUG("this page is nullptr");
//   }
//   Drop();
//   guard_ = std::move(that.guard_);
//   if (that.guard_.page_ != nullptr) {
//     LOG_DEBUG("that page_id= %d", that.guard_.page_->GetPageId());
//   } else {
//     LOG_DEBUG("that page is nullptr");
//   }
//   return *this;
// }

// void WritePageGuard::Drop() {
//   if (guard_.page_ != nullptr) {
//     LOG_DEBUG("page_id= %d", guard_.page_->GetPageId());
//   } else {
//     LOG_DEBUG("page is nullptr");
//   }
//   if (guard_.page_ != nullptr) {
//     guard_.page_->WUnlatch();
//   }
//   guard_.is_dirty_ = true;
//   guard_.Drop();
// }

// WritePageGuard::~WritePageGuard() {
//   if (guard_.page_ != nullptr) {
//     LOG_DEBUG("page_id= %d", guard_.page_->GetPageId());
//   } else {
//     LOG_DEBUG("page is nullptr");
//   }
//   Drop();
// }  // NOLINT

// }  // namespace bustub

#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept {
  this->bpm_ = that.bpm_;
  this->page_ = that.page_;
  this->is_dirty_ = that.is_dirty_;
  that.bpm_ = nullptr;
  that.page_ = nullptr;
}

void BasicPageGuard::Drop() {
  // 如果page_不为空，说明这个page guard是有效的，需要将其unpin
  if (this->page_ != nullptr && this->bpm_ != nullptr) {
    this->bpm_->UnpinPage(this->page_->GetPageId(), this->is_dirty_);
  }
  this->bpm_ = nullptr;
  this->page_ = nullptr;
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & {
  // 如果是自己给自己赋值，直接返回自己
  if (this == &that) {
    return *this;
  }
  // 当前可能page guard会有资源且被pin住，先调用Drop()函数，将原来的page guard丢弃
  Drop();
  this->bpm_ = that.bpm_;
  this->page_ = that.page_;
  this->is_dirty_ = that.is_dirty_;
  that.bpm_ = nullptr;
  that.page_ = nullptr;
  return *this;
}

BasicPageGuard::~BasicPageGuard() { Drop(); };  // NOLINT

auto BasicPageGuard::UpgradeRead() -> ReadPageGuard {
  if (page_ != nullptr) {
    page_->RLatch();
  }
  ReadPageGuard readpagegurad(bpm_, page_);
  bpm_ = nullptr;
  page_ = nullptr;
  return readpagegurad;
}

auto BasicPageGuard::UpgradeWrite() -> WritePageGuard {
  if (page_ != nullptr) {
    page_->WLatch();
  }
  WritePageGuard writepagegurad(bpm_, page_);
  bpm_ = nullptr;
  page_ = nullptr;
  return writepagegurad;
}

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept { this->guard_ = std::move(that.guard_); }

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  if (this == &that) {
    return *this;
  }
  Drop();
  this->guard_ = std::move(that.guard_);
  return *this;
}

void ReadPageGuard::Drop() {
  // 要先unlatch，再drop，因为如果先drop，那么page guard就无效了，无法调用page_->RUnlatch()，会导致page无法latch
  if (this->guard_.page_ != nullptr) {
    this->guard_.page_->RUnlatch();
  }
  this->guard_.Drop();
}

ReadPageGuard::~ReadPageGuard() { this->Drop(); }  // NOLINT

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept { this->guard_ = std::move(that.guard_); }

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  if (this == &that) {
    return *this;
  }
  Drop();
  this->guard_ = std::move(that.guard_);
  return *this;
}

void WritePageGuard::Drop() {
  if (this->guard_.page_ != nullptr) {
    this->guard_.page_->WUnlatch();
  }
  this->guard_.Drop();
}

WritePageGuard::~WritePageGuard() { this->Drop(); }  // NOLINT

}  // namespace bustub
