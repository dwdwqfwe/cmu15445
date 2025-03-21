//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_scheduler.cpp
//
// Identification: src/storage/disk/disk_scheduler.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/disk/disk_scheduler.h"
#include <optional>
#include <thread>
#include <utility>
#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "storage/disk/disk_manager.h"

namespace bustub {

DiskScheduler::DiskScheduler(DiskManager *disk_manager) : disk_manager_(disk_manager) {
  // TODO(P1): remove this line after you have implemented the disk scheduler API
  // throw NotImplementedException(
  //     "DiskScheduler is not implemented yet. If you have finished implementing the disk scheduler, please remove

  //     " "throw exception line in `disk_scheduler.cpp`.");

  // Spawn the background thread
  background_thread_.emplace([&] { StartWorkerThread(); });
}

DiskScheduler::~DiskScheduler() {
  // LOG_DEBUG("diskscheduler---------------------------------");
  // Put a `std::nullopt` in the queue to signal to exit the loop
  request_queue_.Put(std::nullopt);
  if (background_thread_.has_value()) {
    background_thread_->join();
  }
}

void DiskScheduler::Schedule(DiskRequest r) {
  // LOG_DEBUG("schedule---------------------------------");
  std::optional<DiskRequest> request = std::move(r);
  if (!request->is_write_) {
    LOG_DEBUG("read is reading----------");
  }
  request_queue_.Put(std::move(request));
}

void DiskScheduler::StartWorkerThread() {
  // LOG_DEBUG("starthread---------------------------------");
  while (true) {
    std::optional<DiskRequest> task = request_queue_.Get();
    if (task == std::nullopt) {
      break;
    }
    if (!task->is_write_) {
      LOG_DEBUG("read is coming----------");
    }
    th_wait_.emplace_back(&DiskScheduler::ProcessDeal, this, std::move(task));
    th_wait_[th_wait_.size() - 1].join();
  }
  // for (auto &th : th_wait_) {
  //   th.join();
  // }
}

auto DiskScheduler::ProcessDeal(std::optional<DiskRequest> task) -> void {
  // LOG_DEBUG("processdeal---------------------------------");
  if (task == std::nullopt) {
    return;
  }
  if (task->is_write_) {
    disk_manager_->WritePage(task->page_id_, task->data_);
  } else {
    disk_manager_->ReadPage(task->page_id_, task->data_);
  }
  task->callback_.set_value(true);
}

}  // namespace bustub
