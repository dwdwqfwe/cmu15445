#include "concurrency/watermark.h"
#include <exception>
#include <iostream>
#include <memory>
#include <utility>
#include "common/exception.h"
#include "common/logger.h"

namespace bustub {

auto Watermark::AddTxn(timestamp_t read_ts) -> void {
  if (read_ts < commit_ts_) {
    throw Exception("read ts < commit ts");
  }
  if (current_reads_.find(read_ts) == current_reads_.end()) {
    current_reads_.insert(std::make_pair(read_ts, 1));
  } else {
    current_reads_[read_ts]++;
  }
  if (record_reads_.find(read_ts) == record_reads_.end()) {
    record_reads_.insert(std::make_pair(read_ts, 1));
  } else {
    record_reads_[read_ts]++;
  }
  // LOG_DEBUG("waterwork = %ud",record_reads_[read_ts]);
  // TODO(fall2023): implement me!
}

auto Watermark::RemoveTxn(timestamp_t read_ts) -> void {
  // TODO(fall2023): implement me!
  if (current_reads_.find(read_ts) != current_reads_.end()) {
    current_reads_[read_ts]--;
    if (current_reads_[read_ts] == 0) {
      current_reads_.erase(read_ts);
    }
  }
  if (record_reads_.find(read_ts) != record_reads_.end()) {
    if (--record_reads_[read_ts] == 0) {
      if (read_ts == 0) {
        std::cout << "read_ts out"
                  << "\n";
      }
      record_reads_.erase(read_ts);
    }
  }
}

}  // namespace bustub
