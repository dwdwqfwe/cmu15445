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
#include <climits>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <list>
#include <memory>
#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

LRUKNode::LRUKNode(frame_id_t frame_id,size_t k):k_(k),fid_(frame_id)
{

}

auto LRUKNode::FindDistance() -> size_t
{
    if(history_.size()<k_){
        return INFINITY;
    }
    size_t nums=0;
    for(auto & list_temp : history_){
        nums++;
        if(k_==nums){
            return list_temp;
        }
    }
    return 0;
}

auto LRUKNode::SetTure()->void
{
    is_evictable_=true;
}

auto LRUKNode::SetFalse()->void
{
    is_evictable_=false;
}

auto LRUKNode::GetId()->frame_id_t
{
    return fid_;
}

auto LRUKNode::Evictable()->bool
{
    return is_evictable_;
}

auto LRUKNode::GetRealDistance()->size_t
{
    return history_.back();
}

auto LRUKNode::GetList()->std::list<size_t>&
{
    return history_;
}

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : max_replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool
{
    size_t pre_distance=0;
    size_t real_distance=0;
    bool res=false;
    rw_lock_.lock_shared();
    for(auto & lru_node : node_store_){
        size_t distance=current_timestamp_-lru_node.second.FindDistance();
        if(lru_node.second.GetList().size()<k_){
            distance=UINT_MAX;
        }
        bool evictable=lru_node.second.Evictable();
        if(distance>=pre_distance&&evictable){
            pre_distance=distance;
            *frame_id=lru_node.second.GetId();
            if(distance==UINT_MAX){
                real_distance=lru_node.second.GetRealDistance();
            }
            res=true;
        }
        else if(distance==UINT_MAX&&real_distance<=lru_node.second.GetRealDistance()&&evictable){
            real_distance=lru_node.second.GetRealDistance();
            pre_distance=distance;
            *frame_id=lru_node.second.GetId();
        }
    }
    rw_lock_.unlock_shared();
    if(res){
        Remove(*frame_id);
    }
    return res;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type)
{
    BUSTUB_ASSERT((size_t)frame_id<=max_replacer_size_,"the frame_id is invaild");
    rw_lock_.lock();
    if(node_store_.find(frame_id)==node_store_.end()){
        LRUKNode new_node(frame_id,k_);
        std::list<size_t>& new_history=new_node.GetList();
        new_history.push_front(current_timestamp_);
        node_store_.insert(std::make_pair(frame_id,new_node));
    }
    else{
        LRUKNode& temp_node=node_store_.at(frame_id);
        std::list<size_t> &temp_history=temp_node.GetList();
        temp_history.push_front(current_timestamp_);

    }
    current_timestamp_++;
    rw_lock_.unlock();
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable)
{
    BUSTUB_ASSERT(node_store_.find(frame_id)!=node_store_.end(),"frame_id is invalid in set");
    latch_.lock();
    if(set_evictable&&!node_store_[frame_id].Evictable()){
        ++curr_size_;
        node_store_[frame_id].SetTure();
    }
    else if(!set_evictable&&node_store_[frame_id].Evictable()){
        --curr_size_;
        node_store_[frame_id].SetFalse();
    }
    latch_.unlock();
}

void LRUKReplacer::Remove(frame_id_t frame_id)
{
    rw_lock_.lock_shared();
    if(node_store_.find(frame_id)==node_store_.end()){
        return;
    }
    rw_lock_.unlock_shared();
    BUSTUB_ASSERT(node_store_[frame_id].Evictable(),"node is unevictable");
    rw_lock_.lock();
    node_store_.erase(frame_id);
    --curr_size_;
    rw_lock_.unlock();
}

auto LRUKReplacer::Size() -> size_t
{
    rw_lock_.lock_shared();
    size_t res=curr_size_;
    rw_lock_.unlock_shared();
    return res;
}

}  // namespace bustub
