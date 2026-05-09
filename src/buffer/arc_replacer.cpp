// :bustub-keep-private:
//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// arc_replacer.cpp
//
// Identification: src/buffer/arc_replacer.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/arc_replacer.h"
#include <optional>
#include "common/config.h"
using namespace std;
namespace bustub {

/**
 *
 * TODO(P1): Add implementation
 *
 * @brief a new ArcReplacer, with lists initialized to be empty and target size to 0
 * @param num_frames the maximum number of frames the ArcReplacer will be required to cache
 */
ArcReplacer::ArcReplacer(size_t num_frames) : replacer_size_(num_frames) {}

/**
 * TODO(P1): Add implementation
 *
 * @brief Performs the Replace operation as described by the writeup
 * that evicts from either mfu_ or mru_ into its corresponding ghost list
 * according to balancing policy.
 *
 * If you wish to refer to the original ARC paper, please note that there are
 * two changes in our implementation:
 * 1. When the size of mru_ equals the target size, we don't check
 * the last access as the paper did when deciding which list to evict from.
 * This is fine since the original decision is stated to be arbitrary.
 * 2. Entries that are not evictable are skipped. If all entries from the desired side
 * (mru_ / mfu_) are pinned, we instead try victimize the other side (mfu_ / mru_),
 * and move it to its corresponding ghost list (mfu_ghost_ / mru_ghost_).
 *
 * @return frame id of the evicted frame, or std::nullopt if cannot evict
 */
auto ArcReplacer::Evict() -> std::optional<frame_id_t> {
  // Fast path: skip locking when nothing is evictable.
  if (curr_size_ == 0) {
    return std::nullopt;
  }

  std::unique_lock lock(latch_);

  // Re-check under lock.
  if (curr_size_ == 0) {
    return std::nullopt;
  }

  // Snapshot the eviction policy decision before scanning.
  bool prefer_mru = (mru_.size() >= mru_target_size_);

  auto try_evict_from = [&](std::list<frame_id_t> &list,
                            std::list<page_id_t> &ghost_list,
                            ArcStatus ghost_status) -> std::optional<frame_id_t> {
    for (auto rit = list.rbegin(); rit != list.rend(); ++rit) {
      frame_id_t fid = *rit;
      auto live_it = alive_map_.find(fid);
      if (live_it == alive_map_.end()) {
        continue;
      }
      if (!live_it->second->evictable_) {
        continue;
      }

      // Found a victim — perform all mutations.
      auto frame = live_it->second;
      auto normal_it = std::prev(rit.base());

      ghost_list.push_front(frame->page_id_);
      ghost_map_[frame->page_id_] = frame;
      frame->arc_status_ = ghost_status;

      list.erase(normal_it);
      alive_map_.erase(live_it);

      BUSTUB_ASSERT(curr_size_ > 0, "Replacer size underflow in Evict()");
      curr_size_--;

      return fid;
    }
    return std::nullopt;
  };

  std::optional<frame_id_t> result;

  if (prefer_mru) {
    result = try_evict_from(mru_, mru_ghost_, ArcStatus::MRU_GHOST);
    if (!result) {
      result = try_evict_from(mfu_, mfu_ghost_, ArcStatus::MFU_GHOST);
    }
  } else {
    result = try_evict_from(mfu_, mfu_ghost_, ArcStatus::MFU_GHOST);
    if (!result) {
      result = try_evict_from(mru_, mru_ghost_, ArcStatus::MRU_GHOST);
    }
  }

  // Release lock before returning — no more shared state access needed.
  lock.unlock();

  return result;
}
void ArcReplacer::RecordAccess(frame_id_t frame_id, page_id_t page_id, [[maybe_unused]] AccessType access_type) {
    std::unique_lock lock(latch_);

    // --- Case 1: Frame is already alive (in mru_ or mfu_) → promote to MFU front ---
    auto live_it = alive_map_.find(frame_id);
    if (live_it != alive_map_.end()) {
        auto status = live_it->second->arc_status_;
        if (status == ArcStatus::MRU) {
            mru_.remove(frame_id);
        } else {
            mfu_.remove(frame_id);
        }
        mfu_.push_front(frame_id);
        live_it->second->arc_status_ = ArcStatus::MFU;
        lock.unlock();
        return;
    }

    // --- Case 2: Page is in a ghost list → adjust target, promote to MFU ---
    auto ghost_it = ghost_map_.find(page_id);
    if (ghost_it != ghost_map_.end()) {
        auto status = ghost_it->second->arc_status_;
        if (status == ArcStatus::MRU_GHOST) {
            if (mru_ghost_.size() >= mfu_ghost_.size())
                mru_target_size_++;
            else
                mru_target_size_ += mfu_ghost_.size() / mru_ghost_.size();
            mru_target_size_ = std::min(mru_target_size_, replacer_size_);

            mru_ghost_.remove(page_id);
        } else { // MFU_GHOST
            if (mfu_ghost_.size() >= mru_ghost_.size())
                mru_target_size_--;
            else
                mru_target_size_ -= mru_ghost_.size() / mfu_ghost_.size();
            mru_target_size_ = std::max(mru_target_size_, size_t{0});

            mfu_ghost_.remove(page_id);
        }

        ghost_map_.erase(page_id);
        alive_map_[frame_id] = std::make_shared<FrameStatus>(page_id, frame_id, false, ArcStatus::MFU);
        mfu_.push_front(frame_id);
        lock.unlock();
        return;
    }

    // --- Case 3: Completely new page → insert into MRU, maybe evict a ghost ---
    if (mru_.size() + mru_ghost_.size() == replacer_size_) {
        page_id_t victim = mru_ghost_.back();
        mru_ghost_.pop_back();
        ghost_map_.erase(victim);
    } else if (mru_.size() + mru_ghost_.size() + mfu_.size() + mfu_ghost_.size() == 2 * replacer_size_) {
        page_id_t victim = mfu_ghost_.back();
        mfu_ghost_.pop_back();
        ghost_map_.erase(victim);
    }

    alive_map_[frame_id] = std::make_shared<FrameStatus>(page_id, frame_id, false, ArcStatus::MRU);
    mru_.push_front(frame_id);
    lock.unlock();
}




/**
 * TODO(P1): Add implementation
 *
 * @brief Toggle whether a frame is evictable or non-evictable. This function also
 * controls replacer's size. Note that size is equal to number of evictable entries.
 *
 * If a frame was previously evictable and is to be set to non-evictable, then size should
 * decrement. If a frame was previously non-evictable and is to be set to evictable,
 * then size should increment.
 *
 * If frame id is invalid, throw an exception or abort the process.
 *
 * For other scenarios, this function should terminate without modifying anything.
 *
 * @param frame_id id of frame whose 'evictable' status will be modified
 * @param set_evictable whether the given frame is evictable or not
 */
void ArcReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
    std::unique_lock lock(latch_);

    auto it = alive_map_.find(frame_id);
    BUSTUB_ASSERT(it != alive_map_.end(), "Invalid frame id in SetEvictable()");

    auto &frame = it->second;  // reference — avoids shared_ptr copy
    BUSTUB_ASSERT(frame != nullptr, "Frame pointer is null in SetEvictable()");

    if (set_evictable == frame->evictable_) {
        return;  // no-op — lock released automatically
    }

    frame->evictable_ = set_evictable;
    curr_size_ += set_evictable ? 1 : -1;
    lock.unlock();
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Remove an evictable frame from replacer.
 * This function should also decrement replacer's size if removal is successful.
 *
 * Note that this is different from evicting a frame, which always remove the frame
 * decided by the ARC algorithm.
 *
 * If Remove is called on a non-evictable frame, throw an exception or abort the
 * process.
 *
 * If specified frame is not found, directly return from this function.
 *
 * @param frame_id id of frame to be removed
 */
void ArcReplacer::Remove(frame_id_t frame_id) {
    std::unique_lock lock(latch_);

    auto info = alive_map_.find(frame_id);
    if (info == alive_map_.end()) {
        return;  // not found — lock released automatically
    }

    auto &frame = info->second;  // reference — avoids shared_ptr copy
    BUSTUB_ASSERT(frame != nullptr, "Null frame in Remove()");
    BUSTUB_ASSERT(frame->evictable_, "Attempted to remove a non-evictable frame");

    page_id_t id = frame->page_id_;

    switch (frame->arc_status_) {
        case ArcStatus::MRU:
            mru_.remove(frame_id);
            break;
        case ArcStatus::MFU:
            mfu_.remove(frame_id);
            break;
        case ArcStatus::MRU_GHOST:
            mru_ghost_.remove(id);
            break;
        case ArcStatus::MFU_GHOST:
            mfu_ghost_.remove(id);
            break;
        default:
            break;
    }

    alive_map_.erase(frame_id);
    ghost_map_.erase(id);

    BUSTUB_ASSERT(curr_size_ > 0, "curr_size_ underflow in Remove()");
    curr_size_--;
    lock.unlock();
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Return replacer's size, which tracks the number of evictable frames.
 *
 * @return size_t
 */
auto ArcReplacer::Size() -> size_t
{
	std::unique_lock lock(latch_);
	size_t size = curr_size_;
	lock.unlock();
	return size;
}

}  // namespace bustub
