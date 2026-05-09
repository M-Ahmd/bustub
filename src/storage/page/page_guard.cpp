//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// page_guard.cpp
//
// Identification: src/storage/page/page_guard.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/page_guard.h"
#include <memory>
#include "buffer/arc_replacer.h"
#include "common/macros.h"

namespace bustub {

// ============================================================================
// ReadPageGuard
// ============================================================================

/**
 * @brief The only constructor for an RAII `ReadPageGuard` that creates a valid guard.
 *
 * Note that only the buffer pool manager is allowed to call this constructor.
 * The BPM has already:
 *   - Pinned the frame (incremented pin_count_)
 *   - Called RecordAccess / SetEvictable on the replacer
 *   - Released the BPM latch
 * So the guard constructor only needs to acquire the rwlatch in shared mode.
 */
ReadPageGuard::ReadPageGuard(page_id_t page_id, std::shared_ptr<FrameHeader> frame,
                             std::shared_ptr<ArcReplacer> replacer, std::shared_ptr<std::mutex> bpm_latch,
                             std::shared_ptr<DiskScheduler> disk_scheduler)
    : page_id_(page_id),
      frame_(std::move(frame)),
      replacer_(std::move(replacer)),
      bpm_latch_(std::move(bpm_latch)),
      disk_scheduler_(std::move(disk_scheduler)),
      is_valid_(true) {
  // Acquire shared read lock on the frame's rwlatch.
  // BPM latch is NOT held here — no deadlock risk.
  frame_->rwlatch_.lock_shared();
}

/**
 * @brief The move constructor for `ReadPageGuard`.
 */
ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept {
  page_id_ = that.page_id_;
  frame_ = std::move(that.frame_);
  replacer_ = std::move(that.replacer_);
  bpm_latch_ = std::move(that.bpm_latch_);
  disk_scheduler_ = std::move(that.disk_scheduler_);
  is_valid_ = that.is_valid_;

  that.page_id_ = INVALID_PAGE_ID;
  that.is_valid_ = false;
}

/**
 * @brief The move assignment operator for `ReadPageGuard`.
 */
auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  if (this != &that) {
    // Release current resources first.
    Drop();

    page_id_ = that.page_id_;
    frame_ = std::move(that.frame_);
    replacer_ = std::move(that.replacer_);
    bpm_latch_ = std::move(that.bpm_latch_);
    disk_scheduler_ = std::move(that.disk_scheduler_);
    is_valid_ = that.is_valid_;

    that.page_id_ = INVALID_PAGE_ID;
    that.is_valid_ = false;
  }
  return *this;
}

/**
 * @brief Gets the page ID of the page this guard is protecting.
 */
auto ReadPageGuard::GetPageId() const -> page_id_t {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid read guard");
  return page_id_;
}

/**
 * @brief Gets a `const` pointer to the page of data this guard is protecting.
 */
auto ReadPageGuard::GetData() const -> const char * {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid read guard");
  return frame_->GetData();
}

/**
 * @brief Returns whether the page is dirty (modified but not flushed to the disk).
 */
auto ReadPageGuard::IsDirty() const -> bool {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid read guard");
  return frame_->is_dirty_;
}

/**
 * @brief Flushes this page's data safely to disk.
 *
 * For a ReadPageGuard we already hold the shared lock, so the data is consistent.
 * We write it to disk if dirty.
 */
void ReadPageGuard::Flush() {
  if (!is_valid_ || frame_ == nullptr || !frame_->is_dirty_) {
    return;
  }

  DiskRequest request;
  request.is_write_ = true;  // WRITE to disk, not read!
  request.page_id_ = page_id_;
  request.data_ = frame_->GetDataMut();

  auto future = request.callback_.get_future();
  std::vector<DiskRequest> requests;
  requests.push_back(std::move(request));
  disk_scheduler_->Schedule(requests);

  future.get();
  frame_->is_dirty_ = false;
}

/**
 * @brief Manually drops a valid `ReadPageGuard`'s data.
 *
 * Order of operations:
 *   1. Release the shared rwlatch (so other threads can access the page)
 *   2. Acquire BPM latch, decrement pin count, update replacer evictability
 *   3. Invalidate all fields
 */
void ReadPageGuard::Drop() {
  if (!is_valid_) {
    return;
  }

  // 1. Release the read lock on the frame FIRST.
  frame_->rwlatch_.unlock_shared();

  // 2. Decrement pin count and possibly mark as evictable.
  {
    std::scoped_lock lock(*bpm_latch_);
    auto old_pin = frame_->pin_count_.fetch_sub(1);
    BUSTUB_ASSERT(old_pin > 0, "Pin count underflow in ReadPageGuard::Drop()");
    if (old_pin == 1) {
      // pin_count_ is now 0 → the frame is evictable.
      replacer_->SetEvictable(frame_->frame_id_, true);
    }
  }

  // 3. Invalidate this guard.
  frame_ = nullptr;
  replacer_ = nullptr;
  bpm_latch_ = nullptr;
  disk_scheduler_ = nullptr;
  page_id_ = INVALID_PAGE_ID;
  is_valid_ = false;
}

/** @brief The destructor for `ReadPageGuard`. This destructor simply calls `Drop()`. */
ReadPageGuard::~ReadPageGuard() { Drop(); }

/**********************************************************************************************************************/
/**********************************************************************************************************************/
/**********************************************************************************************************************/

// ============================================================================
// WritePageGuard
// ============================================================================

/**
 * @brief The only constructor for an RAII `WritePageGuard` that creates a valid guard.
 *
 * The BPM has already:
 *   - Pinned the frame (incremented pin_count_)
 *   - Called RecordAccess / SetEvictable on the replacer
 *   - Released the BPM latch
 * So the guard constructor only needs to acquire the rwlatch in exclusive mode.
 */
WritePageGuard::WritePageGuard(page_id_t page_id, std::shared_ptr<FrameHeader> frame,
                               std::shared_ptr<ArcReplacer> replacer, std::shared_ptr<std::mutex> bpm_latch,
                               std::shared_ptr<DiskScheduler> disk_scheduler)
    : page_id_(page_id),
      frame_(std::move(frame)),
      replacer_(std::move(replacer)),
      bpm_latch_(std::move(bpm_latch)),
      disk_scheduler_(std::move(disk_scheduler)),
      is_valid_(true) {
  // Acquire exclusive write lock on the frame's rwlatch.
  // BPM latch is NOT held here — no deadlock risk.
  frame_->rwlatch_.lock();
}

/**
 * @brief The move constructor for `WritePageGuard`.
 */
WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept {
  page_id_ = that.page_id_;
  frame_ = std::move(that.frame_);
  replacer_ = std::move(that.replacer_);
  bpm_latch_ = std::move(that.bpm_latch_);
  disk_scheduler_ = std::move(that.disk_scheduler_);
  is_valid_ = that.is_valid_;

  that.page_id_ = INVALID_PAGE_ID;
  that.is_valid_ = false;
}

/**
 * @brief The move assignment operator for `WritePageGuard`.
 */
auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  if (this != &that) {
    Drop();

    page_id_ = that.page_id_;
    frame_ = std::move(that.frame_);
    replacer_ = std::move(that.replacer_);
    bpm_latch_ = std::move(that.bpm_latch_);
    disk_scheduler_ = std::move(that.disk_scheduler_);
    is_valid_ = that.is_valid_;

    that.page_id_ = INVALID_PAGE_ID;
    that.is_valid_ = false;
  }
  return *this;
}

/**
 * @brief Gets the page ID of the page this guard is protecting.
 */
auto WritePageGuard::GetPageId() const -> page_id_t {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid write guard");
  return page_id_;
}

/**
 * @brief Gets a `const` pointer to the page of data this guard is protecting.
 */
auto WritePageGuard::GetData() const -> const char * {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid write guard");
  return frame_->GetData();
}

/**
 * @brief Gets a mutable pointer to the page of data this guard is protecting.
 */
auto WritePageGuard::GetDataMut() -> char * {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid write guard");
  frame_->is_dirty_ = true;  // Mark dirty on mutable access.
  return frame_->GetDataMut();
}

/**
 * @brief Returns whether the page is dirty (modified but not flushed to the disk).
 */
auto WritePageGuard::IsDirty() const -> bool {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid write guard");
  return frame_->is_dirty_;
}

/**
 * @brief Flushes this page's data safely to disk.
 *
 * We hold the exclusive write lock, so the data is consistent.
 */
void WritePageGuard::Flush() {
  if (!is_valid_ || frame_ == nullptr || !frame_->is_dirty_) {
    return;
  }

  DiskRequest request;
  request.is_write_ = true;
  request.page_id_ = page_id_;
  request.data_ = frame_->GetDataMut();

  auto future = request.callback_.get_future();
  std::vector<DiskRequest> requests;
  requests.push_back(std::move(request));
  disk_scheduler_->Schedule(requests);

  future.get();
  frame_->is_dirty_ = false;
}

/**
 * @brief Manually drops a valid `WritePageGuard`'s data.
 *
 * Order of operations:
 *   1. Release the exclusive rwlatch
 *   2. Acquire BPM latch, decrement pin count, update replacer evictability
 *   3. Invalidate all fields
 *
 * Note: We do NOT auto-flush on drop. Dirty pages will be flushed during eviction
 * or via explicit Flush() calls. This avoids unnecessary I/O on every guard drop.
 */
void WritePageGuard::Drop() {
  if (!is_valid_) {
    return;
  }

  // 1. Release the exclusive write lock.
  frame_->rwlatch_.unlock();

  // 2. Decrement pin count and possibly mark as evictable.
  {
    std::scoped_lock lock(*bpm_latch_);
    auto old_pin = frame_->pin_count_.fetch_sub(1);
    BUSTUB_ASSERT(old_pin > 0, "Pin count underflow in WritePageGuard::Drop()");
    if (old_pin == 1) {
      replacer_->SetEvictable(frame_->frame_id_, true);
    }
  }

  // 3. Invalidate this guard.
  frame_ = nullptr;
  replacer_ = nullptr;
  bpm_latch_ = nullptr;
  disk_scheduler_ = nullptr;
  page_id_ = INVALID_PAGE_ID;
  is_valid_ = false;
}

/** @brief The destructor for `WritePageGuard`. This destructor simply calls `Drop()`. */
WritePageGuard::~WritePageGuard() { Drop(); }

}  // namespace bustub
