// -*- Mode: C++; c-basic-offset: 2; indent-tabs-mode: nil -*-
// Copyright (c) 2008, Google Inc.
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are
// met:
//
//     * Redistributions of source code must retain the above copyright
// notice, this list of conditions and the following disclaimer.
//     * Redistributions in binary form must reproduce the above
// copyright notice, this list of conditions and the following disclaimer
// in the documentation and/or other materials provided with the
// distribution.
//     * Neither the name of Google Inc. nor the names of its
// contributors may be used to endorse or promote products derived from
// this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
// "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
// LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
// A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
// OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
// LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
// DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
// THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

// ---
// Author: Sanjay Ghemawat <opensource@google.com>

#ifndef TCMALLOC_THREAD_CACHE_H_
#define TCMALLOC_THREAD_CACHE_H_

#include <config.h>
#ifdef HAVE_PTHREAD
#include <pthread.h>                    // for pthread_t, pthread_key_t
#endif
#include <stddef.h>                     // for size_t, NULL
#ifdef HAVE_STDINT_H
#include <stdint.h>                     // for uint32_t, uint64_t
#endif
#include <sys/types.h>                  // for ssize_t
#include "common.h"
#include "linked_list.h"
#include "maybe_threads.h"
#include "page_heap_allocator.h"
#include "sampler.h"
#include "static_vars.h"

#include "common.h"            // for SizeMap, kMaxSize, etc
#include "internal_logging.h"  // for ASSERT, etc
#include "linked_list.h"       // for SLL_Pop, SLL_PopRange, etc
#include "page_heap_allocator.h"  // for PageHeapAllocator
#include "sampler.h"           // for Sampler
#include "static_vars.h"       // for Static
#include <thread>
#include <condition_variable>

extern "C" {
#include "rseq/rseq.h"
}


struct rseq_section {
	void *begin;
	void *end;
	void *restart;
};

extern struct rseq_section const __start___rseq_sections[]
__attribute((weak));
extern struct rseq_section const __stop___rseq_sections[]
__attribute((weak));

inline int rseq_percpu_cmpxchg(int cpu, intptr_t *p, intptr_t old, intptr_t newv) {
	asm volatile goto (
		"1:\n\t"
		"cmpl %1, %0\n\t"
		"jne %l[fail]\n\t"
		"cmpq %2, %3\n\t"
		"jne %l[fail]\n\t"
		"movq %4, %3\n\t"
		"2:\n\t"
		".pushsection __rseq_sections, \"a\"\n\t"
		".quad 1b, 2b, 1b\n\t"
		".popsection\n\t"
		:
		: "r" (cpu), "m" (__rseq_current_cpu),
		  "r" (old), "m" (*p), "r" (newv)
		: "memory"
		: fail);
	return 0;
fail:
	return -1;
}
inline int rseq_percpu_cmpxchgcheck(int cpu, intptr_t *p, intptr_t old, intptr_t newv,
			intptr_t *check_ptr, intptr_t check_val) {
	asm volatile goto (
		"1:\n\t"
		"cmpl %1, %0\n\t"
		"jne %l[fail]\n\t"
		"cmpq %2, %3\n\t"
		"jne %l[fail]\n\t"
		"cmpq %5, %6\n\t"
		"jne %l[fail]\n\t"
		"movq %4, %3\n\t"
		"2:\n\t"
		".pushsection __rseq_sections, \"a\"\n\t"
		".quad 1b, 2b, 1b\n\t"
		".popsection\n\t"
		:
		: "r" (cpu), "m" (__rseq_current_cpu),
		  "r" (old), "m" (*p), "r" (newv),
		  "r" (check_val), "m" (*check_ptr)
		: "memory"
		: fail);
	return 0;
fail:
	return -1;
}

inline int rseq_percpu_cmpxchgcheckcheck(int cpu, intptr_t *p, intptr_t old, intptr_t newv,
                                  intptr_t *check_ptr, intptr_t check_val,
                                  intptr_t *check_ptr2, intptr_t check_val2) {
	asm volatile goto (
		"1:\n\t"
		"cmpl %1, %0\n\t"
		"jne %l[fail]\n\t"
		"cmpq %2, %3\n\t"
		"jne %l[fail]\n\t"
		"cmpq %5, %6\n\t"
		"jne %l[fail]\n\t"
		"cmpq %7, %8\n\t"
		"jne %l[fail]\n\t"
		"movq %4, %3\n\t"
		"2:\n\t"
		".pushsection __rseq_sections, \"a\"\n\t"
		".quad 1b, 2b, 1b\n\t"
		".popsection\n\t"
		:
		: "r" (cpu), "m" (__rseq_current_cpu),
		  "r" (old), "m" (*p), "r" (newv),
		  "r" (check_val), "m" (*check_ptr),
		  "r" (check_val2), "m" (*check_ptr2)
		: "memory"
		: fail);
	return 0;
fail:
	return -1;
}

namespace tcmalloc {

//-------------------------------------------------------------------
// Data kept per thread
//-------------------------------------------------------------------

class ThreadCache {
 public:
#ifdef HAVE_TLS
  enum { have_tls = true };
#else
  enum { have_tls = false };
#endif

  // All ThreadCache objects are kept in a linked list (for stats collection)
  ThreadCache* next_;
  ThreadCache* prev_;

  void Init(pthread_t tid);
  void Cleanup();

  // Accessors (mostly just for printing stats)
  int freelist_length(size_t cl) const { return list_[cl].length(); }

  // Total byte size in cache
  size_t Size() const { return size_; }

  // Allocate an object of the given size and class. The size given
  // must be the same as the size of the class in the size map.
  void* AllocateThread(size_t size, size_t cl);
  void DeallocateThread(void* ptr, size_t size_class);
  static void* Allocate(ThreadCache* heap, size_t size, size_t cl);
  static void Deallocate(ThreadCache* heap, void* ptr, size_t size_class);

  void Scavenge();

  int GetSamplePeriod();

  // Record allocation of "k" bytes.  Return true iff allocation
  // should be sampled
  bool SampleAllocation(size_t k);

  static void         InitModule();
  static void         InitTSD();
  static ThreadCache* GetThreadHeap();
  static ThreadCache* GetCache();
  static ThreadCache* GetCacheIfPresent();
  static ThreadCache* GetCacheWhichMustBePresent();
  static ThreadCache* CreateCacheIfNecessary();
  static void         BecomeIdle();
  static size_t       MinSizeForSlowPath();
  static void         SetMinSizeForSlowPath(size_t size);

  static bool IsFastPathAllowed() { return MinSizeForSlowPath() != 0; }

  // Return the number of thread heaps in use.
  static inline int HeapsInUse();

  // Adds to *total_bytes the total number of bytes used by all thread heaps.
  // Also, if class_count is not NULL, it must be an array of size kNumClasses,
  // and this function will increment each element of class_count by the number
  // of items in all thread-local freelists of the corresponding size class.
  // REQUIRES: Static::pageheap_lock is held.
  static void GetThreadStats(uint64_t* total_bytes, uint64_t* class_count);

  // Sets the total thread cache size to new_size, recomputing the
  // individual thread cache sizes as necessary.
  // REQUIRES: Static::pageheap lock is held.
  static void set_overall_thread_cache_size(size_t new_size);
  static size_t overall_thread_cache_size() {
    return overall_thread_cache_size_;
  }

 private:
  class FreeList {
   private:
    void*    list_;       // Linked list of nodes

#ifdef _LP64
    // On 64-bit hardware, manipulating 16-bit values may be slightly slow.
   public:
    uint64_t getLength() const {
      return (unsigned long)list_ >> 48;
    }
   private:
    void* getList() const {
      return (void*)((unsigned long)list_ & 0x0000FFFFFFFFFFFF);
    }

    uint32_t lowater_;     // Low water mark for list length.
    uint32_t max_length_;  // Dynamic max list length based on usage.
    // Tracks the number of times a deallocation has caused
    // length_ > max_length_.  After the kMaxOverages'th time, max_length_
    // shrinks and length_overages_ is reset to zero.
    uint32_t length_overages_;
#else
    // If we aren't using 64-bit pointers then pack these into less space.
    uint16_t length_;
    uint16_t lowater_;
    uint16_t max_length_;
    uint16_t length_overages_;
#endif

   public:
    void Init() {
      list_ = NULL;
      lowater_ = 0;
      max_length_ = 1;
      length_overages_ = 0;
    }

    // Return current length of list
    size_t length() const {
      return getLength();
    }

    // Return the maximum length of the list.
    size_t max_length() const {
      return max_length_;
    }

    // Set the maximum length of the list.  If 'new_max' > length(), the
    // client is responsible for removing objects from the list.
    void set_max_length(size_t new_max) {
      max_length_ = new_max;
    }

    // Return the number of times that length() has gone over max_length().
    size_t length_overages() const {
      return length_overages_;
    }

    void set_length_overages(size_t new_count) {
      length_overages_ = new_count;
    }

    // Is list empty?
    bool empty() const {
      return list_ == NULL;
    }

    // Low-water mark management
    int lowwatermark() const { return lowater_; }
    void clear_lowwatermark() { lowater_ = getLength(); }

    void Push(void* ptr) {
      void* list = getList();
      SLL_Push(&list, ptr);
      list_ = (void*)(((getLength() + 1) << 48) | (unsigned long)list);
    }

    bool PushCheck(void* node, int cpu, intptr_t* l) {
      void* old = list_;
      SLL_SetNext(node, (void*)((unsigned long)old & 0x0000FFFFFFFFFFFF));
      node =  (void*)((unsigned long)node | ((getLength() + 1) << 48));
      if (0 == rseq_percpu_cmpxchgcheck(
            cpu,
            (intptr_t *)&list_, (intptr_t)old, (intptr_t)node,
            (intptr_t*)l, 0)){
        ASSERT((int)getLength() >= 0);
        return true;
      }
      return false;
    }

    void* Pop() {
      ASSERT(list_ != NULL);
      void* list = getList();
      if (getLength() < lowater_) lowater_ = getLength();
      auto ret = SLL_Pop(&list);
      list_ = (void*)(((getLength() - 1) << 48) | (unsigned long)list);
      return ret;
    }

    void* PopCheck(int cpu, intptr_t l) {
      //ASSERT(list_ != NULL);
      void* old = list_;
      void* head = (void*)((unsigned long)old & 0x0000FFFFFFFFFFFF);
      if (head == nullptr) {
        return nullptr;
      }
      void* next = SLL_Next(head);
      void* tnext = (void*)((unsigned long)next | ((getLength() - 1) << 48));
      if (0 == rseq_percpu_cmpxchgcheckcheck(
            cpu,
            (intptr_t*)&list_, (intptr_t)old, (intptr_t)tnext,
            (intptr_t *)(reinterpret_cast<void**>(head)), (intptr_t)next,
            (intptr_t*)l, (intptr_t)0)) {
        ASSERT((int)getLength() >= 0);
        if (getLength() < lowater_) lowater_ = getLength();
        return head;
      }

      return nullptr;
    }

    void* Next() {
      void* list = getList();
      return SLL_Next(&list);
    }

    void PushRange(int N, void *start, void *end) {
      void * list = getList();
      SLL_PushRange(&list, start, end);
      list_ = (void*)((unsigned long)list | ((getLength() + N) << 48));
    }

    int PopRange(int N, void **start, void **end) {
      void * list = getList();
      int ret = SLL_PopRange(&list, N, start, end);
      ASSERT(getLength() >= ret);
      list_ = (void*)((unsigned long)list | ((getLength() - ret) << 48));
      if (getLength() < lowater_) lowater_ = getLength();
      return ret;
    }
  };

  // Gets and returns an object from the central cache, and, if possible,
  // also adds some objects of that size class to this thread cache.
  void* FetchFromCentralCache(size_t cl, size_t byte_size);

  // Releases some number of items from src.  Adjusts the list's max_length
  // to eventually converge on num_objects_to_move(cl).
  void ListTooLong(FreeList* src, size_t cl);

  // Releases N items from this thread cache.
  void ReleaseToCentralCache(FreeList* src, size_t cl, int N);

  // Increase max_size_ by reducing unclaimed_cache_space_ or by
  // reducing the max_size_ of some other thread.  In both cases,
  // the delta is kStealAmount.
  void IncreaseCacheLimit();
  // Same as above but requires Static::pageheap_lock() is held.
  void IncreaseCacheLimitLocked();

  // If TLS is available, we also store a copy of the per-thread object
  // in a __thread variable since __thread variables are faster to read
  // than pthread_getspecific().  We still need pthread_setspecific()
  // because __thread variables provide no way to run cleanup code when
  // a thread is destroyed.
  // We also give a hint to the compiler to use the "initial exec" TLS
  // model.  This is faster than the default TLS model, at the cost that
  // you cannot dlopen this library.  (To see the difference, look at
  // the CPU use of __tls_get_addr with and without this attribute.)
  // Since we don't really use dlopen in google code -- and using dlopen
  // on a malloc replacement is asking for trouble in any case -- that's
  // a good tradeoff for us.
#ifdef HAVE___ATTRIBUTE__
#define ATTR_INITIAL_EXEC __attribute__ ((tls_model ("initial-exec")))
#else
#define ATTR_INITIAL_EXEC
#endif

#ifdef HAVE_TLS
  struct ThreadLocalData {
    ThreadCache* heap;
    // min_size_for_slow_path is 0 if heap is NULL or kMaxSize + 1 otherwise.
    // The latter is the common case and allows allocation to be faster
    // than it would be otherwise: typically a single branch will
    // determine that the requested allocation is no more than kMaxSize
    // and we can then proceed, knowing that global and thread-local tcmalloc
    // state is initialized.
    size_t min_size_for_slow_path;
  };
  static __thread ThreadLocalData threadlocal_data_ ATTR_INITIAL_EXEC;
#endif

  // Thread-specific key.  Initialization here is somewhat tricky
  // because some Linux startup code invokes malloc() before it
  // is in a good enough state to handle pthread_keycreate().
  // Therefore, we use TSD keys only after tsd_inited is set to true.
  // Until then, we use a slow path to get the heap object.
  static bool tsd_inited_;
  static pthread_key_t heap_key_;

  // Linked list of heap objects.  Protected by Static::pageheap_lock.
  static ThreadCache* thread_heaps_;
  static ThreadCache* cpu_heaps_[32];
  static std::atomic<int> thread_count_;
  static bool go_percpu_;
  static int thread_heap_count_;

  // A pointer to one of the objects in thread_heaps_.  Represents
  // the next ThreadCache from which a thread over its max_size_ should
  // steal memory limit.  Round-robin through all of the objects in
  // thread_heaps_.  Protected by Static::pageheap_lock.
  static ThreadCache* next_memory_steal_;

  // Overall thread cache size.  Protected by Static::pageheap_lock.
  static size_t overall_thread_cache_size_;

  // Global per-thread cache size.  Writes are protected by
  // Static::pageheap_lock.  Reads are done without any locking, which should be
  // fine as long as size_t can be written atomically and we don't place
  // invariants between this variable and other pieces of state.
  static volatile size_t per_thread_cache_size_;

  // Represents overall_thread_cache_size_ minus the sum of max_size_
  // across all ThreadCaches.  Protected by Static::pageheap_lock.
  static ssize_t unclaimed_cache_space_;

  // This class is laid out with the most frequently used fields
  // first so that hot elements are placed on the same cache line.

  long lock_;
  std::mutex m_;
  std::condition_variable cv_;
  size_t        size_;                  // Combined size of data
  size_t        scavenge_count_;
  size_t        max_size_;              // size_ > max_size_ --> Scavenge()

  // We sample allocations, biased by the size of the allocation
  Sampler       sampler_;               // A sampler

  FreeList      list_[kNumClasses];     // Array indexed by size-class

  pthread_t     tid_;                   // Which thread owns it
  bool          in_setspecific_;        // In call to pthread_setspecific?

  // Allocate a new heap. REQUIRES: Static::pageheap_lock is held.
  static ThreadCache* NewHeap(pthread_t tid);

  // Use only as pthread thread-specific destructor function.
  static void DestroyThreadCache(void* ptr);

  static void DeleteCache(ThreadCache* heap);
  static void RecomputePerThreadCacheSize();

  // Ensure that this class is cacheline-aligned. This is critical for
  // performance, as false sharing would negate many of the benefits
  // of a per-thread cache.
} CACHELINE_ALIGNED;

// Allocator for thread heaps
// This is logically part of the ThreadCache class, but MSVC, at
// least, does not like using ThreadCache as a template argument
// before the class is fully defined.  So we put it outside the class.
extern PageHeapAllocator<ThreadCache> threadcache_allocator;

inline int ThreadCache::HeapsInUse() {
  return threadcache_allocator.inuse();
}

inline bool ThreadCache::SampleAllocation(size_t k) {
  return sampler_.SampleAllocation(k);
}

inline void* ThreadCache::AllocateThread(size_t size, size_t cl) {
  ASSERT(size <= kMaxSize);
  ASSERT(size == Static::sizemap()->ByteSizeForClass(cl));

  FreeList* list = &list_[cl];
  if (UNLIKELY(list->empty())) {
    return FetchFromCentralCache(cl, size);
  }
  size_ -= size;
  return list->Pop();
}

inline void* ThreadCache::Allocate(ThreadCache* heap, size_t size, size_t cl) {
  if (!go_percpu_) {
    return heap->AllocateThread(size, cl);
  }

  ASSERT(size <= kMaxSize);
  ASSERT(size == Static::sizemap()->ByteSizeForClass(cl));
  void* ret = nullptr;
  int cpu;
  FreeList* list;

  retry:
    do {
      cpu = rseq_current_cpu();
      heap = cpu_heaps_[cpu];
      list = &heap->list_[cl];

      if (heap->lock_) {
        std::unique_lock<std::mutex> lk(heap->m_);
        heap->cv_.wait(lk, [&]{return !heap->lock_;});
        continue;
      }

      if (list->empty()) {
        break;
      }
      ret = list->PopCheck(cpu, (intptr_t)&heap->lock_);
    } while (!ret);

    if (!ret) {

      bool expected = false;
      if (0 == rseq_percpu_cmpxchg(cpu, (intptr_t*)&heap->lock_, (intptr_t)0, (intptr_t)1)) {


        ret = heap->FetchFromCentralCache(cl, size);

        std::unique_lock<std::mutex> lk(heap->m_);
        heap->lock_ = false;
        heap->cv_.notify_all();
        return ret;
      } else {
        goto retry;
      }
    }

    if (ret) {
      heap->size_ -= Static::sizemap()->ByteSizeForClass(cl);
    }


    return ret;
}

inline void ThreadCache::DeallocateThread(void* ptr, size_t cl) {
  FreeList* list = &list_[cl];
  size_ += Static::sizemap()->ByteSizeForClass(cl);
  ssize_t size_headroom = max_size_ - size_ - 1;

  // This catches back-to-back frees of allocs in the same size
  // class. A more comprehensive (and expensive) test would be to walk
  // the entire freelist. But this might be enough to find some bugs.
  ASSERT(ptr != list->Next());

  list->Push(ptr);
  ssize_t list_headroom =
      static_cast<ssize_t>(list->max_length()) - list->length();

  // There are two relatively uncommon things that require further work.
  // In the common case we're done, and in that case we need a single branch
  // because of the bitwise-or trick that follows.
  if (UNLIKELY((list_headroom | size_headroom) < 0)) {
    if (list_headroom < 0) {
      ListTooLong(list, cl);
    }
    if (size_ >= max_size_) Scavenge();
  }
}

inline void ThreadCache::Deallocate(ThreadCache* heap, void* ptr, size_t cl) {
  if (!go_percpu_) {
    return heap->DeallocateThread(ptr, cl);
  }

  int cpu;
  FreeList* list;

  do {
    cpu = rseq_current_cpu();
    heap = cpu_heaps_[cpu];
    list = &heap->list_[cl];

    if (heap->lock_) {
      std::unique_lock<std::mutex> lk(heap->m_);
      heap->cv_.wait(lk, [&]{return !heap->lock_;});
      continue;
    }

    // This catches back-to-back frees of allocs in the same size
    // class. A more comprehensive (and expensive) test would be to walk
    // the entire freelist. But this might be enough to find some bugs.
    ASSERT(ptr != list->Next());
    if (list->PushCheck(ptr, cpu, (intptr_t*)&heap->lock_)) {
      break;
    }
  } while (true);
  heap->size_ += Static::sizemap()->ByteSizeForClass(cl);

  if (list->length() > list->max_length() ||
      ++heap->scavenge_count_ >= 100000) {
    if (0 != rseq_percpu_cmpxchg(
          cpu,
          (intptr_t*)&heap->lock_, (intptr_t)0, (intptr_t)1)) {
      printf("lock miss\n");
      return;
    }

    if (list->length() > list->max_length()) {
      heap->ListTooLong(list, cl);
    } else {
      long sz = 0;
      for (int cl = 0; cl < kNumClasses; cl++) {
        size_t bytes = Static::sizemap()->ByteSizeForClass(cl);
        sz += heap->list_[cl].getLength() * bytes;
      }
      if (sz > heap->max_size_ * 16 * (thread_count_/32 + 1)) {
        heap->Scavenge();
      }
    }
    heap->scavenge_count_ = 0;
    std::unique_lock<std::mutex> lk(heap->m_);
    heap->lock_ = false;
    heap->cv_.notify_all();
  }
}

inline ThreadCache* ThreadCache::GetThreadHeap() {

  if (go_percpu_) {
    return cpu_heaps_[rseq_current_cpu()];
  }

#ifdef HAVE_TLS
  return threadlocal_data_.heap;
#else
  return reinterpret_cast<ThreadCache *>(
      perftools_pthread_getspecific(heap_key_));
#endif
}

inline ThreadCache* ThreadCache::GetCacheWhichMustBePresent() {

  if (go_percpu_) {
    return cpu_heaps_[rseq_current_cpu()];
  }

#ifdef HAVE_TLS
  ASSERT(threadlocal_data_.heap);
  return threadlocal_data_.heap;
#else
  ASSERT(perftools_pthread_getspecific(heap_key_));
  return reinterpret_cast<ThreadCache *>(
      perftools_pthread_getspecific(heap_key_));
#endif
}


inline ThreadCache* ThreadCache::GetCache() {
  ThreadCache* ptr = NULL;
  if (!tsd_inited_) {
    InitModule();
  } else {
    ptr = GetThreadHeap();
  }
  if (ptr == NULL) ptr = CreateCacheIfNecessary();
  return ptr;
}

// In deletion paths, we do not try to create a thread-cache.  This is
// because we may be in the thread destruction code and may have
// already cleaned up the cache for this thread.
inline ThreadCache* ThreadCache::GetCacheIfPresent() {
  if (!tsd_inited_) return NULL;
  return GetThreadHeap();
}

inline size_t ThreadCache::MinSizeForSlowPath() {
#ifdef HAVE_TLS
  return threadlocal_data_.min_size_for_slow_path;
#else
  return 0;
#endif
}

inline void ThreadCache::SetMinSizeForSlowPath(size_t size) {
#ifdef HAVE_TLS
  threadlocal_data_.min_size_for_slow_path = size;
#endif
}

}  // namespace tcmalloc

#endif  // TCMALLOC_THREAD_CACHE_H_
