// Copyright 2021 Pslydhh. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
#pragma once

#include <linux/futex.h>
#include <pthread.h>
#include <syscall.h>
#include <unistd.h>

#include <mutex>
#include <vector>

#include "align.h"
#include "primitives.h"
#define NODE_SIZE (1 << 10)
#define N NODE_SIZE
#define NBITS (N - 1)
#define BOT ((void*)0)
#define TOP ((void*)-1)

// Support 1024 threads.
#define HANDLES 1024
struct QuickBlockQueue {
    struct node_t {
        node_t* next DOUBLE_CACHE_ALIGNED;
        long id DOUBLE_CACHE_ALIGNED;
        void* cells[NODE_SIZE] DOUBLE_CACHE_ALIGNED;
    };

    class IdAllocatoT {
    public:
        IdAllocatoT() : upper_bound(0), freed(), lock() {}
        size_t allocate() {
            std::lock_guard<std::mutex> guard(lock);
            if (freed.empty()) {
                return upper_bound++;
            } else {
                size_t id = freed.top();
                freed.pop();
                return id;
            }
        }

        void deallocate(size_t id) {
            std::lock_guard<std::mutex> guard(lock);
            freed.push(id);
        }

    private:
        uint32_t upper_bound;
        std::stack<uint32_t, std::vector<uint32_t>> freed;

        // TODO: use concurrent_stack or concurrent_queue
        std::mutex lock;
    };

    class ConcurrencyControP {
    public:
        ConcurrencyControP(int32_t flag = 0) : flag(flag) {}

        std::atomic<int32_t> flag;
    };

    struct handle_t {
        ConcurrencyControP control;
        node_t* spare;

        std::atomic<node_t*> put_node CACHE_ALIGNED;
        std::atomic<node_t*> pop_node CACHE_ALIGNED;
    };

    static inline node_t* ob_new_node() {
        node_t* n = reinterpret_cast<node_t*>(align_malloc(PAGE_SIZE, sizeof(node_t)));
        memset(n, 0, sizeof(node_t));
        return n;
    }

    QuickBlockQueue(int threshold = 8)
            : init_node(ob_new_node()),
              init_id(0),
              put_index(0),
              pop_index(0),
              enq_handles(),
              enq_handles_size(0),
              futex_addr(1),
              deq_handles(),
              deq_handles_size(0),
              threshold(threshold),
              mutex(),
              id(id_allocator.allocate()) {}

    ~QuickBlockQueue() {
        for (int i = 0; i < enq_handles_size; ++i) {
            auto* handle = enq_handles[i];
            auto& control = handle->control;
            int32_t flag = 0;
            if (control.flag.load() == flag && control.flag.compare_exchange_strong(flag, -1)) {
            } else {
                (&handle->control)->~ConcurrencyControP();
                free(handle->spare);
                free(handle);
            }
        }

        for (int i = 0; i < deq_handles_size; ++i) {
            auto* handle = deq_handles[i];
            auto& control = handle->control;
            int32_t flag = 0;
            if (control.flag.load() == flag && control.flag.compare_exchange_strong(flag, -1)) {
            } else {
                (&handle->control)->~ConcurrencyControP();
                free(handle->spare);
                free(handle);
            }
        }

        id_allocator.deallocate(id);

        do {
            node_t* node = init_node;
            init_node = node->next;
            free(node);
        } while (init_node != nullptr);
    }

    node_t* init_node;
    long init_id DOUBLE_CACHE_ALIGNED;

    std::atomic<int64_t> put_index DOUBLE_CACHE_ALIGNED;
    std::atomic<int64_t> pop_index DOUBLE_CACHE_ALIGNED;

    handle_t* enq_handles[HANDLES];
    int enq_handles_size;
    int futex_addr;

    handle_t* deq_handles[HANDLES];
    int deq_handles_size;

    int threshold;

    std::mutex mutex;
    static IdAllocatoT id_allocator;
    const uint32_t id;

    class HandleAggregate {
    public:
        HandleAggregate() : handles_vector() {}

        template <bool is_consumer>
        handle_t* get_thread_handle(QuickBlockQueue* q) {
            while (handles_vector.size() <= q->id) {
                handle_t* th = (handle_t*)malloc(sizeof(handle_t));

                memset(th, 0, sizeof(handle_t));
                new (&th->control) ConcurrencyControP(-2);

                handles_vector.push_back(th);
            }

            auto th = handles_vector[q->id];
            if (th->control.flag.load(std::memory_order_relaxed) < 0) {
                th->control.flag.store(0, std::memory_order_relaxed);

                if (th->spare == nullptr) {
                    th->spare = ob_new_node();
                }

                std::lock_guard<std::mutex> m(q->mutex);
                if constexpr (!is_consumer) {
                    th->put_node.store(q->init_node, std::memory_order_relaxed);
                    q->enq_handles[q->enq_handles_size++] = th;
                } else {
                    th->pop_node.store(q->init_node, std::memory_order_relaxed);
                    q->deq_handles[q->deq_handles_size++] = th;
                }
            }

            return th;
        }

        ~HandleAggregate() {
            for (auto iter = handles_vector.begin(); iter != handles_vector.end(); ++iter) {
                auto handle = *iter;
                auto& control = handle->control;

                int32_t flag = 0;
                if (control.flag.load() == -2) {
                    (&handle->control)->~ConcurrencyControP();
                    free(handle->spare);
                    free(handle);
                } else if (control.flag.load() == flag &&
                           control.flag.compare_exchange_strong(flag, -1)) {
                    //control.flag.store(-1);
                } else {
                    (&handle->control)->~ConcurrencyControP();
                    free(handle->spare);
                    free(handle);
                }
            }
        }

        std::vector<handle_t*> handles_vector;
    };

    template <bool is_consumer>
    handle_t* get_thread_handle() {
        thread_local HandleAggregate aggregate;
        handle_t* handle = aggregate.get_thread_handle<is_consumer>(this);
        return handle;
    }

    /*
    * ob_find_cell: This is our core operation, locating the offset on the nodes and nodes needed.
    */
    static void* ob_find_cell(std::atomic<node_t*>& ptr, long i, handle_t* th) {
        // get current node
        node_t* curr = ptr.load(std::memory_order_relaxed);
        /*j is thread's local node'id(put node or pop node), (i / N) is the cell needed node'id.
        and we shoud take it, By filling the nodes between the j and (i / N) through 'next' field*/
        long j = curr->id;
        for (; j < i / N; ++j) {
            node_t* next = ACQUIRE(&curr->next);
            // next is nullptr, so we Start filling.
            if (next == nullptr) {
                //if (i == (j + 1) * N) {
                    // use thread's standby node.
                    node_t* temp = th->spare;
                    if (!temp) {
                        temp = ob_new_node();
                        th->spare = temp;
                    }
                    // next node's id is j + 1.
                    temp->id = j + 1;
                    // if true, then use this thread's node, else then thread has have done this.
                    if (CASra(&curr->next, &next, temp)) {
                        next = temp;
                        // now thread there is no standby node.
                        th->spare = nullptr;
                    }
                /*
                } else {
                    while ((next = ACQUIRE(&curr->next)) == nullptr) {
                        PAUSE();
                    }
                }
                */
            }
            // take the next node.
            curr = next;
        }
        // update our node to the present node.
        ptr.store(curr, std::memory_order_relaxed);
        // Orders processor execution, so other thread can see the '*ptr = curr'.
        // asm volatile ("sfence" ::: "cc", "memory");
        // std::atomic_thread_fence(std::memory_order_seq_cst);
        // now we get the needed cell, its' node is curr and index is i % N.
        return &curr->cells[i % N];
    }

    static int ob_futex_wake(void* addr, int val) {
        return syscall(SYS_futex, addr, FUTEX_WAKE, val, NULL, NULL, 0);
    }

    static int ob_futex_wait(void* addr, int val) {
        return syscall(SYS_futex, addr, FUTEX_WAIT, val, NULL, NULL, 0);
    }

    void put(void* v) {
        handle_t* th = this->get_thread_handle<false>();
        // FAAcs(&this->put_index, 1) return the needed index.
        void** c = reinterpret_cast<void**>(
                ob_find_cell(th->put_node, this->put_index.fetch_add(1, std::memory_order_relaxed),
                             th)); // now c is the nedded cell
        void* cv;
        /* if XCHG(ATOMIC: XCHG—Exchange Register/Memory with Register) 
            return BOT, so our value has put into the cell, just return.*/
        if ((cv = XCHG(c, v)) == BOT) return;
        /* else the couterpart pop thread has wait this cell, so we just change the wati'value to 0 and wake it*/
        *((int*)cv) = 0;
        ob_futex_wake(cv, 1);
    }

    void* blocking_get() {
        handle_t* th = this->get_thread_handle<true>();
        int times;
        void* cv;
        // index is the needed pop_index.
        long index = this->pop_index.load(std::memory_order_relaxed);
        this->pop_index.store(index + 1, std::memory_order_relaxed);
        // locate the needed cell.
        void** c = reinterpret_cast<void**>(ob_find_cell(th->pop_node, index, th));
        // because the queue is a blocking queue, so we just use more spin.
        times = (1 << 0);
        do {
            cv = LOAD(c);
            if (cv) goto over;
            PAUSE();
        } while (times-- > 0);
        // XCHG, if return BOT so this cell is NULL, we just wait and observe the futex_addr'value to 0.
        if ((cv = XCHG(c, &this->futex_addr)) == BOT) {
            // call wait before compare futex_addr to prevent use-after-free of futex_addr at ob_enqueue(call wake);
            do {
                ob_futex_wait(&this->futex_addr, 1);
            } while (this->futex_addr == 1);
            this->futex_addr = 1;
            // the couterpart put thread has change futex_addr's value to 0. and the data has into cell(c).
            cv = *c;
        }
    over:
        /* if the index is the node's last cell: (NBITS == 4095), it Try to reclaim the memory.
        * so we just take the smallest ID node that is not reclaimed(init_node), and At the same time, by traversing     
        * the local data of other threads, we get a larger ID node(min_node). 
        * So it is safe to recycle the memory [init_node, min_node).
        */
        if ((index & NBITS) == NBITS) {
            long init_index = ACQUIRE(&this->init_id);
            if ((th->pop_node.load(std::memory_order_relaxed)->id - init_index) >=
                        this->threshold &&
                init_index >= 0 && CASa(&this->init_id, &init_index, -1)) {
                std::lock_guard<std::mutex> m(this->mutex);
                node_t* init_node = this->init_node;

                th = this->deq_handles[0];
                node_t* min_node = th->pop_node.load(std::memory_order_relaxed);

                for (int i = 1; i < this->deq_handles_size; ++i) {
                    handle_t* next = this->deq_handles[i];
                    node_t* next_min = next->pop_node.load(std::memory_order_relaxed);
                    if (next_min->id < min_node->id) min_node = next_min;
                    if (min_node->id <= init_index) break;
                }

                for (int i = 0; i < this->enq_handles_size; ++i) {
                    handle_t* next = this->enq_handles[i];
                    node_t* next_min = next->put_node.load(std::memory_order_relaxed);
                    if (next_min->id < min_node->id) min_node = next_min;
                    if (min_node->id <= init_index) break;
                }

                long new_id = min_node->id;

                //std::cout << "new_id: " << new_id << std::endl;

                if (new_id <= init_index)
                    RELEASE(&this->init_id, init_index);
                else {
                    this->init_node = min_node;
                    RELEASE(&this->init_id, new_id);

                    do {
                        node_t* tmp = init_node->next;
                        free(init_node);
                        init_node = tmp;
                    } while (init_node != min_node);
                }
            }
        }
        return cv;
    }
};
QuickBlockQueue::IdAllocatoT QuickBlockQueue::id_allocator;
