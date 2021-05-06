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

#include <cassert>
#include <cstddef>
#include <iostream>
#include <mutex>
#include <stack>
#include <vector>

#include "align.h"
#include "primitives.h"

template <typename T>
class ScalableBlockingQueue {
    static_assert(sizeof(uintptr_t) <= sizeof(void*),
                  "void* pointer can hold every data pointer, So Its size at least as uintptr_t");

public:
    static constexpr int NODE_SIZE = 1 << 10;
    static constexpr int NODE_BITS = NODE_SIZE - 1;

    struct Cell {
        Cell() : data_field(), control_field(nullptr) {}
        T data_field;
        uint32_t* control_field;
    };

    struct node_t {
        node_t() : next(nullptr), id(0), cells() {}

        node_t* next DOUBLE_CACHE_ALIGNED;
        long id DOUBLE_CACHE_ALIGNED;
        Cell cells[NODE_SIZE] DOUBLE_CACHE_ALIGNED;
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

    struct handle_t {
        uint32_t futex_addr DOUBLE_CACHE_ALIGNED;
        int32_t flag;
        node_t* spare;

        node_t* put_node CACHE_ALIGNED;
        node_t* pop_node CACHE_ALIGNED;
    };

    static inline node_t* ob_new_node() {
        //node_t* n = reinterpret_cast<node_t*>(align_malloc(PAGE_SIZE, sizeof(node_t)));

        node_t* n = new node_t();
        //memset(n, 0, sizeof(node_t));

        return n;
    }

    ScalableBlockingQueue(int threshold = 8)
            : init_node(ob_new_node()),
              init_id(0),
              put_index(0),
              pop_index(0),
              enq_handles(),
              enq_handles_size(0),
              deq_handles(),
              deq_handles_size(0),
              threshold(threshold),
              mutex(),
              id(id_allocator.allocate()) {}

    ~ScalableBlockingQueue() {
        for (int i = 0; i < enq_handles_size; ++i) {
            auto* handle = enq_handles[i];
            int32_t flag = 0;
            if (LOAD(&handle->flag) == flag && CAScs(&handle->flag, &flag, -1)) {
            } else {
                delete handle->spare;
                free(handle);
            }
        }

        for (int i = 0; i < deq_handles_size; ++i) {
            auto* handle = deq_handles[i];
            int32_t flag = 0;
            if (LOAD(&handle->flag) == flag && CAScs(&handle->flag, &flag, -1)) {
            } else {
                delete handle->spare;
                free(handle);
            }
        }

        id_allocator.deallocate(id);

        do {
            node_t* node = init_node;
            init_node = node->next;
            delete node;
        } while (init_node != nullptr);
    }

    node_t* init_node;
    long init_id DOUBLE_CACHE_ALIGNED;

    int64_t put_index DOUBLE_CACHE_ALIGNED;
    int64_t pop_index DOUBLE_CACHE_ALIGNED;

    static constexpr int HANDLES = 1024;

    handle_t* enq_handles[HANDLES];
    int enq_handles_size;

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
        typename std::enable_if<(alignof(std::max_align_t) & 1) == 0, handle_t*>::type
        get_thread_handle(ScalableBlockingQueue* q) {
            while (handles_vector.size() <= q->id) {
                handle_t* th = (handle_t*)malloc(sizeof(handle_t));
                memset(th, 0, sizeof(handle_t));
                th->futex_addr = 1;
                th->flag = -2;

                handles_vector.push_back(th);
            }

            auto th = handles_vector[q->id];
            if (LOAD(&th->flag) < 0) {
                STORE(&th->flag, 0);

                if (th->spare == nullptr) {
                    th->spare = ob_new_node();
                }

                std::lock_guard<std::mutex> m(q->mutex);
                if constexpr (is_consumer) {
                    STORE(&th->pop_node, q->init_node);
                    q->deq_handles[q->deq_handles_size++] = th;
                } else {
                    STORE(&th->put_node, q->init_node);
                    q->enq_handles[q->enq_handles_size++] = th;
                }
            }

            return th;
        }

        ~HandleAggregate() {
            for (auto iter = handles_vector.begin(); iter != handles_vector.end(); ++iter) {
                auto handle = *iter;

                int32_t flag = 0;
                if (LOAD(&handle->flag) == -2) {
                    delete handle->spare;
                    free(handle);
                } else if (LOAD(&handle->flag) == flag && CAScs(&handle->flag, &flag, -1)) {
                } else {
                    delete handle->spare;
                    free(handle);
                }
            }
        }

        std::vector<handle_t*> handles_vector;
    };

    template <bool is_consumer>
    handle_t* get_thread_handle() {
        thread_local HandleAggregate aggregate;
        handle_t* handle = aggregate.template get_thread_handle<is_consumer>(this);
        return handle;
    }

    /*
    * ob_find_cell: This is our core operation, locating the offset on the nodes and nodes needed.
    */
    static Cell* ob_find_cell(node_t** ptr, long i, handle_t* th) {
        // get current node
        node_t* curr = LOAD(ptr);
        /*j is thread's local node'id(put node or pop node), (i / N) is the cell needed node'id.
        and we shoud take it, By filling the nodes between the j and (i / N) through 'next' field*/
        long j = curr->id;
        for (; j < i / NODE_SIZE; ++j) {
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
                // if true, then use this thread's node, else then other thread have done this.
                if (CASra(&curr->next, &next, temp)) {
                    next = temp;
                    // now thread there is no standby node.
                    th->spare = nullptr;
                }
            }
            // take the next node.
            curr = next;
        }
        // update our node to the present node.
        STORE(ptr, curr);
        // Orders processor execution, so other thread can see the '*ptr = curr'.
        // asm volatile ("sfence" ::: "cc", "memory");
        // std::atomic_thread_fence(std::memory_order_seq_cst);
        // now we get the needed cell, its' node is curr and index is i % N.
        return &curr->cells[i % NODE_SIZE];
    }

    static int ob_futex_wake(void* addr, uint32_t val) {
        return syscall(SYS_futex, addr, FUTEX_WAKE, val, NULL, NULL, 0);
    }

    static int ob_futex_wait(void* addr, uint32_t val) {
        return syscall(SYS_futex, addr, FUTEX_WAIT, val, NULL, NULL, 0);
    }

    void put(T v) {
        handle_t* th = this->get_thread_handle<false>();
        // FAAcs(&this->put_index, 1) return the needed index.
        Cell* c = ob_find_cell(&th->put_node, FAA(&put_index, 1), th); // now c is the nedded cell
        uint32_t* cv;
        /* if XCHG(ATOMIC: XCHG—Exchange Register/Memory with Register) 
            return nullptr, so our value has put into the cell, just return.*/
        c->data_field = v;
        if ((cv = XCHG(&c->control_field, (uint32_t*)1)) == nullptr) return;
        /* else the couterpart pop thread has wait this cell, so we just change the wati'value to 0 and wake it*/
        STOREr(cv, 0);
        ob_futex_wake(cv, 1);
    }

    T blocking_get() {
        handle_t* th = this->get_thread_handle<true>();
        int times;
        long index;
        // locate the needed cell.
        Cell* c = ob_find_cell(&th->pop_node, index = FAA(&pop_index, 1), th);
        uint32_t* cv;
        // because the queue is a blocking queue, so we just use more spin.
        times = (1 << 5);
        do {
            cv = LOAD(&c->control_field);
            if (cv) {
                LOADa(&c->control_field);
                goto over;
            }
            PAUSE();
        } while (times-- > 0);
        // XCHG, if return nullptr so this cell is NULL, we just wait and observe the futex_addr'value to 0.
        if ((cv = XCHG(&c->control_field, &th->futex_addr)) == nullptr) {
            // call wait before compare futex_addr to prevent use-after-free of futex_addr at ob_enqueue(call wake);
            do {
                ob_futex_wait(&th->futex_addr, 1);
            } while (LOAD(&th->futex_addr) == 1);
            LOADa(&th->futex_addr);
            STORE(&th->futex_addr, 1);
            // the couterpart put thread has change futex_addr's value to 0. and the data has into cell(c).
            cv = LOAD(&c->control_field);
            assert(cv != nullptr);
        }
    over:
        /* if the index is the node's last cell: (NODE_BITS == 4095), it Try to reclaim the memory.
        * so we just take the smallest ID node that is not reclaimed(init_node), and At the same time, by traversing     
        * the local data of other threads, we get a larger ID node(min_node). 
        * So it is safe to recycle the memory [init_node, min_node).
        */
        if ((index & NODE_BITS) == NODE_BITS) {
            long init_index = ACQUIRE(&this->init_id);
            if ((LOAD(&th->pop_node)->id - init_index) >= this->threshold && init_index >= 0 &&
                CASa(&this->init_id, &init_index, -1)) {
                std::lock_guard<std::mutex> m(this->mutex);
                node_t* init_node = this->init_node;

                th = this->deq_handles[0];
                node_t* min_node = LOAD(&th->pop_node);

                for (int i = 1; i < this->deq_handles_size; ++i) {
                    handle_t* next = this->deq_handles[i];
                    node_t* next_min = LOAD(&next->pop_node);
                    if (next_min->id < min_node->id) min_node = next_min;
                    if (min_node->id <= init_index) break;
                }

                for (int i = 0; i < this->enq_handles_size; ++i) {
                    handle_t* next = this->enq_handles[i];
                    node_t* next_min = LOAD(&next->put_node);
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
                        delete init_node;
                        init_node = tmp;
                    } while (init_node != min_node);
                }
            }
        }
        return c->data_field;
    }
};

template <typename T>
typename ScalableBlockingQueue<T>::IdAllocatoT ScalableBlockingQueue<T>::id_allocator;
