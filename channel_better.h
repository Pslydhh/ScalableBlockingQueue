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
#include <limits.h>

#include <cassert>
#include <cstddef>
#include <iostream>
#include <mutex>
#include <stack>
#include <vector>

#include "align.h"
#include "primitives.h"

template <typename T>
class ChannelBetter {
    static_assert(sizeof(uintptr_t) <= sizeof(void*),
                  "void* pointer can hold every data pointer, So Its size at least as uintptr_t");

public:
    static constexpr int NODE_SIZE = 1 << 8;
    static constexpr int NODE_BITS = NODE_SIZE - 1;

    struct Cell {
        Cell() : data_field(), control_field(0) {}
        T data_field;

        uint32_t control_field;
    };

    struct alignas(PAGE_SIZE) node_t {
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
        int32_t flag;
        node_t* spare;

        uint64_t put_epoch_id CACHE_ALIGNED;
        uint64_t pop_epoch_id CACHE_ALIGNED;

        uint64_t put_node_id CACHE_ALIGNED;
        uint64_t pop_node_id CACHE_ALIGNED;
    };

    static inline node_t *ob_new_node() {
        node_t* n = new node_t();
        return n;
    }

    ChannelBetter(int threshold = 8)
            : init_node(ob_new_node()),
              put_node(init_node),
              pop_node(init_node),
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

    ~ChannelBetter() {
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

    node_t* init_node DOUBLE_CACHE_ALIGNED;
    node_t* put_node DOUBLE_CACHE_ALIGNED;
    node_t* pop_node DOUBLE_CACHE_ALIGNED;

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
        get_thread_handle(ChannelBetter* q) {
            while (handles_vector.size() <= q->id) {
                handle_t* th = (handle_t*)malloc(sizeof(handle_t));
                memset(th, 0, sizeof(handle_t));
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
                    STORE(&th->pop_node_id, LOADcs(&q->pop_node)->id);
                    STORE(&th->pop_epoch_id, -1);
                    q->deq_handles[q->deq_handles_size++] = th;
                } else {
                    STORE(&th->put_node_id, LOADcs(&q->put_node)->id);
                    STORE(&th->put_epoch_id, -1);
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
    template <bool is_consumer>
    Cell* ob_find_cell(node_t* node, node_t** ptr, long i, handle_t* th) {
        // get current node
        node_t* curr = node;

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
                if (CAScs(&curr->next, &next, temp)) {
                    next = temp;
                    // now thread there is no standby node.
                    th->spare = nullptr;

                    if (LOAD(ptr) == curr) {
                        CASra(ptr, &curr, temp);
                    }
                } else {
                    if (LOAD(ptr) == curr) {
                        CASra(ptr, &curr, next);
                    }
                }
            } else {
                if (LOAD(ptr) == curr) {
                    CASra(ptr, &curr, next);
                }
            }

            // take the next node.
            curr = next;
        }
        // update our node to the present node.
        // STORE(ptr, curr);

        //while (old->id < curr->id && !CASra(ptr, &old, curr));

        if constexpr (is_consumer) {
            th->pop_node_id = curr->id;
        } else {
            th->put_node_id = curr->id;
        }

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
        STORE(&th->put_epoch_id, th->put_node_id);

        node_t* local_put_node = LOADcs(&this->put_node);
        Cell* c = ob_find_cell<false>(local_put_node, &this->put_node, FAA(&put_index, 1),
                                      th); // now c is the nedded cell
        uint32_t cv;
        /* if XCHG(ATOMIC: XCHG—Exchange Register/Memory with Register) 
            return nullptr, so our value has put into the cell, just return.*/
        c->data_field = v;
        if ((cv = XCHG(&c->control_field, 2)) != 0) {
            /* else the couterpart pop thread has wait this cell, so we just change the wati'value to 0 and wake it*/
            ob_futex_wake(&c->control_field, 1);
        }

        RELEASE(&th->put_epoch_id, -1);
    }

    T blocking_get() {
        handle_t* th = this->get_thread_handle<true>();
        STORE(&th->pop_epoch_id, th->pop_node_id);

        int times;
        long index;

        node_t* local_pop_node = LOADcs(&this->pop_node);
        Cell* c =
                ob_find_cell<true>(local_pop_node, &this->pop_node, index = FAA(&pop_index, 1), th);
        uint32_t cv;
        // because the queue is a blocking queue, so we just use more spin.
        times = (1 << 1);
        do {
            cv = LOAD(&c->control_field);
            if (cv) {
                LOADa(&c->control_field);
                goto over;
            }
            PAUSE();
        } while (times-- > 0);
        if ((cv = XCHG(&c->control_field, 1)) == 0) {
            do {
                ob_futex_wait(&c->control_field, 1);
            } while (LOAD(&c->control_field) == 1);
            LOADa(&c->control_field);
        }
    over:
        /* if the index is the node's last cell: (NODE_BITS == 4095), it Try to reclaim the memory.
        * so we just take the smallest ID node that is not reclaimed(init_node), and At the same time, by traversing     
        * the local data of other threads, we get a larger ID node(min_node). 
        * So it is safe to recycle the memory [init_node, min_node).
        */
        if ((index & NODE_BITS) == NODE_BITS) {
            long init_index = ACQUIRE(&this->init_id);
            if ((LOADcs(&this->pop_node)->id - init_index) >= this->threshold && init_index >= 0 &&
                CAScs(&this->init_id, &init_index, -1)) {
                uint64_t min_version;
                node_t* init_node;
                node_t* local_init_node;
                {
                    std::lock_guard<std::mutex> m(this->mutex);

                    uint64_t queue_pop_id = LOADcs(&this->pop_node)->id;
                    uint64_t queue_put_id = LOADcs(&this->put_node)->id;

                    min_version = queue_pop_id < queue_put_id ? queue_pop_id : queue_put_id;
                    {
                        for (int i = 0; i < this->deq_handles_size; ++i) {
                            handle_t* next = this->deq_handles[i];
                            uint64_t next_node = LOADcs(&next->pop_epoch_id);
                            if (next_node < min_version) {
                                min_version = next_node;
                            }
                        }
                    }

                    {
                        for (int i = 0; i < this->enq_handles_size; ++i) {
                            handle_t* next = this->enq_handles[i];
                            uint64_t next_node = LOADcs(&next->put_epoch_id);
                            if (next_node < min_version) {
                                min_version = next_node;
                            }
                        }
                    }

                    if (min_version > ((uint64_t) init_index)) {
                        init_node = this->init_node;
                        local_init_node = init_node;
                        do { local_init_node = local_init_node->next;
                        } while (((uint64_t) local_init_node->id) != min_version);

                        this->init_node = local_init_node;
                    }
                }

                if (min_version > ((uint64_t) init_index)) {
                    RELEASE(&this->init_id, min_version);
                    while (init_node != local_init_node) {
                        node_t* tmp = init_node->next;
                        delete init_node;
                        init_node = tmp;
                    }
                } else {
                    RELEASE(&this->init_id, init_index);
                }
            }
        }

        T v = c->data_field;
        RELEASE(&th->pop_epoch_id, -1);
        return v;
    }
};

template <typename T>
typename ChannelBetter<T>::IdAllocatoT ChannelBetter<T>::id_allocator;

