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
class ScalableBoundedBlockingQueue {
    static_assert(sizeof(uintptr_t) <= sizeof(void*),
                  "void* pointer can hold every data pointer, So Its size at least as uintptr_t");

public:
    static constexpr int NODE_SIZE = 1 << 8;
    static constexpr int NODE_BITS = NODE_SIZE - 1;

    struct Cell {
        Cell() : data_field(), control_field(0), put_version_field(0), pop_version_field(0) {}
        T data_field;

        int64_t control_field;
        int64_t put_version_field;
        int64_t pop_version_field;
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

    ScalableBoundedBlockingQueue(int threshold = 8)
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

    ~ScalableBoundedBlockingQueue() {
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
        get_thread_handle(ScalableBoundedBlockingQueue* q) {
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
        int64_t put_index_local = FAA(&put_index, 1);
        Cell* c = ob_find_cell(&th->put_node, put_index_local, th);

        int64_t put_version_field_local;
        while ((put_version_field_local = LOADcs(&c->put_version_field)) <
               (put_index_local / NODE_SIZE)) {
            ob_futex_wait(&c->put_version_field, put_version_field_local);
        }

        c->data_field = v;

        int64_t local;
        if ((local = XCHG(&c->control_field, 2)) != 0) {
            ob_futex_wake(&c->control_field, 1);
            assert(local == 1);
        }
    }

    T blocking_get() {
        handle_t* th = this->get_thread_handle<true>();
        int64_t pop_index_local = FAA(&pop_index, 1);
        Cell* c = ob_find_cell(&th->pop_node, pop_index_local, th);

        int64_t pop_version_field_local;
        while ((pop_version_field_local = LOADcs(&c->pop_version_field)) <
               (pop_index_local / NODE_SIZE)) {
            ob_futex_wait(&c->pop_version_field, pop_version_field_local);
        }

        int times = (1 << 5);
        do {
            int cv = LOAD(&c->control_field);
            if (cv) {
                LOADa(&c->control_field);
                goto over;
            }
            PAUSE();
        } while (times-- > 0);

        int64_t local;
        if ((local = XCHG(&c->control_field, 1)) == 0) {
            do {
                ob_futex_wait(&c->control_field, 1);
            } while (LOAD(&c->control_field) == 1);
            LOADa(&c->control_field);
        } else {
            assert(local == 2);
        }

    over:
        STORE(&c->control_field, 0);
        T local_result = c->data_field;

        local = LOAD(&c->put_version_field);
        STOREcs(&c->put_version_field, local + 1);
        if ((LOADcs(&put_index) - pop_index_local) > NODE_SIZE) {
            ob_futex_wake(&c->put_version_field, INT_MAX);
        }

        local = LOAD(&c->pop_version_field);
        STOREcs(&c->pop_version_field, local + 1);
        if ((LOADcs(&pop_index) - pop_index_local) > NODE_SIZE) {
            ob_futex_wake(&c->pop_version_field, INT_MAX);
        }

        return local_result;
    }
};

template <typename T>
typename ScalableBoundedBlockingQueue<T>::IdAllocatoT ScalableBoundedBlockingQueue<T>::id_allocator;
