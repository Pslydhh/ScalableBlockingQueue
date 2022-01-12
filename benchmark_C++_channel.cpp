// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <sys/sysinfo.h>
#include <sys/time.h>

#include <chrono>
#include <condition_variable>
#include <deque>
#include <list>
#include <mutex>
#include <set>
#include <thread>

#include "channel.h"
#include "channel_better.h"
#include "fixed_channel.h"
#include "fixed_channel_wrong.h"

static int n_const = 10000000;
static int nthreads_const = 8;

void test_trivial_channel(int count, int num, int num2, int scores) {
    std::cout << "test trivial channel:" << std::endl;
    std::deque<int> queue;
    std::mutex mutex;
    std::condition_variable not_empty;
    std::vector<std::thread> threads;

    {
        auto beginTime = std::chrono::high_resolution_clock::now();
        for (int i = 0; i < num; ++i) {
            threads.emplace_back(
                [&queue, count, num, &mutex, &not_empty, scores]() -> void {
                    for (int j = 0; j < (count / num); ++j) {
                        for (int k = scores - 1; k >= 0; --k) {
                            std::unique_lock<std::mutex> m(mutex);
                            queue.push_back(53211);
                            m.unlock();
                            not_empty.notify_one();
                        }
                    }
                });
        }

        num = num2;
        for (int i = 0; i < num; ++i) {
            threads.emplace_back(
                [&queue, count, num, &mutex, &not_empty, scores]() -> void {
                    for (int j = 0; j < (count / num); ++j) {
                        for (int k = scores - 1; k >= 0; --k) {
                            std::unique_lock<std::mutex> m(mutex);
                            not_empty.wait(
                                m, [&queue]() { return !queue.empty(); });
                            int value = queue.front();
                            queue.pop_front();
                            assert(value == 53211);
                        }
                    }
                });
        }

        for (std::thread& th : threads)
            th.join();
        threads.clear();

        auto endTime = std::chrono::high_resolution_clock::now();
        auto elapsedTime =
            std::chrono::duration_cast<std::chrono::milliseconds>(endTime -
                                                                  beginTime);
        std::cout << "elapsed time is " << elapsedTime.count()
                  << " milliseconds" << std::endl;
    }
}

void test_fixed_channel(int count, int num, int num2, int scores) {
    std::cout << "test fixed channel:" << std::endl;
    FixedChannel<int, 10> qq;
    std::vector<std::thread> threads;

    {
        auto beginTime = std::chrono::high_resolution_clock::now();
        auto q = &qq;

        for (int i = 0; i < num; ++i) {
            threads.emplace_back([count, num, q, scores]() -> void {
                for (int j = 0; j < (count / num); ++j) {
                    for (int k = scores - 1; k >= 0; --k) {
                        // if (!q->put_non_blocking(53211)) {
                        q->put_blocking(53211);
                        //}
                    }
                }
            });
        }

        num = num2;
        for (int i = 0; i < num; ++i) {
            threads.emplace_back([count, num, q, scores]() -> void {
                for (int j = 0; j < (count / num); ++j) {
                    for (int k = scores - 1; k >= 0; --k) {
                        int value;
                        /*for (;;) {
                            bool is_data;
                            value = q->get_non_blocking(&is_data);
                            if (is_data) {
                                break;
                            }
                        }*/
                        value = q->get_blocking();
                        assert(value == 53211);
                    }
                }
            });
        }

        for (std::thread& th : threads)
            th.join();
        threads.clear();

        auto endTime = std::chrono::high_resolution_clock::now();
        auto elapsedTime =
            std::chrono::duration_cast<std::chrono::milliseconds>(endTime -
                                                                  beginTime);
        std::cout << "elapsed time is " << elapsedTime.count()
                  << " milliseconds" << std::endl;
    }
}

void test_fixed_channel_wrong(int count, int num, int num2, int scores) {
    std::cout << "test fixed channel wrong:" << std::endl;
    FixedChannelWrong<int> qq;
    std::vector<std::thread> threads;

    {
        auto beginTime = std::chrono::high_resolution_clock::now();
        auto q = &qq;

        for (int i = 0; i < num; ++i) {
            threads.emplace_back([count, num, q, scores]() -> void {
                for (int j = 0; j < (count / num); ++j) {
                    for (int k = scores - 1; k >= 0; --k) {
                        // if (!q->put_non_blocking(53211)) {
                        q->put_blocking(53211);
                        //}
                    }
                }
            });
        }

        num = num2;
        for (int i = 0; i < num; ++i) {
            threads.emplace_back([count, num, q, scores]() -> void {
                for (int j = 0; j < (count / num); ++j) {
                    for (int k = scores - 1; k >= 0; --k) {
                        int value;
                        /*for (;;) {
                            bool is_data;
                            value = q->get_non_blocking(&is_data);
                            if (is_data) {
                                break;
                            }
                        }*/
                        value = q->get_blocking();
                        assert(value == 53211);
                    }
                }
            });
        }

        for (std::thread& th : threads)
            th.join();
        threads.clear();

        auto endTime = std::chrono::high_resolution_clock::now();
        auto elapsedTime =
            std::chrono::duration_cast<std::chrono::milliseconds>(endTime -
                                                                  beginTime);
        std::cout << "elapsed time is " << elapsedTime.count()
                  << " milliseconds" << std::endl;
    }
}

void test_channel_better(int count, int num, int num2, int scores) {
    std::cout << "test channel better:" << std::endl;
    ChannelBetter<int> qq;
    std::vector<std::thread> threads;

    {
        auto beginTime = std::chrono::high_resolution_clock::now();
        auto q = &qq;

        for (int i = 0; i < num; ++i) {
            threads.emplace_back([count, num, q, scores]() -> void {
                for (int j = 0; j < (count / num); ++j) {
                    for (int k = scores - 1; k >= 0; --k) {
                        q->put(53211);
                    }
                }
            });
        }

        num = num2;
        for (int i = 0; i < num; ++i) {
            threads.emplace_back([count, num, q, scores]() -> void {
                for (int j = 0; j < (count / num); ++j) {
                    for (int k = scores - 1; k >= 0; --k) {
                        int value = q->blocking_get();
                        assert(value == 53211);
                    }
                }
            });
        }

        for (std::thread& th : threads)
            th.join();
        threads.clear();

        auto endTime = std::chrono::high_resolution_clock::now();
        auto elapsedTime =
            std::chrono::duration_cast<std::chrono::milliseconds>(endTime -
                                                                  beginTime);
        std::cout << "elapsed time is " << elapsedTime.count()
                  << " milliseconds" << std::endl;
    }
}

void test_channel(int count, int num, int num2, int scores) {
    std::cout << "test channel:" << std::endl;
    Channel<int> qq;
    std::vector<std::thread> threads;

    {
        auto beginTime = std::chrono::high_resolution_clock::now();
        auto q = &qq;

        for (int i = 0; i < num; ++i) {
            threads.emplace_back([count, num, q, scores]() -> void {
                for (int j = 0; j < (count / num); ++j) {
                    for (int k = scores - 1; k >= 0; --k) {
                        q->put(53211);
                    }
                }
            });
        }

        num = num2;
        for (int i = 0; i < num; ++i) {
            threads.emplace_back([count, num, q, scores]() -> void {
                for (int j = 0; j < (count / num); ++j) {
                    for (int k = scores - 1; k >= 0; --k) {
                        int value = q->blocking_get();
                        assert(value == 53211);
                    }
                }
            });
        }

        for (std::thread& th : threads)
            th.join();
        threads.clear();

        auto endTime = std::chrono::high_resolution_clock::now();
        auto elapsedTime =
            std::chrono::duration_cast<std::chrono::milliseconds>(endTime -
                                                                  beginTime);
        std::cout << "elapsed time is " << elapsedTime.count()
                  << " milliseconds" << std::endl;
    }
}

int main(int argc, char* argv[]) {
    int times = atoi(argv[1]);
    n_const = atoi(argv[2]);
    nthreads_const = atoi(argv[3]);
    int nthreads_const2;
    if (argc >= 5) {
        nthreads_const2 = atoi(argv[4]);
    }

    int scores;
    if (argc >= 6) {
        scores = atoi(argv[5]);
    } else {
        std::cout << "argc is less than 6: " << std::endl;
        exit(-1);
    }

    for (int i = 0; i < times; ++i) {
        std::thread thread([i, nthreads_const2, scores]() -> void {
            std::cout << "" << i << " times: " << std::endl;
            test_channel_better(n_const, nthreads_const, nthreads_const2,
                                scores);
            test_channel(n_const, nthreads_const, nthreads_const2, scores);
            test_fixed_channel(n_const, nthreads_const, nthreads_const2,
                               scores);
            // test_trivial_channel(n_const, nthreads_const, nthreads_const2,
            // scores); test_fixed_channel_wrong(n_const, nthreads_const,
            // nthreads_const2, scores);
        });
        thread.join();
        std::cout << std::endl;
    }

    return 0;
}
