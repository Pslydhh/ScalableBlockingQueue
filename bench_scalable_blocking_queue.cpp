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

#include "scalable_blocking_queue.h"
#include "scalable_blocking_queue_not_hold_local_node.h"
#include "scalable_bounded_blocking_queue.h"

static int n_const = 10000000;
static int nthreads_const = 8;

void test_use_mutex(int count, int num, int num2, int scores) {
  std::cout << "use mutex:" << std::endl;
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
                not_empty.wait(m, [&queue]() { return !queue.empty(); });
                int value = queue.front();
                queue.pop_front();
                assert(value == 53211);
              }
            }
          });
    }

    for (std::thread &th : threads)
      th.join();
    threads.clear();

    auto endTime = std::chrono::high_resolution_clock::now();
    auto elapsedTime = std::chrono::duration_cast<std::chrono::milliseconds>(
        endTime - beginTime);
    std::cout << "mutex elapsed time is " << elapsedTime.count()
              << " milliseconds" << std::endl;
  }
}

void test_new_bounded_blocking_queue(int count, int num, int num2, int scores) {
  std::cout << "use new blocking queue:" << std::endl;
  ScalableBoundedBlockingQueue<int, 1000000> qq;
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
/*	    for (;;) {
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

    for (std::thread &th : threads)
      th.join();
    threads.clear();

    auto endTime = std::chrono::high_resolution_clock::now();
    auto elapsedTime = std::chrono::duration_cast<std::chrono::milliseconds>(
        endTime - beginTime);
    std::cout << "new blocking queue elapsed time is " << elapsedTime.count()
              << " milliseconds" << std::endl;
  }
}

void test_new_blocking_queue_v2(int count, int num, int num2, int scores) {
  std::cout << "use new blocking queue:" << std::endl;
  ScalableBlockingQueueV2<int> qq;
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

    for (std::thread &th : threads)
      th.join();
    threads.clear();

    auto endTime = std::chrono::high_resolution_clock::now();
    auto elapsedTime = std::chrono::duration_cast<std::chrono::milliseconds>(
        endTime - beginTime);
    std::cout << "new blocking queue elapsed time is " << elapsedTime.count()
              << " milliseconds" << std::endl;
  }
}

void test_new_blocking_queue(int count, int num, int num2, int scores) {
  std::cout << "use new blocking queue:" << std::endl;
  ScalableBlockingQueue<int> qq;
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

    for (std::thread &th : threads)
      th.join();
    threads.clear();

    auto endTime = std::chrono::high_resolution_clock::now();
    auto elapsedTime = std::chrono::duration_cast<std::chrono::milliseconds>(
        endTime - beginTime);
    std::cout << "new blocking queue elapsed time is " << elapsedTime.count()
              << " milliseconds" << std::endl;
  }
}

int main(int argc, char *argv[]) {
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
      test_new_blocking_queue_v2(n_const, nthreads_const, nthreads_const2,
                                 scores);
      test_new_blocking_queue(n_const, nthreads_const, nthreads_const2, scores);
      test_new_bounded_blocking_queue(n_const, nthreads_const, nthreads_const2,
                                      scores);
      test_use_mutex(n_const, nthreads_const, nthreads_const2, scores);
    });
    thread.join();
  }

  return 0;
}
