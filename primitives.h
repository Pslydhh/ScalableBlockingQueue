// This codes is originally from the
// https://github.com/chaoran/fast-wait-free-queue

/** @file */
#pragma once

/**
 * An atomic fetch-and-add.
 */
#define FAA(ptr, val) __atomic_fetch_add(ptr, val, __ATOMIC_RELAXED)
/**
 * An atomic fetch-and-add that also ensures sequential consistency.
 */
#define FAAcs(ptr, val) __atomic_fetch_add(ptr, val, __ATOMIC_SEQ_CST)

/**
 * An atomic compare-and-swap.
 */
#define CAS(ptr, cmp, val)                                          \
    __atomic_compare_exchange_n(ptr, cmp, val, 0, __ATOMIC_RELAXED, \
                                __ATOMIC_RELAXED)
/**
 * An atomic compare-and-swap that also ensures sequential consistency.
 */
#define CAScs(ptr, cmp, val)                                        \
    __atomic_compare_exchange_n(ptr, cmp, val, 0, __ATOMIC_SEQ_CST, \
                                __ATOMIC_SEQ_CST)
/**
 * An atomic compare-and-swap that ensures release semantic when succeed
 * or acquire semantic when failed.
 */
#define CASra(ptr, cmp, val)                                        \
    __atomic_compare_exchange_n(ptr, cmp, val, 0, __ATOMIC_RELEASE, \
                                __ATOMIC_ACQUIRE)
/**
 * An atomic compare-and-swap that ensures acquire semantic when succeed
 * or relaxed semantic when failed.
 */
#define CASa(ptr, cmp, val)                                         \
    __atomic_compare_exchange_n(ptr, cmp, val, 0, __ATOMIC_ACQUIRE, \
                                __ATOMIC_RELAXED)

#define XCHG(mem, newvalue) __atomic_exchange_n(mem, newvalue, __ATOMIC_ACQ_REL)
#define XCHGcs(mem, newvalue) \
    __atomic_exchange_n(mem, newvalue, __ATOMIC_SEQ_CST)

/**
 * An atomic swap.
 */
#define SWAP(ptr, val) __atomic_exchange_n(ptr, val, __ATOMIC_RELAXED)

/**
 * An atomic swap that ensures acquire release semantics.
 */
#define SWAPra(ptr, val) __atomic_exchange_n(ptr, val, __ATOMIC_ACQ_REL)

/**
 * A memory fence to ensure sequential consistency.
 */
#define FENCE() __atomic_thread_fence(__ATOMIC_SEQ_CST)

/**
 * An atomic store.
 */
#define STORE(ptr, val) __atomic_store_n(ptr, val, __ATOMIC_RELAXED)

/**
 * An atomic store.
 */
#define STOREr(ptr, val) __atomic_store_n(ptr, val, __ATOMIC_RELEASE)

/**
 * An atomic store.
 */
#define STOREcs(ptr, val) __atomic_store_n(ptr, val, __ATOMIC_SEQ_CST)

/**
 * An atomic load.
 */
#define LOAD(ptr) __atomic_load_n(ptr, __ATOMIC_RELAXED)

/**
 * An atomic load(ACQUIRE).
 */
#define LOADa(ptr) __atomic_load_n(ptr, __ATOMIC_ACQUIRE)

/**
 * An atomic load(ACQUIRE).
 */
#define LOADcs(ptr) __atomic_load_n(ptr, __ATOMIC_SEQ_CST)

/**
 * A store with a preceding release fence to ensure all previous load
 * and stores completes before the current store is visiable.
 */
#define RELEASE(ptr, val) __atomic_store_n(ptr, val, __ATOMIC_RELEASE)

/**
 * A load with a following acquire fence to ensure no following load and
 * stores can start before the current load completes.
 */
#define ACQUIRE(ptr) __atomic_load_n(ptr, __ATOMIC_ACQUIRE)

#define PAUSE() __asm__("pause")

static inline int _CAS2(volatile long* ptr,
                        long* cmp1,
                        long* cmp2,
                        long val1,
                        long val2) {
    char success;
    long tmp1 = *cmp1;
    long tmp2 = *cmp2;

    __asm__ __volatile__(
        "lock cmpxchg16b %1\n"
        "setz %0"
        : "=q"(success), "+m"(*ptr), "+a"(tmp1), "+d"(tmp2)
        : "b"(val1), "c"(val2)
        : "cc");

    *cmp1 = tmp1;
    *cmp2 = tmp2;
    return success;
}
#define CAS2(p, o1, o2, n1, n2) \
    _CAS2((volatile long*)p, (long*)o1, (long*)o2, (long)n1, (long)n2)

#define BTAS(ptr, bit)                                    \
    ({                                                    \
        char __ret;                                       \
        __asm__ __volatile__("lock btsq %2, %0; setnc %1" \
                             : "+m"(*ptr), "=r"(__ret)    \
                             : "ri"(bit)                  \
                             : "cc");                     \
        __ret;                                            \
    })
