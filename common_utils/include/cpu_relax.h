#ifndef CPU_RELAX_H
#define CPU_RELAX_H

/*
 * cpu_relax.h — one PAUSE instruction, for spin-wait loops. It tells the
 * core it is spinning: the core waits a few dozen cycles, stops piling up
 * speculative loads of the watched variable (fewer reads of a cache line
 * other threads are writing, no pipeline flush when it changes) and gives
 * its execution units to the sibling hyperthread. The thread keeps the
 * CPU; this is not a yield. A no-op on CPUs other than x86.
 */
static inline void cpu_relax(void)
{
#if defined(__x86_64__) || defined(__i386__)
    __builtin_ia32_pause();
#endif
}

#endif /* CPU_RELAX_H */
