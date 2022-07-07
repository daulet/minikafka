package bench

/*
#include <pthread.h>
#include <time.h>
#include <stdio.h>

static long long getProcessCpuTimeNs() {
    struct timespec t;
    if (clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &t)) {
        perror("clock_gettime");
        return 0;
    }
    return t.tv_sec * 1000000000LL + t.tv_nsec;
}
*/
import "C"
import "time"

func GeProcessCpuTime() time.Duration {
	return time.Duration(C.getProcessCpuTimeNs())
}
