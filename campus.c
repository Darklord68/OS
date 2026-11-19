#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <limits.h>
#include <unistd.h>
#include <signal.h>
#include <pthread.h>
#include <semaphore.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/wait.h>
#include <sys/resource.h>

static int reap(pid_t p, int *st);

#include "part1_scheduling.c"
#include "part2_logger.c"
#include "part3_ipc_banker.c"
#include "part4_process_cli.c"
