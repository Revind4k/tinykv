/* Readiness polling over kqueue (macOS/BSD) or epoll (Linux). Level-triggered. */
#ifndef TINYKV_EVENT_H
#define TINYKV_EVENT_H

#define EV_READ 1
#define EV_WRITE 2

typedef struct {
    int fd;
    int mask;
} ev_fired;

int ev_create(void);
/* Changes fd's interest set from oldmask to mask (0 removes it). Returns 0 or -1. */
int ev_set(int ep, int fd, int oldmask, int mask);
/* Waits up to timeout_ms (-1 forever). Returns the number of fired events, or -1. */
int ev_wait(int ep, ev_fired *out, int max, int timeout_ms);
const char *ev_backend(void);

#endif
