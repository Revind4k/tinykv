#include "event.h"

#include <errno.h>

#if defined(__linux__)
#include <sys/epoll.h>
#include <unistd.h>

const char *ev_backend(void) { return "epoll"; }

int ev_create(void) { return epoll_create1(EPOLL_CLOEXEC); }

int ev_set(int ep, int fd, int oldmask, int mask) {
    struct epoll_event e = {0};
    e.data.fd = fd;
    if (mask & EV_READ) e.events |= EPOLLIN;
    if (mask & EV_WRITE) e.events |= EPOLLOUT;
    int op = mask == 0 ? EPOLL_CTL_DEL : oldmask == 0 ? EPOLL_CTL_ADD : EPOLL_CTL_MOD;
    return epoll_ctl(ep, op, fd, &e);
}

int ev_wait(int ep, ev_fired *out, int max, int timeout_ms) {
    struct epoll_event evs[256];
    if (max > 256) max = 256;
    int n = epoll_wait(ep, evs, max, timeout_ms);
    if (n < 0) return errno == EINTR ? 0 : -1;
    for (int i = 0; i < n; i++) {
        out[i].fd = evs[i].data.fd;
        out[i].mask = 0;
        /* Errors and hangups are reported as readable so the read path sees EOF/error. */
        if (evs[i].events & (EPOLLIN | EPOLLERR | EPOLLHUP)) out[i].mask |= EV_READ;
        if (evs[i].events & EPOLLOUT) out[i].mask |= EV_WRITE;
    }
    return n;
}

#else /* kqueue */
#include <sys/event.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>

const char *ev_backend(void) { return "kqueue"; }

int ev_create(void) { return kqueue(); }

int ev_set(int ep, int fd, int oldmask, int mask) {
    struct kevent ch[2];
    int n = 0;
    /* kqueue tracks read and write as separate filters; change only what differs. */
    if ((oldmask ^ mask) & EV_READ)
        EV_SET(&ch[n++], fd, EVFILT_READ, (mask & EV_READ) ? EV_ADD : EV_DELETE, 0, 0, NULL);
    if ((oldmask ^ mask) & EV_WRITE)
        EV_SET(&ch[n++], fd, EVFILT_WRITE, (mask & EV_WRITE) ? EV_ADD : EV_DELETE, 0, 0, NULL);
    return n ? kevent(ep, ch, n, NULL, 0, NULL) : 0;
}

int ev_wait(int ep, ev_fired *out, int max, int timeout_ms) {
    struct kevent evs[256];
    if (max > 256) max = 256;
    struct timespec ts, *tp = NULL;
    if (timeout_ms >= 0) {
        ts.tv_sec = timeout_ms / 1000;
        ts.tv_nsec = (long)(timeout_ms % 1000) * 1000000L;
        tp = &ts;
    }
    int n = kevent(ep, NULL, 0, evs, max, tp);
    if (n < 0) return errno == EINTR ? 0 : -1;
    for (int i = 0; i < n; i++) {
        out[i].fd = (int)evs[i].ident;
        out[i].mask = evs[i].filter == EVFILT_READ ? EV_READ : EV_WRITE;
    }
    return n;
}
#endif
