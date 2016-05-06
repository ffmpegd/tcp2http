#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <ctype.h>
#include <getopt.h>
#include <netdb.h>
#include <sys/epoll.h>
#include <sys/times.h>
#include <signal.h>
#include <string.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <errno.h>
#include <sys/ioctl.h>

#include "map.h"

#define MAX_FD (1024 * 1024 * 10)
#define MAX_EVENTS 1024
#define BUF_SIZE 1600

typedef struct _conn_info 
{
    int status;
    char buf[BUF_SIZE];
    int send_size;
    int skt;
} conn_info_t;


static int do_connect(conn_info_t *info, const char *ip, int port, const char *path, int efd, map_t *map)
{
    int unblock = 1;
    struct sockaddr_in sockaddr;
    in_addr_t serverip;
    struct epoll_event event;
    int ret;

    memset(info, 0, sizeof(*info));

    memset(&sockaddr, 0, sizeof(sockaddr));
    sockaddr.sin_family = AF_INET;
    sockaddr.sin_port = htons(port);
    serverip = inet_addr(ip);
    sockaddr.sin_addr = *(struct in_addr *)&serverip;

    info->skt = socket(AF_INET, SOCK_STREAM, 0);
    if (info->skt == -1)
    {
        fprintf(stderr, "socket FAILED! %s\n", strerror(errno));
        return -1;
    }

    ioctl(info->skt, FIONBIO, (int)&unblock);
    if (connect(info->skt, (struct sockaddr *)&sockaddr, sizeof(sockaddr)) != 0)
    {
        fprintf(stderr, "connect FAILED! %s\n", strerror(errno));
        return -1;
    }

    snprintf(info->buf, BUF_SIZE, "GET %s HTTP/1.1\r\n\r\n", path);

    memset(&event, 0, sizeof(event));
    event.data.fd = info->skt;
    event.events = EPOLLOUT;
    ret = epoll_ctl(efd, EPOLL_CTL_ADD, info->skt, &event);
    if (ret == -1)
    {
        fprintf(stderr, "epoll_ctl add error: %s\n", strerror(errno));
        return -1;
    }
    if (map_add(map, info->skt, info) != 0)
    {
        fprintf(stderr, "add %d to map FAILED!\n", info->skt);
        return -1;
    }
    return 0;
}


static int do_disconnect(conn_info_t *info, int efd, map_t *map)
{
    struct epoll_event event;
    memset(&event, 0, sizeof(event));
    event.data.fd = info->skt;
    epoll_ctl(efd, EPOLL_CTL_DEL, info->skt, &event);
    close(info->skt);
    map_remove(map, info->skt);
    memset(info, 0, sizeof(*info));
    return 0;
}


int main(int argc, char **argv)
{
    char ip[256];
    int port;
    char path[1024];
    int count;
    conn_info_t *conns;
    conn_info_t *info;
    struct epoll_event event;
    map_t *map;
    int ret;
    int i;

    int efd;
    struct epoll_event *events;

    snprintf(ip, 256, "%s", argv[1]);
    port = atoi(argv[2]);
    snprintf(path, 1024, "%s", argv[3]);
    fprintf(stderr, "ip:%s, port:%d, path:%s\n", ip, port, path);

    count = atoi(argv[4]);
    fprintf(stderr, "info count:%d\n", count);

    map = map_create(MAX_FD);
    if (map == NULL)
    {
        fprintf(stderr, "map_create FAILED!\n");
        return -1;
    }

    efd = epoll_create1(0);
    if (efd == -1)
    {
        fprintf(stderr, "epoll_create1 error:%s\n", strerror(errno));
        return -1;
    }

    events = (struct epoll_event *)malloc(MAX_EVENTS * sizeof(struct epoll_event));
    if (events == NULL)
    {
        fprintf(stderr, "malloc events FAILED!\n");
        return -1;
    }

    conns = (conn_info_t *)malloc(sizeof(conn_info_t) * count);
    if (conns == NULL)
    {
        fprintf(stderr, "malloc conns FAILED!\n");
        return -1;
    }

    for (i = 0; i < count; i++)
    {
        ret = do_connect(&conns[i], ip, port, path, efd, map);
        if (ret != 0)
        {
            fprintf(stderr, "connect %d FAILED!\n", i);
            exit(-1);
        }
    }

    for (; ;)
    {
        count = epoll_wait(efd, events, MAX_EVENTS, 100);

        for (i = 0; i < count; i++)
        {
            if ((events[i].events & EPOLLERR) || (events[i].events & EPOLLHUP))
            {
                info = map_get(map, events[i].data.fd);
                if (info != NULL)
                    do_disconnect(info, efd, map);
            }
            else
            {
                info = map_get(map, events[i].data.fd);
                if (info == NULL)
                {
                    fprintf(stderr, "info is NULL, should not go here!\n");
                    continue;
                }

                if (info->status == 0)
                {
                    ret = send(info->skt, info->buf + info->send_size, strlen(info->buf) - info->send_size, 0);
                    if (ret == -1)
                    {
                        if (errno != EAGAIN)
                        {
                            fprintf(stderr, "%d| send got error:%s\n", info->skt, strerror(errno));
                            do_disconnect(info, efd, map);
                        }
                    }
                    else if (ret == 0)
                    {
                        do_disconnect(info, efd, map);
                    }
                    else
                    {
                        info->send_size += ret;
                        if (info->send_size >= strlen(info->buf))
                        {
                            //fprintf(stderr, "send header ok %d\n", info->skt);
                            memset(&event, 0, sizeof(event));
                            event.data.fd = info->skt;
                            event.events = EPOLLIN;
                            ret = epoll_ctl(efd, EPOLL_CTL_MOD, info->skt, &event);
                            if (ret == -1)
                            {
                                fprintf(stderr, "%d| epoll_ctl EPOLL_CTL_MOD EPOLLOUT FAILED!\n", info->skt);
                                continue;
                            }
                            info->status = 1;
                        }
                    }
                }
                else
                {
                    ret = read(info->skt, info->buf, BUF_SIZE);
                    if (ret == -1)
                    {
                        if (errno != EAGAIN)
                        {
                            printf("read got error:%s\n", strerror(errno));
                            continue;
                        }
                    }
                }
            }
        }
    }

    return 0;
}
