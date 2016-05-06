/**
 * @brief A simple HTTP TS/FLV server. Input stream use a modified tcp protocol of ffmpeg which add stream publish url.
 *
 * TODO: 
 *      client in重新接入后，应比较流信息，如果不一致则踢掉所有client out
 */

#include <stdio.h>
#include <unistd.h>
#include <ctype.h>
#include <getopt.h>
#include <netdb.h>
#include <sys/epoll.h>
#include <sys/times.h>
#include <signal.h>

//getrlimit and setrlimit
#include <sys/time.h>
#include <sys/resource.h>

#define __USE_GNU
#include <sched.h>
#include <pthread.h>

#include "libavformat/avformat.h"
#include "map.h"
#include "utils.h"

//#define DEBUG
#define DO_LOCK

#define MAX_URL_SIZE 1024
#define MAX_TOKEN_SIZE 32
#define MAX_EVENTS 256
#define MAX_FD_ALLOWED (1024 * 1024 * 100)
#define MAX_GOP 512
#define MAX_OUT_THREAD 128

#define TRY_OPEN_STEP (1024 * 256)
#define TRY_OPEN_SKIP (1024 * 1024 * 10)

#define QUEUE_INPUT_SIZE (1024 * 1024 * 5) //能放下一个大的I帧
#define AVIO_CTX_BUFFER_SIZE 4096
#define REQUEST_BUF_SIZE 4096

#define INACTIVE_TIMEOUT 10 //in seconds

#define CLIENT_IN 0xACEDBF59
#define CLIENT_OUT 0xFB95CADE

typedef struct _client_out_t client_out_t;

typedef struct _outs_info_t 
{
    client_out_t *outs;
    client_out_t *outs2send;
    int64_t wait_index;
} outs_info_t;

typedef struct _client_in_t 
{
    //ATTENTION: 以下变量位置不能改变
    uint32_t client_type;

    struct _client_in_t *p_next;
    int in_queue;

    time_t last_active;
    int fd;

    int url_read;
    char url[1024 + 1];
    int opened;
    outs_info_t outs_info[MAX_OUT_THREAD];

    int keep_frames;
    int video_index;
    int video_pid;
    FILE *dump_fp;

    pthread_t thrd_read_frame;
    int exit_flag;

    uint8_t queue_buf_input[QUEUE_INPUT_SIZE];
    int64_t input_head_offset;
    int read_pos;
    int write_pos;
    int head;
    int read_error;

    void *gop[MAX_GOP];
    int64_t gop_head;
    int64_t gop_tail;
    int last_is_key;
    int key_count;

    int try_open_size;
    int try_open_skip;
    AVFormatContext *fmt_ctx;
    AVIOContext *avio_ctx;
    AVPacket pkt;

    int is_flv;
    int flv_skip_header; //skip flv header in stream
    AVBufferRef *flv_header;
} client_in_t;

struct _client_out_t 
{
    //ATTENTION: 以下变量位置不能改变
    uint32_t client_type;

    client_out_t *p_next;
    int in_queue;
    client_out_t *p_next2;
    int in_queue2;

    int thread_index;
    int fd;

    client_in_t *client_in;
    AVBufferRef *frame;
    int write_pos;
    int64_t frame_pos;

    int status;
    char *request_buf;

    FILE *dump_fp;
};

typedef struct _tcp2http_t tcp2http_t;

typedef struct _out_thread_t 
{
    int index;
    int exit_flag;
    int efd;
    tcp2http_t *tcp2http;
    struct epoll_event *events;
    int64_t client_count;
    pthread_t thrd_out;
    pthread_mutex_t mutex;
} out_thread_t;

typedef struct _tcp2http_t 
{
    int exit_flag;
    int efd;
    struct epoll_event *events;
    int sfd_in;
    int sfd_out;
    map_t *map;
    client_in_t *inputs_inactive;
    client_in_t *inputs;
    out_thread_t out_threads[MAX_OUT_THREAD];

    //parameters
    int input_port;
    int output_port;
    int console_mode;
    int keep_frames;
    int max_fd;
    char dump_dir[MAX_URL_SIZE];
    char token[MAX_TOKEN_SIZE + 1];
    int out_threads_count;
    int out_threads_bind[MAX_OUT_THREAD];
} tcp2http_t;

#define ADD_NODE(p_head, node, type, ptr_name, in_queue) \
{ \
    if ((node)->in_queue == 0) \
    { \
        (node)->ptr_name = (p_head); \
        (p_head) = (node); \
        (node)->in_queue = 1; \
    } \
}
#define REMOVE_NODE(p_head, node, type, ptr_name, in_queue) \
{ \
    type *pp, *qq; \
    pp = NULL; \
    qq = (p_head); \
    while (qq != NULL) \
    { \
        if (qq == (node)) \
        { \
            if (pp == NULL) \
                (p_head) = qq->ptr_name; \
            else \
                pp->ptr_name = qq->ptr_name; \
            qq->in_queue = 0; \
            break; \
        } \
        pp = qq; \
        qq = qq->ptr_name; \
    } \
}
#define add_node(p_head, node, type) ADD_NODE(p_head, node, type, p_next, in_queue)
#define remove_node(p_head, node, type) REMOVE_NODE(p_head, node, type, p_next, in_queue)
static void process_client_out(out_thread_t *out_thread, client_out_t *client, int epoll_call);

#ifdef DO_LOCK
#define LOCK(mutex) pthread_mutex_lock(mutex)
#define UNLOCK(mutex) pthread_mutex_unlock(mutex)
#else
#define LOCK(mutex)
#define UNLOCK(mutex)
#endif

static const char *short_options = "h";
 
static const struct option long_options[] =
{
    { "help", 0, NULL, 'h' },

    { "input_port", 1, NULL, 0 },
    { "output_port", 1, NULL, 0 },
    { "keep_frames", 1, NULL, 0 },
    { "max_fd", 1, NULL, 0 },
    { "token", 1, NULL, 0 },
    { "threads_count", 1, NULL, 0 },
    { "threads_bind", 1, NULL, 0 },

    //just for debug
    { "dump_dir", 1, NULL, 0 },
    { "console_mode", 0, NULL, 0 },

    { NULL, 0, NULL, 0 }
};


static void print_help(const char *name, FILE *stream)
{
    fprintf(stream, "tcp2http [A simple HTTP server for live FLV and MPEG TS streams]\n"
            "Usage\n"

            "\t-h --help:\t\tDisplay this usage information.\n\n"

            "\t--input_port:\t\tSet stream input port.\n"
            "\t--output_port:\t\tSet the HTTP server port.\n"
            "\t--keep_frames:\t\tLet server keep more video frames than a GOP, "
                    "which allow players to quick start but image may later than realtime.\n"
            "\t--max_fd:\t\tSet the max fd to be used.\n"
            "\t--token:\t\tSet the password of input stream.\n\n"

            "\t--threads_count:\tSet how many output threads to use.\n"
            "\t--threads_bind:\t\tSet cpu index of output threads to bind.\n\n"

            "\t--dump_dir:\t\tSpecify stream dump directory, just for debug.\n"
            "\t--console_mode:\t\tDebug in console, press enter key to exit program.\n"
            );
}


static int parser_opt(int argc, char* const *argv, tcp2http_t *tcp2http)
{
    int ret;
    int long_index;

    optind = 0; //ATTENTION: reset getopt_long
    while ((ret = getopt_long(argc, argv, short_options, long_options, &long_index)) != -1)
    {
        switch (ret)
        {
            case 0:
                //long only options
                if (strncmp(long_options[long_index].name, "input_port", strlen("input_port")) == 0)
                {
                    tcp2http->input_port = atoi(optarg);
                }
                else if (strncmp(long_options[long_index].name, "output_port", strlen("output_port")) == 0)
                {
                    tcp2http->output_port = atoi(optarg);
                }
                else if (strncmp(long_options[long_index].name, "keep_frames", strlen("keep_frames")) == 0)
                {
                    tcp2http->keep_frames = atoi(optarg);
                    if (tcp2http->keep_frames < 0 || tcp2http->keep_frames > MAX_GOP)
                    {
                        fprintf(stderr, "keep_frames value should be in range [0, %d]\n", MAX_GOP);
                        exit(-1);
                    }
                }
                else if (strncmp(long_options[long_index].name, "max_fd", strlen("max_fd")) == 0)
                {
                    tcp2http->max_fd = atoi(optarg);
                    if (tcp2http->max_fd < 1024 || tcp2http->max_fd > MAX_FD_ALLOWED)
                    {
                        fprintf(stderr, "max_fa value should be in range [1024, MAX_FD_ALLOWED)\n");
                        exit(-1);
                    }
                }
                else if (strncmp(long_options[long_index].name, "token", strlen("token")) == 0)
                {
                    strncpy(tcp2http->token, optarg, MAX_TOKEN_SIZE);
                    fprintf(stderr, "set server token to [%s]\n", tcp2http->token);
                }
                else if (strncmp(long_options[long_index].name, "threads_count", strlen("threads_count")) == 0)
                {
                    tcp2http->out_threads_count = atoi(optarg);
                    if (tcp2http->out_threads_count < 1 || tcp2http->out_threads_count > MAX_OUT_THREAD)
                    {
                        fprintf(stderr, "out_threads_count value should be in range [1, MAX_OUT_THREAD]\n");
                        exit(-1);
                    }
                }
                else if (strncmp(long_options[long_index].name, "threads_bind", strlen("threads_bind")) == 0)
                {
                    char tmp[MAX_URL_SIZE];
                    char *p = tmp, *q;
                    int i;
                    strncpy(tmp, optarg, MAX_URL_SIZE - 1);
                    for (i = 0; i < MAX_OUT_THREAD; i++)
                    {
                        q = strsep(&p, ",");
                        if (q == NULL)
                            break;
                        tcp2http->out_threads_bind[i] = atoi(q);
                    }
                }
                else if (strncmp(long_options[long_index].name, "dump_dir", strlen("dump_dir")) == 0)
                {
                    strncpy(tcp2http->dump_dir, optarg, MAX_URL_SIZE - 1);
                }
                else if (strncmp(long_options[long_index].name, "console_mode", strlen("console_mode")) == 0)
                {
                    tcp2http->console_mode = 1;
                }
                break;

            case 'h':
                print_help(argv[0], stdout);
                exit(0);

            case ':':
                fprintf(stderr, "parameter reqired!\n");
                print_help(argv[0], stderr);
                exit(-1);
            case '?':
                fprintf(stderr, "unknown options!\n");
                print_help(argv[0], stderr);
                exit(-1);
            case -1:
                //处理完毕 
                break;
            default:
                fprintf(stderr, "unknown options! abort\n");
                return -1;
        }
    }

    return 0;
}


#ifdef DEBUG
static unsigned int get_tick_ms()
{
    struct tms tm;
    static uint32_t timeorigin;
    static int firsttimehere = 0;
    uint32_t now = times(&tm);
    if(firsttimehere == 0)
    {
        timeorigin = now;
        firsttimehere = 1;
    }
    return (now - timeorigin) * 10;
}


static void print_list(client_in_t *head)
{
    client_in_t *ci;
    int i;
    for (ci = head, i = 0; ci != NULL; ci = ci->p_next, i++)
        fprintf(stderr, "%d:%p|\t", i, ci);
    fprintf(stderr, "\n");
}
#endif


static int parser_url(char *url, char *keys[], char *values[])
{
    char *p, *q, *r;
    int i;

    p = strchr(url, '?');
    if (p == NULL)
        return 0;
    *p = '\0';
    p += 1;

    if (strchr(p, '/') != NULL)
    {
        fprintf(stderr, "invalid params:%s\n", p);
        return -1;
    }

    for (; ;)
    {
        q = strchr(p, '=');
        if (q == NULL)
            break;
        *q = '\0';
        r = strchr(q + 1, '&');
        if (r != NULL)
            *r = '\0';
        for (i = 0; keys[i] != NULL; i++)
        {
            if (strcmp(keys[i], p) == 0)
            {
                p = q + 1;
                values[i] = p;
                break;
            }
        }
        if (r == NULL)
            break;
        p = r + 1;
    }

    return 0;
}


static void clean_url(char *url)
{
    char *p;
    for (; ;)
    {
        p = strstr(url, "//");
        if (p == NULL)
            break;
        memmove(p, p + 1, strlen(p + 1) + 1);
    }
}


static int clean_client_in(tcp2http_t *h, client_in_t *client, int inactive)
{
    struct epoll_event event;
    client_out_t *co;
    int i;

    map_remove(h->map, client->fd);

    memset(&event, 0, sizeof(event));
    event.data.fd = client->fd;
    epoll_ctl(h->efd, EPOLL_CTL_DEL, client->fd, &event);
    close(client->fd);

    client->exit_flag = 1;
    while (client->exit_flag != 2)
        usleep(10000);

    if (client->dump_fp != NULL)
        fclose(client->dump_fp);

    if (client->avio_ctx != NULL)
    {
        av_freep(&client->avio_ctx->buffer);
        av_freep(&client->avio_ctx);
    }
    if (client->fmt_ctx != NULL && client->opened > 0)
        avformat_close_input(&client->fmt_ctx);

    av_buffer_unref(&client->flv_header);

    for (i = 0; i < MAX_GOP; i++)
    {
        if (client->gop[i] != NULL)
            av_buffer_unref((AVBufferRef **)&client->gop[i]);
    }

    for (i = 0; i < h->out_threads_count; i++)
        LOCK(&h->out_threads[i].mutex);
    remove_node(h->inputs, client, client_in_t);
    for (i = 0; i < h->out_threads_count; i++)
        UNLOCK(&h->out_threads[i].mutex);
    if (inactive != 0)
    {
        fprintf(stderr, "put in client [%d:%s] to inactive\n", client->fd, client->url);
        add_node(h->inputs_inactive, client, client_in_t);
        time(&client->last_active);
        client->gop_head = 0;
        client->gop_tail = 0;
        for (i = 0; i < h->out_threads_count; i++)
        {
            LOCK(&h->out_threads[i].mutex);
            for (co = client->outs_info[i].outs; co != NULL; co = co->p_next)
            {
#ifdef DEBUG
                fprintf(stderr, "reset client_out [%d:%p]\n", co->fd, co);
#endif
                co->frame_pos = -2;
            }
            UNLOCK(&h->out_threads[i].mutex);
        }
    }
    return 0;
}


static void destroy_client_out(tcp2http_t *h, client_out_t *client)
{
    struct epoll_event event;
    int ret;

    map_remove(h->map, client->fd);
    if (client->client_in != NULL)
    {
        remove_node(client->client_in->outs_info[client->thread_index].outs, client, client_out_t);
        REMOVE_NODE(client->client_in->outs_info[client->thread_index].outs2send, client, client_out_t, p_next2, in_queue2);
    }

    memset(&event, 0, sizeof(event));
    event.data.fd = client->fd;
    ret = epoll_ctl(h->out_threads[client->thread_index].efd, EPOLL_CTL_DEL, client->fd, &event);
#ifdef DEBUG
    fprintf(stderr, "epoll_ctl delete thread:%d, %d, efd:%d, sfd:%d\n", client->thread_index, (int)pthread_self(),
            h->out_threads[client->thread_index].efd, client->fd);
#endif
    if (ret == -1)
        fprintf(stderr, "epoll_ctl delete %d error: %s\n", client->fd, strerror(errno));
    close(client->fd);

    if (client->dump_fp != NULL)
        fclose(client->dump_fp);

    av_buffer_unref(&client->frame);
    free(client);
}


static void destroy_client_in(tcp2http_t *h, client_in_t *client)
{
    client_out_t *co;
    outs_info_t *out_info;
    int i;
    for (i = 0; i < h->out_threads_count; i++)
    {
        out_info = &client->outs_info[i];
        LOCK(&h->out_threads[i].mutex);
        for (; ;)
        {
            co = out_info->outs;
            if (co == NULL)
                break;
            destroy_client_out(h, co);
        }
        UNLOCK(&h->out_threads[i].mutex);
    }
    free(client);
}


static void* output_proc(void *arg)
{
    out_thread_t *out_thread = (out_thread_t *)arg;
    tcp2http_t *tcp2http = out_thread->tcp2http;
    client_out_t *co, *cot;
    client_in_t *ci;
    int count;
    int i;

    for (; ;)
    {
        if (out_thread->exit_flag == 1)
        {
            fprintf(stderr, "output_proc %d got exit flag\n", out_thread->index);
            out_thread->exit_flag = 2;
            return NULL;
        }

        //发送端进程挂掉时，此处无法立即发现，长时间只是认为对应的fd没有数据
        count = epoll_wait(out_thread->efd, out_thread->events, MAX_EVENTS, 100);

        for (i = 0; i < count; i++)
        {
            if ((out_thread->events[i].events & EPOLLERR) || (out_thread->events[i].events & EPOLLHUP))
            {
                fprintf(stderr, "receive epoll error, %d, %x, %x, %x, %x\n", out_thread->events[i].data.fd,
                       EPOLLERR, EPOLLHUP, EPOLLIN, EPOLLOUT);
                co = map_get(tcp2http->map, out_thread->events[i].data.fd);
                if (co != NULL && co->client_type == CLIENT_OUT)
                {
                    LOCK(&out_thread->mutex);
                    destroy_client_out(tcp2http, co);
                    UNLOCK(&out_thread->mutex);
                }
                continue;
            }
            else
            {
                co = map_get(tcp2http->map, out_thread->events[i].data.fd);
                if (co == NULL)
                    fprintf(stderr, "client is NULL for fd [%d], should not go here!\n", out_thread->events[i].data.fd);
                if (co->client_type == CLIENT_OUT)
                {
                    LOCK(&out_thread->mutex);
                    process_client_out(out_thread, co, 1);
                    UNLOCK(&out_thread->mutex);
                }
                else
                    fprintf(stderr, "unknown client type [%d] for fd [%d]\n", co->client_type, out_thread->events[i].data.fd);
            }
        }

        LOCK(&out_thread->mutex);
        for (ci = tcp2http->inputs; ci != NULL; ci = ci->p_next)
        {
            outs_info_t *info = &ci->outs_info[out_thread->index];
            if (info->wait_index < ci->gop_tail)
            {
                co = info->outs2send;
                cot = NULL;
                for (; ;)
                {
                    if (co == NULL)
                        break;
                    cot = co->p_next2;
                    process_client_out(out_thread, co, 0);
                    co = cot;
                }
                info->wait_index = ci->gop_tail;
            }
        }
        UNLOCK(&out_thread->mutex);
    }

    return NULL;
}


static int output_thread_create(tcp2http_t *h, out_thread_t *out_thread, int index, int bind_to)
{
    pthread_attr_t attr;
    out_thread->index = index;
    out_thread->tcp2http = h;
    int ret;

    out_thread->efd = epoll_create1(0);
    if (out_thread->efd == -1)
    {
        fprintf(stderr, "epoll_create1 error:%s\n", strerror(errno));
        return -1;
    }

    out_thread->events = (struct epoll_event *)malloc(MAX_EVENTS * sizeof(struct epoll_event));
    if (out_thread->events == NULL)
    {
        fprintf(stderr, "malloc FAILED! %d\n", (int)(MAX_EVENTS * sizeof(struct epoll_event)));
        close(out_thread->efd);
        return -1;
    }

    ret = pthread_mutex_init(&out_thread->mutex, NULL);
    if (ret != 0)
    {
        fprintf(stderr, "pthread_mutex_init FAILED! %s\n", strerror(errno));
        free(out_thread->events);
        close(out_thread->efd);
        return -1;
    }

    pthread_attr_init(&attr);  
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);  
    if (pthread_create(&out_thread->thrd_out, &attr, output_proc, out_thread) != 0)
    {
        fprintf(stderr, "pthread_create FAILED! %s\n", strerror(errno));
        free(out_thread->events);
        close(out_thread->efd);
        pthread_mutex_destroy(&out_thread->mutex);
        return -1;
    }
    fprintf(stderr, "thread out %d: %d, out_ptr:%p, efd:%d\n", index, (int)out_thread->thrd_out, out_thread, out_thread->efd);

    if (bind_to >= 0)
    {
        cpu_set_t mask;
        CPU_ZERO(&mask);
        CPU_SET(bind_to, &mask);
        ret = pthread_setaffinity_np(out_thread->thrd_out, sizeof(mask), &mask);
        if (ret == 0)
            fprintf(stderr, "bind output thread %d to cpu %d OK!\n", (int)out_thread->thrd_out, bind_to);
        else
            fprintf(stderr, "bind output thread %d to cpu %d FAILED!\n", (int)out_thread->thrd_out, bind_to);
    }

    return 0;
}


static void tcp2http_destroy(tcp2http_t *h)
{
    client_in_t *c;

    if (h->sfd_in != 0)
        close(h->sfd_in);
    if (h->sfd_out != 0)
        close(h->sfd_out);

    for (; ;)
    {
        c = h->inputs;
        if (c == NULL)
            break;
        clean_client_in(h, c, 0);
        destroy_client_in(h, c);
    }

    while (h->inputs_inactive != NULL)
    {
        c = h->inputs_inactive;
        remove_node(h->inputs_inactive, c, client_in_t);
        destroy_client_in(h, c);
    }

    if (h->map != NULL)
        map_destroy(h->map);

    if (h->efd != 0)
        close(h->efd);
    if (h->events != NULL)
        free(h->events);
    free(h);
}


static int tcp2http_create(tcp2http_t *h)
{
    struct epoll_event event;
    int ret;
    int i;

    //除了参数size被忽略外, 此函数和epoll_create完全相同
    h->efd = epoll_create1(0);
    if (h->efd == -1)
    {
        fprintf(stderr, "epoll_create1 error:%s\n", strerror(errno));
        goto FAIL;
    }

    h->events = (struct epoll_event *)malloc(MAX_EVENTS * sizeof(event));
    if (h->events == NULL)
    {
        fprintf(stderr, "malloc FAILED! %d\n", (int)(MAX_EVENTS * sizeof(event)));
        goto FAIL;
    }

    h->map = map_create(h->max_fd);
    if (h->map == NULL)
    {
        fprintf(stderr, "map_create FAILED! max_fd:%d\n", h->max_fd);
        goto FAIL;
    }

    h->sfd_in = serve_socket(h->input_port);
    if (h->sfd_in == -1)
        goto FAIL;
    ret = set_socket_nonblocking(h->sfd_in);
    if (ret == -1)
        goto FAIL;
    ret = listen(h->sfd_in, SOMAXCONN);
    if (ret == -1)
    {
        fprintf(stderr, "listen error:%s\n", strerror(errno));
        goto FAIL;
    }

    memset(&event, 0, sizeof(event));
    event.data.fd = h->sfd_in;
    event.events = EPOLLIN | EPOLLET; //读入, 边缘触发
    ret = epoll_ctl(h->efd, EPOLL_CTL_ADD, h->sfd_in, &event);
    if (ret == -1)
    {
        fprintf(stderr, "epoll_ctl sfd_in error: %s\n", strerror(errno));
        goto FAIL;
    }

    h->sfd_out = serve_socket(h->output_port);
    if (h->sfd_out == -1)
        goto FAIL;
    ret = set_socket_nonblocking(h->sfd_out);
    if (ret == -1)
        goto FAIL;
    ret = listen(h->sfd_out, SOMAXCONN);
    if (ret == -1)
    {
        fprintf(stderr, "listen error:%s\n", strerror(errno));
        goto FAIL;
    }

    memset(&event, 0, sizeof(event));
    event.data.fd = h->sfd_out;
    event.events = EPOLLIN | EPOLLET; //读入, 边缘触发
    ret = epoll_ctl(h->efd, EPOLL_CTL_ADD, h->sfd_out, &event);
    if (ret == -1)
    {
        fprintf(stderr, "epoll_ctl sfd_out error: %s\n", strerror(errno));
        goto FAIL;
    }

    for (i = 0; i < h->out_threads_count; i++)
    {
        ret = output_thread_create(h, &h->out_threads[i], i, h->out_threads_bind[i]);
        if (ret != 0)
        {
            int sum;
            fprintf(stderr, "output_thread_create %d FAILED!\n", i);
            h->out_threads_count = i;
            for (i = 0; i < h->out_threads_count; i++)
                h->out_threads[i].exit_flag = 1;
            for (; ;)
            {
                sum = 0;
                for (i = 0; i < h->out_threads_count; i++)
                    sum += h->out_threads[i].exit_flag;
                if (sum == h->out_threads_count * 2)
                    break;
                usleep(100000);
            }
            fprintf(stderr, "all output thread exited\n");
            for (i = 0; i < h->out_threads_count; i++)
            {
                pthread_mutex_destroy(&h->out_threads[i].mutex);
                free(h->out_threads[i].events);
            }
            goto FAIL;
        }
    }

    return 0;

FAIL:
    tcp2http_destroy(h);
    return -1;
}


static void* read_frame_proc(void *arg)
{
    client_in_t *client = (client_in_t *)arg;
    AVBufferRef *buf;
    int pkt_size;
    int pos;

    for (; ;)
    {
        if (client->exit_flag == 1)
        {
            fprintf(stderr, "%d| read_frame_proc receive found exit flag\n", client->fd);
            client->exit_flag = 2;
            return NULL;
        }

        if (client->opened < 2)
        {
            usleep(10000);
            continue;
        }

        if (av_read_frame(client->fmt_ctx, &client->pkt) != 0)
        {
            fprintf(stderr, "%d| av_read_frame FAILED! should not go here!\n", client->fd);
            usleep(10000);
            continue;
        }

        if (client->video_index >= 0 && client->pkt.stream_index != client->video_index)
        {
            av_free_packet(&client->pkt);
            continue;
        }

        if (client->is_flv)
            client->pkt.pos -= 16;

        if (client->pkt.pos < 0)
        {
            //fprintf(stderr, "%d| pkt.pos is %"PRId64"\n", client->fd, client->pkt.pos);
            av_free_packet(&client->pkt);
            continue;
        }
        pkt_size = client->pkt.pos - client->input_head_offset;

        //跳过第一个GOP, 避免重复发送flv_header导致ffplay出现"Stream discovered after head already parsed" 
        //实测ffmpeg推送的码流没问题, 其他情况不确定.
        if (client->flv_skip_header <= 1)
        {
            if (client->flv_skip_header == 0)
                client->flv_skip_header += 1;
            else if (client->pkt.flags & AV_PKT_FLAG_KEY)
            {
                client->key_count += 1;
                client->last_is_key = 1;
                client->flv_skip_header += 1;
            }
            client->head += pkt_size;
            client->input_head_offset += pkt_size;
            av_free_packet(&client->pkt);
            continue;
        }

        pos = client->gop_tail % MAX_GOP;
        buf = av_buffer_alloc(pkt_size + 1);
        if (buf == NULL)
        {
            fprintf(stderr, "%d| av_buffer_alloc FAILED! size:%d\n", client->fd, pkt_size + 1);
            av_free_packet(&client->pkt);
            continue;
        }
        if (client->head <= client->write_pos)
        {
            memcpy(buf->data + 1, client->queue_buf_input + client->head, pkt_size);
            client->head += pkt_size;
            client->input_head_offset += pkt_size;
        }
        else
        {
            if (QUEUE_INPUT_SIZE - client->head >= pkt_size)
            {
                memcpy(buf->data + 1, client->queue_buf_input + client->head, pkt_size);
                client->head += pkt_size;
                if (client->head == QUEUE_INPUT_SIZE)
                    client->head = 0;
                client->input_head_offset += pkt_size;
            }
            else
            {
                int part = QUEUE_INPUT_SIZE - client->head;
                memcpy(buf->data + 1, client->queue_buf_input + client->head, part);
                memcpy(buf->data + part + 1, client->queue_buf_input, pkt_size - part);
                client->input_head_offset += pkt_size;
                client->head = pkt_size - part;
            }
        }

        //ATTENTION: gop的frame直到位置需要被空出来前才会被释放
        if (client->gop[pos] != NULL)
            av_buffer_unref((AVBufferRef **)&client->gop[pos]);
        client->gop[pos] = buf;

        if (client->video_index < 0) //audio only
        {
            if (client->gop_tail - client->gop_head > client->keep_frames)
                client->gop_head += 1;
            client->gop_tail += 1;
            av_free_packet(&client->pkt);
            continue;
        }

        buf->data[0] = 0;
        if (client->last_is_key == 1)
            buf->data[0] = 1;

        if (client->key_count > 1 && client->gop_tail - client->gop_head > client->keep_frames)
        {
            int64_t i;
#ifdef DEBUG
            fprintf(stderr, "%d| key_count:%d, head:%"PRId64", tail:%"PRId64", keep:%d\n", client->fd,
                    client->key_count, client->gop_head, client->gop_tail, client->keep_frames);
#endif
            for (i = client->gop_head + 1; i < client->gop_tail; i++)
            {
                AVBufferRef *buf = client->gop[i % MAX_GOP];
                if (buf->data[0] == 1)
                {
                    client->key_count -= 1;
                    client->gop_head = i;
#ifdef DEBUG
                    fprintf(stderr, "%d|%d| key_count:%d, set head to:%"PRId64", frames:%"PRId64"\n",
                            get_tick_ms(), client->fd, client->key_count, client->gop_head,
                            client->gop_tail - client->gop_head);
#endif
                }

                //ATTENTION keep_frames * 3 / 4为了避免频繁检查重置gop_head
                //实际保留帧数为(keep_frames * 3 / 4, keep_frames * 3 / 4 + GOP_SIZE)
                if (client->gop_tail - i <= client->keep_frames * 3 / 4)
                    break;
            }
        }

        if (client->dump_fp != NULL)
            fwrite(buf->data + 1, buf->size - 1, 1, client->dump_fp);

        if (client->pkt.flags & AV_PKT_FLAG_KEY)
        {
#ifdef DEBUG
            fprintf(stderr, "%d| got key frame, tail:%"PRId64", write to:%d, key_count:%d\n",
                    client->fd, client->gop_tail, (pos + 1) % MAX_GOP, client->key_count);
#endif
            client->key_count += 1;
            client->last_is_key = 1;
        }
        else
        {
            client->last_is_key = 0;
        }

        client->gop_tail += 1;
        av_free_packet(&client->pkt);
    }

    return NULL;
}


static int read_packet(void *opaque, uint8_t *buf, int buf_size)
{
    client_in_t *client = (client_in_t *)opaque;
    int read_size = 0;

    for (; ;)
    {
        if (client->exit_flag == 1)
            return -1;

        if (client->read_pos == client->write_pos)
        {
            if (client->opened < 2)
            {
                client->read_error = 1;
                return -1;
            }
            usleep(10000);
            continue;
        }
        else if (client->read_pos < client->write_pos)
        {
            read_size = client->write_pos - client->read_pos;
            if (read_size > buf_size)
                read_size = buf_size;
            memcpy(buf, client->queue_buf_input + client->read_pos, read_size);
            client->read_pos += read_size;
        }
        else
        {
            read_size = QUEUE_INPUT_SIZE - client->read_pos;
            if (read_size > buf_size)
                read_size = buf_size;
            memcpy(buf, client->queue_buf_input + client->read_pos, read_size);
            client->read_pos += read_size;
            if (client->read_pos == QUEUE_INPUT_SIZE)
            {
                client->read_pos = 0;
            }
        }

        break;
    }

    return read_size;
}


static void accept_client_in(tcp2http_t *h)
{
    char hbuf[NI_MAXHOST], sbuf[NI_MAXSERV];
    struct epoll_event event;
    struct sockaddr in_addr;
    socklen_t in_len;
    client_in_t *client;
    pthread_attr_t attr;
    int in_fd;
    int ret;
    int i;

    for (; ;)
    {
        in_len = sizeof(in_addr);
        in_fd = accept(h->sfd_in, &in_addr, &in_len);
        if (in_fd == -1)
        {
            if ((errno == EAGAIN) || (errno == EWOULDBLOCK))
            {
                //we have processed all incoming connections. 
                break;
            }
            else
            {
                fprintf(stderr, "client in accept error:%s\n", strerror(errno));
                break;
            }
        }

        ret = set_socket_nonblocking(in_fd);
        if (ret == -1)
        {
            fprintf(stderr, "receive set socket non-blocking FAILED! fd:%d\n", in_fd);
            close(in_fd);
            continue;
        }

        client = (client_in_t *)malloc(sizeof(client_in_t));
        if (client == NULL)
        {
            fprintf(stderr, "%d| malloc FAILED! %d\n", in_fd, (int)sizeof(client_in_t));
            close(in_fd);
            break;
        }
        memset(client, 0, sizeof(*client));
        client->client_type = CLIENT_IN;
        client->keep_frames = h->keep_frames;
        client->video_index = -1;
        client->video_pid = -1;
        client->try_open_size = TRY_OPEN_STEP;
        client->fd = in_fd;

        ret = getnameinfo(&in_addr, in_len, hbuf, sizeof(hbuf), sbuf, sizeof(sbuf),
                NI_NUMERICHOST | NI_NUMERICSERV);
        if (ret == 0)
            fprintf(stderr, "receive new in client, fd:%d, ptr:%p, host:%s, port:%s\n", in_fd, client, hbuf, sbuf);

        av_init_packet(&client->pkt);

        if (map_add(h->map, in_fd, client) != 0)
        {
            clean_client_in(h, client, 0);
            destroy_client_in(h, client);
            continue;
        }

        for (i = 0; i < h->out_threads_count; i++)
            LOCK(&h->out_threads[i].mutex);
        add_node(h->inputs, client, client_in_t);
        for (i = 0; i < h->out_threads_count; i++)
            UNLOCK(&h->out_threads[i].mutex);

        pthread_attr_init(&attr);  
        pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);  
        if (pthread_create(&client->thrd_read_frame, &attr, read_frame_proc, client) != 0)
        {
            fprintf(stderr, "%d| pthread_create FAILED!\n", client->fd);
            clean_client_in(h, client, 0);
            destroy_client_in(h, client);
            continue;
        }

        memset(&event, 0, sizeof(event));
        event.data.fd = in_fd;
        event.events = EPOLLIN | EPOLLET;
        ret = epoll_ctl(h->efd, EPOLL_CTL_ADD, in_fd, &event);
        if (ret == -1)
        {
            fprintf(stderr, "%d| receive in EPOLL_CTL_ADD error:%s\n", in_fd, strerror(errno));
            clean_client_in(h, client, 0);
            destroy_client_in(h, client);
            continue;
        }
    }
}


static void accept_client_out(tcp2http_t *h)
{
#ifdef DEBUG
    char hbuf[NI_MAXHOST], sbuf[NI_MAXSERV];
#endif
    struct epoll_event event;
    struct sockaddr in_addr;
    socklen_t in_len;
    int in_fd;
    client_out_t *client;
    int min_client_index;
    int64_t min_client_count;
    int ret;
    int i;

    for (; ;)
    {
        in_len = sizeof(in_addr);
        in_fd = accept(h->sfd_out, &in_addr, &in_len);
        if (in_fd == -1)
        {
            if ((errno == EAGAIN) || (errno == EWOULDBLOCK))
            {
                //we have processed all incoming connections. 
                break;
            }
            else
            {
                fprintf(stderr, "client out accept error:%s\n", strerror(errno));
                break;
            }
        }

        ret = set_socket_nonblocking(in_fd);
        if (ret == -1)
        {
            fprintf(stderr, "%d| receive set socket non-blocking FAILED!\n", in_fd);
            close(in_fd);
            continue;
        }

        client = (client_out_t *)malloc(sizeof(client_out_t));
        if (client == NULL)
        {
            fprintf(stderr, "%d| malloc FAILED! %d\n", in_fd, (int)sizeof(client_out_t));
            close(in_fd);
            break;
        }
        memset(client, 0, sizeof(*client));
        client->request_buf = (char *)malloc(REQUEST_BUF_SIZE);
        if (client->request_buf == NULL)
        {
            fprintf(stderr, "%d| malloc FAILED! %d\n", in_fd, REQUEST_BUF_SIZE);
            close(in_fd);
            free(client);
            break;
        }
        memset(client->request_buf, 0, REQUEST_BUF_SIZE);
        client->client_type = CLIENT_OUT;
        client->frame_pos = -1;
        client->fd = in_fd;

#ifdef DEBUG
        ret = getnameinfo(&in_addr, in_len, hbuf, sizeof(hbuf), sbuf, sizeof(sbuf),
                NI_NUMERICHOST | NI_NUMERICSERV);
        if (ret == 0)
            fprintf(stderr, "receive new out client, fd:%d, ptr:%p, host:%s, port:%s\n", in_fd, client, hbuf, sbuf);
#endif

        if (map_add(h->map, in_fd, client) != 0)
        {
            close(in_fd);
            free(client->request_buf);
            free(client);
            continue;
        }

        min_client_index = -1;
        min_client_count = -1;
        for (i = 0; i < h->out_threads_count; i++)
        {
            out_thread_t *out_thread = &h->out_threads[i];
            if (out_thread->efd > 0 && (out_thread->client_count < min_client_count || min_client_index < 0))
            {
                min_client_index = i;
                min_client_count = out_thread->client_count;
            }
        }
        if (min_client_index >= 0)
        {
            memset(&event, 0, sizeof(event));
            event.data.fd = in_fd;
            event.events = EPOLLIN | EPOLLET; //读入, 边缘触发
            ret = epoll_ctl(h->out_threads[min_client_index].efd, EPOLL_CTL_ADD, in_fd, &event);
            if (ret == -1)
            {
                fprintf(stderr, "%d| receive out EPOLL_CTL_ADD error:%s\n", in_fd, strerror(errno));
                close(in_fd);
                free(client->request_buf);
                free(client);
                continue;
            }
            client->thread_index = min_client_index;
            h->out_threads[min_client_index].client_count += 1;
#ifdef DEBUG
            fprintf(stderr, "add %d to thread %d, client_count: %"PRId64", efd:%d\n", 
                    in_fd, min_client_index, h->out_threads[min_client_index].client_count, h->out_threads[min_client_index].efd);
#endif
        }
        else
        {
            fprintf(stderr, "no output thread found, should not go here!\n");
            close(in_fd);
            free(client->request_buf);
            map_remove(h->map, in_fd);
            free(client);
        }
    }
}


static int process_client_in_url(tcp2http_t *h, client_in_t *client)
{
    client_in_t *ci;
    client_out_t *co;
    outs_info_t *out_info;
    char *keys[] = { "dump_file", "keep_frames", "video_index", "video_pid", "token", NULL };
    char *values[] = { 0, 0, 0, 0, 0, 0 };
    char dump_path[MAX_URL_SIZE] = { 0 };
    int i;

    fprintf(stderr, "%d| client_in url:%s\n", client->fd, client->url);

    parser_url(client->url, keys, values);
    clean_url(client->url);
    if (client->url[0] != '/')
    {
        memmove(client->url + 1, client->url, strlen(client->url) + 1);
        client->url[0] = '/';
    }
    fprintf(stderr, "%d| stream path:%s\n", client->fd, client->url);

    if (values[0] != NULL) //dump_file
    {
        if (strlen(h->dump_dir) <= 0)
        {
            fprintf(stderr, "%d| client_in request dump, but server not set dump_path\n", client->fd);
        }
        else
        {
            snprintf(dump_path, MAX_URL_SIZE, "%s/%s", h->dump_dir, values[0]);
            fprintf(stderr, "%d| dump client_in to file:%s\n", client->fd, dump_path);
        }
        client->dump_fp = fopen(dump_path, "wb");
        if (client->dump_fp == NULL)
        {
            fprintf(stderr, "%d| open dump_file %s FAILED! %s\n", client->fd, dump_path, strerror(errno));
        }
    }
    if (values[1] != NULL) //keep_frames
    {
        int keep_frames = atoi(values[1]);
        if (keep_frames < 0 || keep_frames > MAX_GOP)
        {
            fprintf(stderr, "%d| invalid keep_frames value [%d] set, should be in range [0, %d]",
                    client->fd, keep_frames, MAX_GOP);
        }
        else
        {
            client->keep_frames = keep_frames;
            fprintf(stderr, "%d| set keep_frames to:%d\n", client->fd, client->keep_frames);
        }
    }
    if (values[2] != NULL) //video_index
    {
        client->video_index = atoi(values[2]);
        fprintf(stderr, "%d| video_index specified is:%d\n", client->fd, client->video_index);
    }
    if (values[3] != NULL) //video_pid
    {
        client->video_pid = atoi(values[3]);
        fprintf(stderr, "%d| video_pid specified is:%d\n", client->fd, client->video_pid);
    }
    if ((strlen(h->token) > 0 && values[4] == NULL) || strcmp(h->token, values[4]) != 0)
    {
        fprintf(stderr, "%d| invalid token, server [%s], given [%s]\n", client->fd, h->token, values[4]);
        return -1;
    }

    for (ci = h->inputs; ci != NULL; ci = ci->p_next)
    {
        if (ci != client && strcmp(ci->url, client->url) == 0)
        {
            fprintf(stderr, "%d| url [%s] already streamed in by fd [%d]\n", client->fd, ci->url, ci->fd);
            return -1;
        }
    }

    for (ci = h->inputs_inactive; ci != NULL; ci = ci->p_next)
    {
        if (strcmp(ci->url, client->url) == 0)
        {
            fprintf(stderr, "found inactive in for %d, url:%s\n", client->fd, client->url);

            for (i = 0; i < h->out_threads_count; i++)
            {
                LOCK(&h->out_threads[i].mutex);
                out_info = &ci->outs_info[i];
                for (; ;)
                {
                    co = out_info->outs;
                    if (co == NULL)
                        break;
                    out_info->outs = co->p_next;
                    fprintf(stderr, "modify client_in to [%d:%p:%s] for client_out [%d:%p]\n",
                            client->fd, client, client->url, co->fd, co);
                    co->client_in = client;
                    co->frame_pos = -2;
                    co->in_queue = 0;
                    co->in_queue2 = 0;
                    add_node(client->outs_info[i].outs, co, client_out_t);
                    ADD_NODE(client->outs_info[i].outs2send, co, client_out_t, p_next2, in_queue2);
                }
                UNLOCK(&h->out_threads[i].mutex);
            }

            remove_node(h->inputs_inactive, ci, client_in_t);
            free(ci);
            break;
        }
    }

    client->url_read += 1;
    return 0;
}


static void process_client_in_open(tcp2http_t *h, client_in_t *client)
{
    uint8_t *avio_ctx_buffer;
    uint8_t *buf;
    int size;
    int ret;
    int i;

    if (client->write_pos < client->try_open_size)
        return;

    avio_ctx_buffer = av_malloc(AVIO_CTX_BUFFER_SIZE);
    if (avio_ctx_buffer == NULL)
    {
        fprintf(stderr, "%d| avio_ctx_buffer: %p\n", client->fd, avio_ctx_buffer);
        goto FAIL;
    }
    client->avio_ctx = avio_alloc_context(avio_ctx_buffer, AVIO_CTX_BUFFER_SIZE,
            0, client, &read_packet, NULL, NULL);
    if (client->avio_ctx == NULL)
    {
        fprintf(stderr, "%d| avio_alloc_context FAILED!\n", client->fd);
        goto FAIL;
    }

    client->fmt_ctx = avformat_alloc_context();
    if (client->fmt_ctx == NULL)
    {
        fprintf(stderr, "%d| fmt_ctx: %p\n", client->fd, client->fmt_ctx);
        goto FAIL;
    }
    client->fmt_ctx->pb = client->avio_ctx;

    client->read_pos = 0;
    client->read_error = 0;
    ret = avformat_open_input(&client->fmt_ctx, NULL, NULL, NULL);
    if (ret < 0)
    {
        fprintf(stderr, "%d| avformat_open_input FAILED! ret:%d\n", client->fd, ret);
        goto REOPEN;
    }
    client->opened = 1;

    ret = avformat_find_stream_info(client->fmt_ctx, NULL);
    if (ret < 0)
    {
        fprintf(stderr, "%d| avformat_find_stream_info FAILED! ret:%d\n", client->fd, ret);
        goto REOPEN;
    }
    if (client->read_error > 0)
        goto REOPEN;

    if (client->video_pid >= 0)
    {
        for (i = 0; i < client->fmt_ctx->nb_streams; i++)
        {
            if (client->fmt_ctx->streams[i]->id == client->video_pid)
            {
                if (client->fmt_ctx->streams[i]->codec->codec_type != AVMEDIA_TYPE_VIDEO)
                {
                    fprintf(stderr, "%d| specified video_pid %d is not video\n", client->fd, client->video_pid);
                    goto FAIL;
                }
                client->video_index = i;
                fprintf(stderr, "%d| set video_index to %d for pid %d\n", client->fd, i, client->video_pid);
                break;
            }
        }
        if (i == client->fmt_ctx->nb_streams)
        {
            fprintf(stderr, "%d| specified video_pid %d not found!\n", client->fd, client->video_pid);
            goto FAIL;
        }
    }
    else if (client->video_index >= 0 && client->video_index < client->fmt_ctx->nb_streams)
    {
        if (client->fmt_ctx->streams[client->video_index]->codec->codec_type == AVMEDIA_TYPE_VIDEO)
        {
            fprintf(stderr, "%d| use specified video_index %d\n", client->fd, client->video_index);
        }
        else
        {
            fprintf(stderr, "%d| spedified video_index %d is not video!\n", client->fd, client->video_index);
            goto FAIL;
        }
    }
    else
    {
        for (i = 0; i < client->fmt_ctx->nb_streams; i++)
        {
            if (client->fmt_ctx->streams[i]->codec->codec_type == AVMEDIA_TYPE_VIDEO)
            {
                fprintf(stderr, "%d| no video_index specified, auto set to %d\n", client->fd, i);
                client->video_index = i;
                break;
            }
        }
        if (i == client->fmt_ctx->nb_streams)
        {
            fprintf(stderr, "%d| no video_index or video_pid specified, and no video found in streams\n", client->fd);
            client->video_index = -1;
        }
    }

    if (strcmp(client->fmt_ctx->iformat->name, "flv") == 0)
    {
        make_flv_header(client->fmt_ctx, &buf, &size);
        client->flv_header = av_buffer_alloc(size + 1);
        client->flv_header->data[0] = 0;
        memcpy(client->flv_header->data + 1, buf, size);
        av_freep(&buf);
        client->is_flv = 1;
        fprintf(stderr, "%d| flv stream\n", client->fd);
    }

    av_dump_format(client->fmt_ctx, 0, "", 0);
    client->opened = 2;
    fprintf(stderr, "%d| open input stream ok, write_pos:%d\n", client->fd, client->write_pos);
    return;

FAIL:
    clean_client_in(h, client, 0);
    destroy_client_in(h, client);
    return;

REOPEN:
    if (client->avio_ctx != NULL)
    {
        av_freep(&client->avio_ctx->buffer);
        av_freep(&client->avio_ctx);
    }
    if (client->fmt_ctx != NULL && client->opened > 0)
        avformat_close_input(&client->fmt_ctx);
    if (client->try_open_skip > TRY_OPEN_SKIP)
    {
        fprintf(stderr, "%d| open input stream FAILED!\n", client->fd);
        goto FAIL;
    }
    client->try_open_size += TRY_OPEN_STEP;
    client->opened = 0;
    if (client->try_open_size > QUEUE_INPUT_SIZE - TRY_OPEN_STEP)
        client->try_open_size = QUEUE_INPUT_SIZE - TRY_OPEN_STEP;
    if (client->write_pos >= QUEUE_INPUT_SIZE - TRY_OPEN_STEP)
    {
        memmove(client->queue_buf_input, client->queue_buf_input + TRY_OPEN_STEP,
                client->write_pos - TRY_OPEN_STEP);
        client->write_pos -= TRY_OPEN_STEP;
        client->try_open_skip += TRY_OPEN_STEP;
    }
}


//we must read whatever data is available completely, 
//as we are running in edge-triggered mode and won't get a notification again for the same data 
static void process_client_in(tcp2http_t *h, client_in_t *client)
{
    int done = 0;
    int read_size;
    int total_read = 0;

    while (client->url_read < 1024)
    {
        read_size = read(client->fd, client->url + client->url_read, 1024 - client->url_read);
        if (read_size == -1)
        {
            if (errno != EAGAIN)
            {
                fprintf(stderr, "%d| receive read got error:%s\n", client->fd, strerror(errno));
                done = 1;
            }
            return;
        }
        else if (read_size == 0)
        {
            done = 1;
            break;
        }
        client->url_read += read_size;
    }

    if (done != 0)
    {
        if (clean_client_in(h, client, 1) != 0)
            destroy_client_in(h, client);
        return;
    }

    if (client->url_read == 1024)
    {
        if (process_client_in_url(h, client) != 0)
        {
            clean_client_in(h, client, 0);
            destroy_client_in(h, client);
            return;
        }
    }

    for (; ;)
    {
        if ((client->write_pos + 1) % QUEUE_INPUT_SIZE == client->head)
        {
            fprintf(stderr, "%d| input buffer is full\n", client->fd);
            break;
        }
        if (client->write_pos >= QUEUE_INPUT_SIZE)
            client->write_pos = 0;

        if (client->write_pos >= client->head)
            read_size = read(client->fd, client->queue_buf_input + client->write_pos, QUEUE_INPUT_SIZE - client->write_pos);
        else
            read_size = read(client->fd, client->queue_buf_input + client->write_pos, client->head - client->write_pos - 1);

        if (read_size == -1)
        {
            //errno == EAGAIN means that we have read all data
            if (errno != EAGAIN)
            {
                fprintf(stderr, "%d| receive read got error:%s\n", client->fd, strerror(errno));
                done = 1;
            }
            break;
        }
        else if (read_size == 0)
        {
            //end of file. the remote has closed the connection 
            done = 1;
            break;
        }
        total_read += read_size;
        client->write_pos += read_size;
    }

    if (done != 0)
    {
        if (clean_client_in(h, client, 1) != 0)
            destroy_client_in(h, client);
        return;
    }

    if (total_read <= 0)
        return;

    if (client->opened == 0)
        process_client_in_open(h, client);
}


static void process_client_out_get(out_thread_t *out_thread, client_out_t *client)
{
    client_in_t *client_in;
    char *p = client->request_buf + 3;
    char *q;
    char *keys[] = { "dump_file", NULL };
    char *values[2] = { 0, 0 };
    char dump_path[MAX_URL_SIZE] = { 0 };

    while (isblank(*p))
        p += 1;
    q = p;
    while (!isblank(*q) && *q != '\0')
        q += 1;
    if (isblank(*q))
        *q = '\0';
    parser_url(p, keys, values);
    clean_url(p);
#ifdef DEBUG
    fprintf(stderr, "%d| request url:%s\n", client->fd, p);
#endif

    for (client_in = out_thread->tcp2http->inputs; client_in != NULL; client_in = client_in->p_next)
    {
        if (strcmp(client_in->url, p) == 0)
        {
#ifdef DEBUG
            fprintf(stderr, "%d| found in\n", client->fd);
#endif
            client->client_in = client_in;
            add_node(client_in->outs_info[out_thread->index].outs, client, client_out_t);
            break;
        }
    }
    if (client->client_in == NULL)
    {
        fprintf(stderr, "%d| request url not found:%s\n", client->fd, p);

    }
    else if (values[0] != NULL)
    {
        if (strlen(out_thread->tcp2http->dump_dir) <= 0)
        {
            fprintf(stderr, "%d| client_out request dump, but server not set dump_path\n", client->fd);
        }
        else
        {
            snprintf(dump_path, MAX_URL_SIZE, "%s/%s", out_thread->tcp2http->dump_dir, values[0]);
            fprintf(stderr, "%d| dump client_out to file:%s\n", client->fd, dump_path);
        }
        client->dump_fp = fopen(dump_path, "wb");
        if (client->dump_fp == NULL)
        {
            fprintf(stderr, "%d| open dump_file %s FAILED! %s\n", client->fd, dump_path, strerror(errno));
        }
    }
}


static void process_client_out_request(out_thread_t *out_thread, client_out_t *client)
{
    struct epoll_event event;
    int write_size;
    int ret;

    if (client->status == 0)
    {
        for (; ;)
        {
            ret = read_line(client->fd, client->request_buf + strlen(client->request_buf),
                    REQUEST_BUF_SIZE - strlen(client->request_buf));
            if (ret != 0)
            {
                if (ret < 0)
                    destroy_client_out(out_thread->tcp2http, client);
                return;
            }
            if (strncasecmp(client->request_buf, "\r\n", 2) == 0)
            {
                client->status = 1;

                if (client->client_in == NULL)
                {
                    snprintf(client->request_buf, REQUEST_BUF_SIZE, "HTTP/1.1 404 Url Not Found\r\n\r\n");
                }
                else
                {
#ifdef DEBUG
                    fprintf(stderr, "set client_in to [%d:%s] for client_out [%d]\n", client->client_in->fd,
                            client->client_in->url, client->fd);
#endif
                    snprintf(client->request_buf, REQUEST_BUF_SIZE, "HTTP/1.1 200 OK\r\n\r\n");
                }

                memset(&event, 0, sizeof(event));
                event.data.fd = client->fd;
                event.events = EPOLLOUT;
                if (client->client_in != NULL)
                    event.events = event.events | EPOLLET;
                ret = epoll_ctl(out_thread->efd, EPOLL_CTL_MOD, client->fd, &event);
                if (ret == -1)
                {
                    fprintf(stderr, "%d| epoll_ctl EPOLL_CTL_MOD EPOLLOUT FAILED!\n", client->fd);
                    destroy_client_out(out_thread->tcp2http, client);
                }
                if (client->client_in != NULL)
                {
                    ADD_NODE(client->client_in->outs_info[out_thread->index].outs2send,
                            client, client_out_t, p_next2, in_queue2);
                }
                return;
            }

            if (strncasecmp(client->request_buf, "GET", 3) == 0)
                process_client_out_get(out_thread, client);

            memset(client->request_buf, 0, REQUEST_BUF_SIZE);
        }

        return;
    }
    else if (client->status == 1)
    {
        write_size = send(client->fd, client->request_buf, strlen(client->request_buf), 0);
        if (write_size == -1)
        {
            //errno == EAGAIN means that we have read all data
            if (errno != EAGAIN)
            {
                fprintf(stderr, "%d| resend send got error:%s\n", client->fd, strerror(errno));
                destroy_client_out(out_thread->tcp2http, client);
                return;
            }
        }
        else if (write_size == 0)
        {
            destroy_client_out(out_thread->tcp2http, client);
            return;
        }

#ifdef DEBUG
        fprintf(stderr, "%d| client_ptr:%p, out_ptr:%p, out_index:%d, type:%x, %d, %d\n", 
                client->fd, client, out_thread, out_thread->index, client->client_type, write_size, (int)pthread_self());
        fprintf(stderr, "%d\n", (int)strlen(client->request_buf));
#endif
        if (write_size == strlen(client->request_buf))
        {
            free(client->request_buf);
            client->request_buf = NULL;
            client->status = 2;
        }
        else
        {
            memmove(client->request_buf, client->request_buf + write_size,
                    strlen(client->request_buf) - write_size + 1);
        }
    }
}


static void process_client_out(out_thread_t *out_thread, client_out_t *client, int epoll_call)
{
    int conn_least;
    int write_size;

    if (client->status < 2)
    {
        process_client_out_request(out_thread, client);
        return;
    }
    else if (client->client_in == NULL)
    {
        destroy_client_out(out_thread->tcp2http, client);
        return;
    }

    //尽可能多的发送数据，直到没有数据或受到EAGAIN
    for (; ;)
    {
        if (client->frame == NULL)
        {
            //client_in重新接入后，积攒一定数量的帧在开始发送
            conn_least = 0;
            if (client->frame_pos == -2)
            {
                conn_least = 60;
                if (conn_least > out_thread->tcp2http->keep_frames / 2)
                    conn_least = out_thread->tcp2http->keep_frames / 2;
            }

            if (client->frame_pos >= 0 && client->frame_pos < client->client_in->gop_tail 
                    && client->frame_pos > client->client_in->gop_tail - MAX_GOP)
            {
                client->frame = av_buffer_ref(client->client_in->gop[client->frame_pos % MAX_GOP]);
                client->frame_pos += 1;
            }
            else if (client->frame_pos <= client->client_in->gop_head
                    && client->client_in->gop_tail > client->client_in->gop_head + conn_least)
            {
                //after client_in reconnect, frame_pos is -2, and flv_header should not be sent
                if (client->frame_pos < 0 && client->frame_pos != -2 && client->client_in->is_flv)
                {
                    client->frame = av_buffer_ref(client->client_in->flv_header);
                    client->frame_pos = client->client_in->gop_head;
                }
                else
                {
                    client->frame = av_buffer_ref(client->client_in->gop[client->client_in->gop_head % MAX_GOP]);
                    client->frame_pos = client->client_in->gop_head + 1;
                }
            }

            if (client->frame == NULL)
            {
                if (epoll_call != 0)
                {
                    ADD_NODE(client->client_in->outs_info[out_thread->index].outs2send,
                            client, client_out_t, p_next2, in_queue2);
                }
                return;
            }
            client->write_pos = 0;
        }

        if (client->write_pos >= client->frame->size - 1)
        {
            av_buffer_unref(&client->frame);
            client->frame = NULL;
            continue;
        }

        write_size = send(client->fd, client->frame->data + 1 + client->write_pos,
                client->frame->size - 1 - client->write_pos, 0);
        if (write_size == -1)
        {
            //errno == EAGAIN means that we have read all data
            if (errno == EAGAIN)
            {
                if (epoll_call == 0)
                {
                    REMOVE_NODE(client->client_in->outs_info[out_thread->index].outs2send,
                            client, client_out_t, p_next2, in_queue2);
                }
            }
            else
            {
                fprintf(stderr, "%d| resend send got error:%s, client:%p, epoll_call:%d\n",
                        client->fd, strerror(errno), client, epoll_call);
                destroy_client_out(out_thread->tcp2http, client);
            }
            return;
        }
        else if (write_size == 0)
        {
            destroy_client_out(out_thread->tcp2http, client);
            return;
        }

        if (client->dump_fp != NULL)
            fwrite(client->frame->data + 1 + client->write_pos, write_size, 1, client->dump_fp);

        if (epoll_call != 0)
        {
            ADD_NODE(client->client_in->outs_info[out_thread->index].outs2send,
                    client, client_out_t, p_next2, in_queue2);
        }
        client->write_pos += write_size;
    }
}


static void* tcp2http_proc(void *arg)
{
    tcp2http_t *h = (tcp2http_t *)arg;
    client_in_t *ci, *cit;
    int last_clean_count = 0;
    time_t t;
    int count;
    int i;

    for (; ;)
    {
        if (h->exit_flag == 1)
        {
            int sum;
            fprintf(stderr, "tcp2http_proc got exit flag\n");
            for (i = 0; i < h->out_threads_count; i++)
                h->out_threads[i].exit_flag = 1;
            for (; ;)
            {
                sum = 0;
                for (i = 0; i < h->out_threads_count; i++)
                    sum += h->out_threads[i].exit_flag;
                if (sum == h->out_threads_count * 2)
                    break;
                usleep(100000);
            }
            fprintf(stderr, "all output thread exited\n");
            for (i = 0; i < h->out_threads_count; i++)
            {
                pthread_mutex_destroy(&h->out_threads[i].mutex);
                free(h->out_threads[i].events);
            }
            h->exit_flag = 2;
            return NULL;
        }

        if (last_clean_count > 100)
        {
            time(&t);
            last_clean_count = 0;
            ci = h->inputs_inactive;
            cit = NULL;
            for (; ;)
            {
                if (ci == NULL)
                    break;
                cit = ci->p_next;
                if (t - ci->last_active > INACTIVE_TIMEOUT)
                {
                    fprintf(stderr, "clean timeout inactive in [%p:%s]\n", ci, ci->url);
                    remove_node(h->inputs_inactive, ci, client_in_t);
                    destroy_client_in(h, ci);
                }
                ci = cit;
            }
        }
        last_clean_count += 1;

        //发送端进程挂掉时，此处无法立即发现，长时间只是认为对应的fd没有数据
        count = epoll_wait(h->efd, h->events, MAX_EVENTS, 100);

        for (i = 0; i < count; i++)
        {
            if ((h->events[i].events & EPOLLERR) || (h->events[i].events & EPOLLHUP))
            {
                fprintf(stderr, "receive epoll error, %d, %x, %x, %x, %x\n", h->events[i].data.fd,
                       EPOLLERR, EPOLLHUP, EPOLLIN, EPOLLOUT);
                ci = map_get(h->map, h->events[i].data.fd);
                if (ci != NULL && ci->client_type == CLIENT_IN)
                {
                    if (clean_client_in(h, ci, 1) != 0)
                    {
                        destroy_client_in(h, ci);
                    }
                }

                continue;
            }
            else if (h->events[i].data.fd == h->sfd_in)
            {
                accept_client_in(h);
            }
            else if (h->events[i].data.fd == h->sfd_out)
            {
                accept_client_out(h);
            }
            else
            {
                ci = map_get(h->map, h->events[i].data.fd);
                if (ci == NULL)
                    fprintf(stderr, "client is NULL for fd [%d], should not go here!\n", h->events[i].data.fd);
                if (ci->client_type == CLIENT_IN)
                    process_client_in(h, ci);
                else
                    fprintf(stderr, "unknown client type [%d] for fd [%d]\n", ci->client_type, h->events[i].data.fd);
            }
        }
    }

    return NULL;
}


int main(int argc, char **argv)
{
    tcp2http_t *tcp2http = (tcp2http_t *)malloc(sizeof(tcp2http_t));
    pthread_t thrd_tcp2http;
    pthread_attr_t attr;
    struct sched_param sparam;
    struct rlimit limit;
    int i;

    signal(SIGPIPE, SIG_IGN);

    av_register_all();

    memset(tcp2http, 0, sizeof(*tcp2http));
    tcp2http->input_port = 10000;
    tcp2http->output_port = 20000;
    tcp2http->out_threads_count = 1;
    for (i = 0; i < MAX_OUT_THREAD; i++)
        tcp2http->out_threads_bind[i] = -1;
    parser_opt(argc, argv, tcp2http);
    fprintf(stderr, "input_port:%d, output_port:%d, out_threads_count:%d\n",
            tcp2http->input_port, tcp2http->output_port, tcp2http->out_threads_count);

    if (getrlimit(RLIMIT_NOFILE, &limit) != 0)
    {
        fprintf(stderr, "getrlimit FAILED! %s\n", strerror(errno));
    }
    else if (tcp2http->max_fd < limit.rlim_cur)
    {
        tcp2http->max_fd = limit.rlim_cur;
        fprintf(stderr, "set max_fd to system rlim_cur: %d\n", (int)limit.rlim_cur);
    }
    limit.rlim_cur = tcp2http->max_fd;
    limit.rlim_max = tcp2http->max_fd;
    if (setrlimit(RLIMIT_NOFILE, &limit) != 0)
    {
        fprintf(stderr, "setrlimit (%d:%d) FAILED! %s\n", (int)limit.rlim_cur, (int)limit.rlim_max, strerror(errno));
    }
    fprintf(stderr, "max_fd is: %d\n", tcp2http->max_fd);

    if (tcp2http_create(tcp2http) != 0)
    {
        fprintf(stderr, "tcp2http_create FAILED!\n");
        return -1;
    }

    pthread_attr_init(&attr);  
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);  
    //pthread_attr_setschedpolicy(&attr, SCHED_RR);
    sparam.sched_priority = 90;
    pthread_attr_setschedparam(&attr, &sparam);
    if (pthread_create(&thrd_tcp2http, &attr, tcp2http_proc, tcp2http) != 0)
    {
        fprintf(stderr, "[%d] pthread_create FAILED!\n", __LINE__);
        pthread_attr_destroy(&attr);
        return -1;
    }
    pthread_attr_destroy(&attr);

    if (tcp2http->console_mode)
    {
        getchar();
        fprintf(stderr, "======== exit ========\n");

        tcp2http->exit_flag = 1;
        while (tcp2http->exit_flag != 2)
            usleep(10000);
        tcp2http_destroy(tcp2http);
    }
    else
    {
        for (; ;)
            sleep(10);
    }

    return 0;
}
