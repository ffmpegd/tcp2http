#include <unistd.h>
#include <netdb.h>
#include <fcntl.h>

#include "libavformat/avformat.h"
#include "utils.h"


int serve_socket(int port)
{
    struct addrinfo hints;
    struct addrinfo *result, *rp;
    char port_str[16];
    int reuse = 1;
    int fd;
    int ret;

    memset(&hints, 0, sizeof(struct addrinfo));
    hints.ai_family = AF_UNSPEC; //return IPv4 and IPv6 choices 
    hints.ai_socktype = SOCK_STREAM; //we want a TCP socket 
    hints.ai_flags = AI_PASSIVE; //all interfaces 

    snprintf(port_str, 16, "%d", port);
    ret = getaddrinfo(NULL, port_str, &hints, &result);
    if (ret != 0)
    {
        printf("getaddrinfo: %s\n", gai_strerror(ret));
        return -1;
    }

    for (rp = result; rp != NULL; rp = rp->ai_next)
    {
        fd = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
        if (fd == -1)
            continue;
        setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

        ret = bind(fd, rp->ai_addr, rp->ai_addrlen);
        if (ret == 0)
        {
            //we managed to bind successfully! 
            break;
        }

        close(fd);
    }
    if (rp == NULL)
    {
        printf("bind error\n");
        return -1;
    }

    //setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
    freeaddrinfo(result);
    return fd;
}


int set_socket_nonblocking(int fd)
{
    int flags;
    int ret;

    flags = fcntl(fd, F_GETFL, 0);
    if (flags == -1)
    {
        printf("fnctl F_GETFL got error: %s\n", strerror(errno));
        return -1;
    }

    flags |= O_NONBLOCK;
    ret = fcntl(fd, F_SETFL, flags);
    if (ret == -1)
    {
        printf("fnctl F_SETFL got error: %s\n", strerror(errno));
        return -1;
    }

    return 0;
}


//0->read ok, 1->uncompleted, -1->error
int read_line(int fd, char *buf, int buf_size)
{
    int read_size;
    int ret;
    memset(buf, 0, buf_size);
    for (read_size = 0; read_size < buf_size - 1; read_size += 1)
    {
        ret = read(fd, buf + read_size, 1);
        if (ret == -1)
        {
            if (errno != EAGAIN)
            {
                printf("read_line read got error:%s\n", strerror(errno));
                return -1;
            }
            return 1;
        }
        else if (ret == 0)
        {
            return -1;
        }
        if (buf[read_size] == '\n')
        {
            return 0;
        }
    }
    return 1;
}


int make_flv_header(AVFormatContext *ic, uint8_t **buf, int *size)
{
    AVFormatContext *oc;
    AVIOContext *dpb;
    int ret;
    int i;

    *size = 0;
    
    if (avformat_alloc_output_context2(&oc, NULL, "flv", NULL) != 0)
    {
        printf("avformat_alloc_output_context2 flv FAILED!\n");
        return -1;
    } 

    if (avio_open_dyn_buf(&dpb) < 0)
    {
        printf("avio_open_dyn_buf FAILED!\n");
        avformat_free_context(oc);
        return -1;
    }

    oc->pb = dpb;
    oc->ctx_flags = ic->ctx_flags;
    oc->nb_streams = ic->nb_streams;
    oc->streams = ic->streams;
    oc->metadata = ic->metadata;

    for (i = 0; i < ic->nb_streams; i++)
    {
        if (ic->streams[i]->codec->codec_tag == 0)
            ic->streams[i]->codec->codec_tag = 
                av_codec_get_tag(oc->oformat->codec_tag, ic->streams[i]->codec->codec_id);
    }

    ret = oc->oformat->write_header(oc);
    printf("write_header return:%d\n", ret);

    oc->ctx_flags = 0;
    oc->nb_streams = 0;
    oc->streams = NULL;
    oc->metadata = NULL;
    avformat_free_context(oc);
    *size = avio_close_dyn_buf(dpb, buf);
    printf("flv header size:%d\n", *size);
    return 0;
}
