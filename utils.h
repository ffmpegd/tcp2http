#ifndef __UTILS_H__
#define __UTILS_H__

#ifdef __cplusplus
extern "C" {
#endif


int serve_socket(int port);

int set_socket_nonblocking(int fd);

//0->read ok, 1->uncompleted, -1->error
int read_line(int fd, char *buf, int buf_size);

//returned buf should be freed by av_free()
int make_flv_header(AVFormatContext *ic, uint8_t **buf, int *size);


#ifdef __cplusplus
}
#endif

#endif // __UTILS_H__

