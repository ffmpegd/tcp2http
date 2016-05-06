/**
 * @brief a simple integer to pointer map
 */

#ifndef __MAP_H__
#define __MAP_H__

#ifdef __cplusplus
extern "C" {
#endif


typedef struct _map map_t;

map_t* map_create(int max_key);
void map_destroy(map_t *h);

int map_add(map_t *h, int key, void *value);
int map_remove(map_t *h, int key);
void* map_get(map_t *h, int key);

int map_get_next(map_t *h, int curr_key, int *key, void **value);


#ifdef __cplusplus
}
#endif

#endif // __MAP_H__

