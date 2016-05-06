#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "map.h"

struct _map
{
    int max_key;
    void **keys;
};


map_t* map_create(int max_key)
{
    map_t *h = (map_t *)malloc(sizeof(map_t));
    if (h == NULL)
    {
        printf("[map] malloc FAILED! %d\n", (int)sizeof(map_t));
        return NULL;
    }
    memset(h, 0, sizeof(*h));

    h->max_key = max_key;
    h->keys = malloc(sizeof(void *) * (max_key + 1));
    if (h->keys == NULL)
    {
        free(h);
        printf("[map] malloc FAILED! %d\n", (int)sizeof(void *) * (max_key + 1));
        return NULL;
    }
    memset(h->keys, 0, sizeof(void *) * (max_key + 1));
    return h;
}


void map_destroy(map_t *h)
{
    free(h->keys);
    free(h);
}


int map_add(map_t *h, int key, void *value)
{
    if (key < 0 || key > h->max_key)
    {
        printf("[map add] key [%d] exceed max_key [%d]\n", key, h->max_key);
        return -1;
    }
    if (h->keys[key] != NULL)
    {
        printf("[map add] key [%d] already exist\n", key);
        return -1;
    }
    h->keys[key] = value;
    return 0;
}


int map_remove(map_t *h, int key)
{
    if (key < 0 || key > h->max_key)
    {
        fprintf(stderr, "[map remove] key [%d] exceed max_key [%d]\n", key, h->max_key);
        return -1;
    }
    if (h->keys[key] == NULL)
    {
        fprintf(stderr, "[map remove] key [%d] doesn't exist\n", key);
        return -1;
    }
    h->keys[key] = NULL;
    return 0;
}


void* map_get(map_t *h, int key)
{
    if (key < 0 || key > h->max_key)
    {
        printf("[map get] key [%d] exceed max_key [%d]\n", key, h->max_key);
        return NULL;
    }
    return h->keys[key];
}


int map_get_next(map_t *h, int curr_key, int *key, void **value)
{
    int k;
    for (k = curr_key + 1; k <= h->max_key; k++)
    {
        if (h->keys[k] != NULL)
        {
            *key = k;
            *value = h->keys[k];
            return 0;
        }
    }
    return -1;
}

