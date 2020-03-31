#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include "mapreduce.h"
#include <string.h>
#include <semaphore.h>

// TODO:
// Find how to free last bit of memory
// Allow less than map threads of files


typedef struct MapperArgs {
    char **argv;
    int argc;
} MapperArgs;

typedef struct ReducerArgs{
    int partitionNum;

} ReducerArgs;

typedef struct Intermediate{
    struct Intermediate *next;
    char *key;
    char *value;
} Intermediate;

pthread_mutex_t reduceLock;
__thread Intermediate **head = NULL;
__thread int size_head = 256;
__thread char** combine_key;
__thread int size_combine = 256;
__thread int count_combine = 0;
__thread Intermediate *free_me = NULL;
Intermediate ***final = NULL;
char **files;
int num_files;
int *size_final;
Mapper mapFunc;
Reducer reduceFunc;
Combiner combineFunc;
Partitioner partitionFunc;
int numPartitions;

char *get_next_reduce(char* key, int partition_number){
    unsigned long hash = MR_DefaultHashPartition(key, size_final[partition_number]);
    Intermediate *iter = final[partition_number][hash];
    if(iter == NULL){
        return NULL;
    }
    Intermediate *prev = NULL;
    while(iter != NULL) {
        if(strcmp(iter->key, key) == 0){
            if(prev != NULL){
                prev->next = iter->next;
            } else {
                final[partition_number][hash] = iter->next;
            }
            iter->next = free_me;
            free_me = iter;
            return iter->value;
        }
        prev = iter;
        iter = iter->next;
    }
    return NULL;
}

char *get_next(char* key){
    unsigned long hash = MR_DefaultHashPartition(key, size_head);
    Intermediate *iter = head[hash];
    if(iter == NULL){
        return NULL;
    }
    Intermediate *prev = NULL;
    while(iter != NULL) {
        if(strcmp(iter->key, key) == 0){
            if(prev != NULL){
                prev->next = iter->next;
            } else {
                head[hash] = iter->next;
            }
            iter->next = free_me;
            free_me = iter;
            return iter->value;
        }
        prev = iter;
        iter = iter->next;
    }
    return NULL;
}

void MR_EmitToCombiner(char *key, char *value){
    char *new_key = malloc(strlen(key) + 1);
    strcpy(new_key, key);
    unsigned long hash = MR_DefaultHashPartition(new_key, size_head);
    Intermediate *new = malloc(sizeof(Intermediate));
    new->value = malloc(strlen(value) + 1);
    new->key = malloc(strlen(new_key) + 1);
    strcpy(new->value, value);
    strcpy(new->key, new_key);
    count_combine++;
    if(count_combine == size_combine){
        size_combine *= 2;
        combine_key = realloc(combine_key, sizeof(char*) * size_combine);
    }
    combine_key[count_combine - 1] = new->key;
    new->next = head[hash];
    head[hash] = new;
}

void MR_EmitToReducer(char *key, char *value){
    unsigned long partition = partitionFunc(key, numPartitions);
    Intermediate *new = malloc(sizeof(Intermediate));
    new->key = key; 
    new->value = malloc(strlen(value) + 1);
    strcpy(new->value, value);
    pthread_mutex_lock(&reduceLock);
    unsigned long hash = MR_DefaultHashPartition(key, size_final[partition]);
    new->next = final[partition][hash];
    final[partition][hash] = new;
    pthread_mutex_unlock(&reduceLock);
}

unsigned long MR_DefaultHashPartition(char *key, int num_partitions) {
    unsigned long hash = 5381;
    int c;
    while ((c = *key++) != '\0')
        hash = hash * 33 + c;
    return hash % num_partitions;
}

void combineAll(){
    for(int i = 0; i < count_combine; i++){
        combineFunc(combine_key[i], get_next);
        free(combine_key[i]);
        Intermediate *iter = free_me;
        Intermediate *prev = NULL;
        while(iter != NULL){
            if(prev == NULL){
                // do nothing
            }else{
                free_me = iter;
                free(prev->value);
                free(prev);
                prev = NULL;
            }
            prev = iter;
            iter = iter->next;
        }
        if(prev != NULL){
            free(prev->value);
            free(prev);
        }
    }
    count_combine = 0;
    free(combine_key);
}

void *mapper_wrapper(void *mapper_args){
    head = malloc(sizeof(Intermediate*) * size_head);
    MapperArgs *args = (MapperArgs*)mapper_args;
    for(int i = 0; i < args->argc; i++) {
        combine_key = malloc(sizeof(char*) * size_combine);
        mapFunc(args->argv[i]);
        combineAll();
        free(args->argv[i]);
    }
    free(head);
    free(args->argv);
    free(args);
    return NULL;
}

void *reduce_wrapper(void *reduce_args){
    ReducerArgs *args = (ReducerArgs*)reduce_args;
    for(int i = 0; i < size_final[args->partitionNum]; i++){
        while(final[args->partitionNum][i] != NULL){
            reduceFunc(final[args->partitionNum][i]->key, NULL, get_next_reduce, args->partitionNum);
            Intermediate *iter = free_me;
            Intermediate *prev = NULL;
            while(iter != NULL){
                if(prev == NULL){
                    // do nothing
                }else{
                    free_me = iter;
                    free(prev->value);
                    free(prev);
                    prev = NULL;
                }
                prev = iter;
                iter = iter->next;
            }
            if(prev != NULL){
                free(prev->value);
                free(prev);
            }
        }
    }
    
    free(args);
    return NULL;
}

void MR_Run(int argc, char *argv[],
        Mapper map, int num_mappers,
        Reducer reduce, int num_reducers,
        Combiner combine,
        Partitioner partition){
            mapFunc = map;
            reduceFunc = reduce;
            combineFunc = combine;
            partitionFunc = partition;
            //Step 0: create data structure that will hold intermediate data
            //Step 1: Launch some threads to run map function
            //  -use pthread_create make sure to use gcc mapreduce.c mapreduce.h wordcount.c -pthread
            numPartitions = num_reducers;
            final = calloc(sizeof(Intermediate**), num_reducers);
            size_final = malloc(sizeof(int) * num_reducers);
            for(int i = 0; i < num_reducers; i++){
                size_final[i] = 256;
                final[i] = calloc(sizeof(Intermediate*), size_final[i]);
            }
            if(pthread_mutex_init(&reduceLock, NULL) != 0){
                printf("mutex init has failed\n");
                exit(1);
            }

            pthread_t map_threads[num_mappers];
            MapperArgs *mapper_args;
            int files = (argc - 1) / num_mappers;
            if(files == 0){
                files = 1;
            }
            int last = 1;
            for (int i = 0; i < num_mappers && last < argc - 1; i++){
                mapper_args = malloc(sizeof(MapperArgs));
                mapper_args->argv = malloc(sizeof(char*) * files);
                mapper_args->argc = files;
                for(int x = 0; x < files; x++) {
                    mapper_args->argv[x] = malloc(strlen(argv[x + last]) + 1);
                    strcpy(mapper_args->argv[x], argv[x + last]);
                }
                last += files;
                pthread_create(&map_threads[i], NULL, mapper_wrapper, mapper_args);
            }
            for (int i = 0; i < num_mappers; i++){
                pthread_join(map_threads[i], NULL);
            }
            // Barrier
            
            pthread_t reduce_threads[num_reducers];
            ReducerArgs *reduce_args;
            for (int i = 0; i < num_reducers; i++){
                reduce_args = malloc(sizeof(ReducerArgs));
                reduce_args->partitionNum = i;
                pthread_create(&reduce_threads[i], NULL, reduce_wrapper, reduce_args);
            }
            for (int i = 0; i < num_reducers; i++){
                pthread_join(reduce_threads[i], NULL);
                free(final[i]);
            }
            free(size_final);
            free(final);
        }