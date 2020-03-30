#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include "mapreduce.h"
#include <string.h>
#include <semaphore.h>

//TODO:
// More error checking
// get working
// implement hashbucket using the default hash function


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

pthread_mutex_t intermediate;
pthread_mutex_t reduceLock;
__thread Intermediate *head = NULL;
__thread char** combine;
__thread size_t size_combine = 0;
Intermediate **final = NULL;
Mapper mapFunc;
Reducer reduceFunc;
Combiner combineFunc;
Partitioner partitionFunc;
int numPartitions;

char *get_next_reduce(char* key, int partition_number){
    Intermediate *iter = final[partition_number];
    if(iter == NULL){
        return NULL;
    }
    Intermediate *prev = NULL;
    while(iter != NULL) {
        if(strcmp(iter->key, key) == 0){
            if(prev != NULL){
                prev->next = iter->next;
            } else {
                final[partition_number] = iter->next;
            }
            char* value = malloc(strlen(iter->value) + 1);
            strcpy(value, iter->value);
            free(iter);
            return value;
        }
        prev = iter;
        iter = iter->next;
    }
    return NULL;
}

char *get_next(char* key){
    pthread_mutex_lock(&intermediate);
    Intermediate *iter = head;
    if(iter == NULL){
        pthread_mutex_unlock(&intermediate);
        return NULL;
    }
    Intermediate *prev = NULL;
    while(iter != NULL) {
        if(strcmp(iter->key, key) == 0){
            if(prev != NULL){
                prev->next = iter->next;
            } else {
                head = iter->next;
            }
            pthread_mutex_unlock(&intermediate);
            char* value = malloc(strlen(iter->value) + 1);
            strcpy(value, iter->value);
            free(iter);
            return value;
        }
        prev = iter;
        iter = iter->next;
    }
    pthread_mutex_unlock(&intermediate);
    return NULL;
}

void MR_EmitToCombiner(char *key, char *value){
    Intermediate *new = malloc(sizeof(Intermediate));
    new->key = key;
    new->value = value;
    size_combine++;
    combine = realloc(combine, sizeof(char*) * size_combine);
    combine[size_combine - 1] = key;
    pthread_mutex_lock(&intermediate);
    new->next = head;
    head = new;
    pthread_mutex_unlock(&intermediate);
}

void MR_EmitToReducer(char *key, char *value){
    unsigned long partition = partitionFunc(key, numPartitions);
    Intermediate *new = malloc(sizeof(Intermediate));
    new->key = key; 
    new->value = value;
    pthread_mutex_lock(&reduceLock);
    new->next = final[partition];
    final[partition] = new;
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
    for(int i = 0; i < size_combine; i++){
        combineFunc(combine[i], get_next);
    }
    free(combine);
}

void *mapper_wrapper(void *mapper_args){
    MapperArgs *args = (MapperArgs*)mapper_args;
    combine = malloc(sizeof(char*));
    for(int i = 0; i < args->argc; i++) {
        mapFunc(args->argv[i]);
        combineAll();
        free(args->argv[i]);
    }
    free(args->argv);
    free(args);
}

void *reduce_wrapper(void *reduce_args){
    ReducerArgs *args = (ReducerArgs*)reduce_args;
    while(final[args->partitionNum] != NULL){
        reduceFunc(final[args->partitionNum]->key, get_next_reduce, args->partitionNum);
    }
    free(args);
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
            final = malloc(sizeof(Intermediate*) * num_reducers);
            if(pthread_mutex_init(&intermediate, NULL) != 0 || pthread_mutex_init(&reduceLock, NULL) != 0){
                printf("mutex init has failed\n");
                exit(1);
            }
            printf("hello\n");
            pthread_t map_threads[num_mappers];
            MapperArgs *mapper_args;
            int files = (argc - 1) / num_mappers;
            if(files == 0){
                files = 1;
            }
            int last = 1;
            for (int i = 0; i < num_mappers; i++){
                mapper_args = malloc(sizeof(Intermediate));
                mapper_args->argv = malloc(sizeof(char*) * files);
                mapper_args->argc = files;
                for(int x = 0; x < files; x++) {
                    mapper_args->argv[x] = malloc(strlen(argv[x + last]));
                    strcpy(mapper_args->argv[x], argv[x + last]);
                }
                last += files;
                pthread_create(&map_threads[i], NULL, mapper_wrapper, mapper_args);
            }
            for (int i = 0; i < num_mappers; i++){
                pthread_join(map_threads[i], NULL);
            }
            // Barrier
            
            if(pthread_mutex_init(&reduceLock, NULL) != 0){
                printf("mutex init has failed\n");
                exit(1);
            }
            pthread_t reduce_threads[num_reducers];
            ReducerArgs *reduce_args;
            for (int i = 0; i < num_reducers; i++){
                reduce_args = malloc(sizeof(ReducerArgs));
                reduce_args->partitionNum = i;
                pthread_create(&reduce_threads[i], NULL, reduce_wrapper, reduce_args);
            }
            for (int i = 0; i < num_reducers; i++){
                pthread_join(reduce_threads[i], NULL);
            }
        }