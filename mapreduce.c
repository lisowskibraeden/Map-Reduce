#include <stdio.h>
#include <pthread.h>
#include "mapreduce.h"
#include <string.h>

typedef struct MapperArgs {
    char *argv[];
    int argc;
} MapperArgs;


Mapper mapFunc;
Reducer reduceFunc;
Combiner combineFunc;
Partitioner partitionFunc;


void MR_EmitToCombiner(char *key, char *value){

}

void MR_EmitToReducer(char *key, char *value){

}

unsigned long MR_DefaultHashPartition(char *key, int num_partitions) {
    unsigned long hash = 5381;
    int c;
    while ((c = *key++) != '\0')
        hash = hash * 33 + c;
    return hash % num_partitions;
}

void *mapper_wrapper(void *mapper_args){
    
    
}

void *reduce_wrapper(void *reduce_args){

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
            pthread_t map_threads[num_mappers];
            MapperArgs mapper_args;
            mapper_args.argc = argc / num_mappers;
            int last = 0;
            for (int i = 0; i < num_mappers; i++){
                for(int x = last; x < last + mapper_args.argc; x++) {
                    mapper_args.argv[x - last] = argv[x];
                }
                last += mapper_args.argc;
                pthread_create(&map_threads[i], NULL, mapper_wrapper, &mapper_args);
            }
            
            pthread_join();
            pthread_t reduce_threads[num_reducers];
            for (int i = 0; i < num_reducers; i++){
                pthread_create(&reduce_threads[i], NULL, reduce_wrapper, reduce_args);
            }
            pthread_join();
        }