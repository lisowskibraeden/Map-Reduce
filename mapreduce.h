#ifndef __mapreduce_h__
#define __mapreduce_h__

// Different function pointer types used by MR
typedef char *(*CombinerGetter)(char *key);
typedef char *(*ReduceGetter)(char *key, int partition_number);

typedef void (*Mapper)(char *file_name);
typedef void (*Combiner)(char *key, CombinerGetter get_func);
typedef void (*Reducer)(char *key, ReduceGetter get_func, int partition_number);
typedef unsigned long (*Partitioner)(char *key, int num_partitions);

// External functions: these are what *you must implement*
void MR_EmitToCombiner(char *key, char *value);
void MR_EmitToReducer(char *key, char *value);

unsigned long MR_DefaultHashPartition(char *key, int num_partitions);

void MR_Run(int argc, char *argv[],
        Mapper map, int num_mappers,
        Reducer reduce, int num_reducers,
        Combiner combine,
        Partitioner partition);

#endif // __mapreduce_h__