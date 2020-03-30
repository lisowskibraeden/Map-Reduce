mapreduce: 
	gcc -g mapreduce.c mapreduce.h wordcount.c -pthread -o mapreduce.o
clean:
	rm mapreduce.o