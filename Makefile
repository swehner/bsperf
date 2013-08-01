
bsperf: bsperf.o beanstalk.o
	gcc -o bsperf bsperf.o beanstalk.o -lpthread

clean:
	rm *.o bsperf
