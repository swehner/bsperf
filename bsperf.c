#include <pthread.h>
#include "beanstalk.h"
#include <inttypes.h>

#define MAX_THREADS 10000
#define MAX_SOCKETS 1000
int port;
int count;
int threads;
int nqueues;
int socksperthread;

int64_t GetMicroSeconds() {
  struct timeval tim;
  gettimeofday(&tim, 0);
  return tim.tv_usec + (tim.tv_sec * 1000000);
}

void* readJobs(void *arg) {
  int64_t t = (int64_t)arg;
  int sockets[MAX_SOCKETS];
  int is;
  char name[256];
  for (is=0; is<socksperthread; is++) {
    sockets[is] = bs_connect("127.0.0.1", port);
    sprintf(name, "tube_%"PRId64"_%d", t, is);
    //printf("sock %d watch %s\n", is, name);
    bs_watch(sockets[is], name);
  }

  BSJ *job;

  int i;
  for (i=0; i < count; i++) {
    for (is=0; is<socksperthread; is++) {
      //printf("sock %d reserve\n", is);
      int res = bs_reserve(sockets[is], &job);
      if (res == BS_STATUS_OK){
        bs_delete(sockets[is], job->id);
        bs_free_job(job);
      }
    }
  }

  for (is=0; is<socksperthread; is++) {
    bs_disconnect(sockets[is]);
  }
  return NULL;
}

void *enqueue(void *arg) {
  int64_t t = (int64_t)arg;
  int i;
  char name[256];
  int sockets[MAX_SOCKETS];
  int is;
  for (is=0; is<socksperthread; is++) {
    sockets[is] = bs_connect("127.0.0.1", port);
    sprintf(name, "tube_%"PRId64"_%d", t, is);
    //printf("sock %d use %s\n", is, name);
    bs_use(sockets[is], name);
  }

  for (i=0; i < count; i++) {
    for (is=0; is<socksperthread; is++) {
      //printf("sock %d put\n", is);
      bs_put(sockets[is], 1000, 0, 100, "hola", 4);
    }
  }
  for (is=0; is<socksperthread; is++) {
    bs_disconnect(sockets[is]);
  }
}

int main(int argc, char**argv){
  port = atoi(argv[1]);
  threads = atoi(argv[2]);
  nqueues = atoi(argv[3]);
  int total = atoi(argv[4]);
  socksperthread = nqueues/threads;
  count = total/threads/socksperthread;

  printf("Using port %d, threads %d, queues %d, socksperthread %d each using %d jobs\n", port, threads, nqueues, socksperthread, count);

  int64_t start = GetMicroSeconds();
  pthread_t threade[threads];
  pthread_t threadr[threads];
  int64_t i=0;
  for (i=0; i<threads; i++){
    pthread_create(&threade[i], NULL, enqueue, (void*)(i%nqueues));
  }
  for (i=0; i<threads; i++){
    pthread_join(threade[i], NULL);
  }
  printf("Spawning readers\n");

  for (i=0; i<threads; i++){
    pthread_create(&threadr[i], NULL, readJobs, (void*)(i%nqueues));
  }
  for (i=0; i<threads; i++){
    pthread_join(threadr[i], NULL);
  }

  int64_t tt = GetMicroSeconds() - start;
  double rate = ((double)total)/tt*1000000;
  printf("Total time: %f s %f/s\n", tt/1000000.0, rate);
}

