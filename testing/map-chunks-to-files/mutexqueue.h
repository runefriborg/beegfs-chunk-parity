#include <pthread.h>
/*
 * A queue implemented in a circular array.
 * Thread-safe functions:
 *   enqueue(...)
 *   dequeue(...)
 * 
 * dequeue blocks when the queue is empty.
 * As a shutdown feature, all threads are released from blocking, if all threads are waiting on dequeue()
 *
 * This queue is particularly well suited for converting recursive methods into a multithreading method.
 * Example:
 * compute() {
 *   while (1) {
 *     next = dequeue();
 *     if (next == NULL)
 *       break;
 *     if (..)
 *       enqueue(..);
 *     if (..)
 *       enqueue(..);
 * }
 * main() {
 *   queue_create(..)
 *   enqueue(root_problem)
 *   start compute threads
 *   join threads
 *   queue_destroy(..)
 * }
 */

struct mutexqueue {
  pthread_mutex_t _mutex;
  pthread_cond_t _cond;
  int front,rear;
  int capacity;
  int thread_count;
  int thread_waiting;
  char **array;
};


struct mutexqueue *mutexqueue_create(int thread_count, int size) {
   struct mutexqueue *q=malloc(sizeof(struct mutexqueue));
   if(!q)return NULL;
   q->thread_count=thread_count;
   q->thread_waiting=0;
   q->capacity=size;
   q->front=-1;
   q->rear=-1;
   q->array=malloc(q->capacity*sizeof(char *));
   if(!q->array)return NULL;

   /* Initialize mutex and condition variable objects */
   pthread_mutex_init(&q->_mutex, NULL);
   pthread_cond_init (&q->_cond, NULL);

   return q;
}

void mutexqueue_destroy(struct mutexqueue *q) {
  pthread_mutex_destroy(&q->_mutex);
  pthread_cond_destroy(&q->_cond);
  free(q->array);
  free(q);
}

int isemptyqueue(struct mutexqueue *q) {
   return(q->front==-1);
}

int isfullqueue(struct mutexqueue *q) {
   return((q->rear+1)%q->capacity==q->front);
}

int queuesize(struct mutexqueue *q) {
   return(q->capacity-q->rear+q->front+1)%q->capacity;
}


int enqueue(struct mutexqueue *q,char * x) {

  pthread_mutex_lock(&q->_mutex);
  if (isfullqueue(q)) {
    pthread_mutex_unlock(&q->_mutex);
    fprintf(stderr, "queue overflow\n");
    return 1;
  }
  
  q->rear=(q->rear+1)%q->capacity;
  q->array[q->rear]=x;
  if(q->front==-1) {
    q->front=q->rear;
  }
  if (q->thread_waiting > 0) {
    pthread_cond_signal(&q->_cond);
  }
  pthread_mutex_unlock(&q->_mutex);
  return 0;
}

char * dequeue(struct mutexqueue *q) {
   char * data;

   pthread_mutex_lock(&q->_mutex);
   if(isemptyqueue(q)) {
      q->thread_waiting++;
      while (1) {
	if (q->thread_waiting == q->thread_count) {
	  // all threads are waiting. Quit!
	  pthread_cond_signal(&q->_cond);
	  pthread_mutex_unlock(&q->_mutex);
	  return NULL;
	}
	pthread_cond_wait(&q->_cond, &q->_mutex);
	if (!isemptyqueue(q)) {
	  break;
	}
      }
      q->thread_waiting--;
   }
   
   data=q->array[q->front];
   if(q->front==q->rear)
     q->front=q->rear=-1;
   else
     q->front=(q->front+1)%q->capacity;

   pthread_mutex_unlock(&q->_mutex);

   return data;
}
