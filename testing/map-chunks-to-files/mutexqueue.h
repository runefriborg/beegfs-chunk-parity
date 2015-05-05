#include <pthread.h>

pthread_mutex_t _mutex;
pthread_cond_t _cond;

struct circqueue {
  int front,rear;
  int capacity;
  int thread_count;
  int thread_waiting;
  char **array;
};


struct circqueue *mutexqueue(int thread_count, int size) {
   struct circqueue *q=malloc(sizeof(struct circqueue));
   if(!q)return NULL;
   q->thread_count=thread_count;
   q->thread_waiting=0;
   q->capacity=size;
   q->front=-1;
   q->rear=-1;
   q->array=malloc(q->capacity*sizeof(char *));
   if(!q->array)return NULL;

   /* Initialize mutex and condition variable objects */
   pthread_mutex_init(&_mutex, NULL);
   pthread_cond_init (&_cond, NULL);

   return q;
}

void mutexqueue_destroy(struct circqueue *q) {
  pthread_mutex_destroy(&_mutex);
  pthread_cond_destroy(&_cond);
  free(q->array);
  free(q);
}

int isemptyqueue(struct circqueue *q) {
   return(q->front==-1);
}

int isfullqueue(struct circqueue *q) {
   return((q->rear+1)%q->capacity==q->rear);
}

int queuesize(struct circqueue *q) {
   return(q->capacity-q->rear+q->front+1)%q->capacity;
}


void enqueue(struct circqueue *q,char * x) {

  pthread_mutex_lock(&_mutex);
   if(isfullqueue(q))
      printf("queue overflow\n");
   else{
      q->rear=(q->rear+1)%q->capacity;
      q->array[q->rear]=x;
      if(q->front==-1) {
         q->front=q->rear;
      }
      if (q->thread_waiting > 0) {
	pthread_cond_signal(&_cond);
      }
   }
   pthread_mutex_unlock(&_mutex);
}

char * dequeue(struct circqueue *q) {
   char * data;

   pthread_mutex_lock(&_mutex);
   if(isemptyqueue(q)) {
      q->thread_waiting++;
      while (1) {
	if (q->thread_waiting == q->thread_count) {
	  // all threads are waiting. Quit!
	  pthread_mutex_unlock(&_mutex);
	  return NULL;
	}
	pthread_cond_wait(&_cond, &_mutex);
	if (!isemptyqueue(q)) {
	  break;
	}
      }
      q->thread_waiting--;
   }
   else {
      data=q->array[q->front];
      if(q->front==q->rear)
         q->front=q->rear=-1;
      else
         q->front=(q->front+1)%q->capacity;
   }
   pthread_mutex_unlock(&_mutex);

   return data;
}
