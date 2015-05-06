#include <pthread.h>


struct circqueue {
  pthread_mutex_t _mutex;
  pthread_cond_t _cond;
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
   pthread_mutex_init(&q->_mutex, NULL);
   pthread_cond_init (&q->_cond, NULL);

   return q;
}

void mutexqueue_destroy(struct circqueue *q) {
  pthread_mutex_destroy(&q->_mutex);
  pthread_cond_destroy(&q->_cond);
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

  pthread_mutex_lock(&q->_mutex);
   if(isfullqueue(q))
      printf("queue overflow\n");
   else{
      q->rear=(q->rear+1)%q->capacity;
      q->array[q->rear]=x;
      if(q->front==-1) {
         q->front=q->rear;
      }
      if (q->thread_waiting > 0) {
	pthread_cond_signal(&q->_cond);
      }
   }
   pthread_mutex_unlock(&q->_mutex);
}

char * dequeue(struct circqueue *q) {
   char * data;

   pthread_mutex_lock(&q->_mutex);
   if(isemptyqueue(q)) {
      q->thread_waiting++;
      printf("waitinc");
      while (1) {
	if (q->thread_waiting == q->thread_count) {
	  // all threads are waiting. Quit!
	  printf("quit");
	  pthread_cond_broadcast(&q->_cond);
	  pthread_mutex_unlock(&q->_mutex);
	  return NULL;
	}
	printf("wait");
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
