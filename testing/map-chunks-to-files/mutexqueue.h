#include <pthread.h>

pthread_mutex_t _mutex;
pthread_cond_t _cond;

struct circqueue {
   int front,rear;
   int capacity;
   int threadcount;
   char **array;
};


struct circqueue *mutexqueue(int threadcount, int size) {
   struct circqueue *q=malloc(sizeof(struct circqueue));
   if(!q)return NULL;
   q->threadcount=threadcount;
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
      pthread_cond_signal(&_cond);
   }
   pthread_mutex_unlock(&_mutex);
}

char * dequeue(struct circqueue *q) {
   char * data;

   pthread_mutex_lock(&_mutex);
   if(isemptyqueue(q)) {
      printf("queue underflow");
      pthread_cond_wait(&_cond, &_mutex);
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
