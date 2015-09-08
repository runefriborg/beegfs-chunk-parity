/* Performance profiling for a threaded environment
 * 
 * In a worker thread:
 *   perf_entry_t * worker = perf_create("Worker1", 0, -1);
 * 
 *   perf_update_start(worker); // Set t1=now
 *   // Do work
 *   perf_update_tick(worker); // record now-t1. Set t1=now
 *
 *   perf_submit(worker); // Send profiling data to main record
 * 
 * In the main thread:
 *   perf_global_init();
 *   // Start threads
 *   // Stop threads
 *   perf_output_report(0);
 *   perf_global_free();
 * 
 */ 
#include <pthread.h>
#include <stdlib.h> 
#include <string.h> 
#include <errno.h>
#include <stdio.h> 
#include <sys/time.h>

#include "perf.h"

#define MIN(a,b) (((a)<(b))?(a):(b))
#define MAX(a,b) (((a)>(b))?(a):(b))

// INIT GLOBAL RECORD
perf_global_t report;

/* single thread usage */
perf_entry_t * perf_create(char * name, int id, int parent_id) {
  perf_entry_t *p = (perf_entry_t *) malloc(sizeof(perf_entry_t));

  if (p == NULL) {
    fprintf(stderr, "Unable to allocate memory: %s\n", strerror(errno));
    exit(1);
  }
  
  strncpy(p->name, name, 256);
  p->id = id;
  p->parent_id = parent_id;
  p->count = 0;
  p->sum = 0.0;
  p->max = 0.0;
  p->min = 10000000000;
  
  return p;
}

/* single thread usage */
void perf_update_start(perf_entry_t * p) {
  struct timeval tv;
  gettimeofday(&tv, NULL); 
  p->last_update_time = tv.tv_sec + tv.tv_usec / 1000000.0; 
}

/* single thread usage */
void perf_update_tick(perf_entry_t * p) {
  struct timeval tv;
  double t,d;
  gettimeofday(&tv, NULL); 
  t = tv.tv_sec + tv.tv_usec / 1000000.0; 

  d = t - p->last_update_time;

  // Prepare for possible next tick
  p->last_update_time = t;

  // Update values
  p->max = MAX(p->max, d);
  p->min = MIN(p->min, d);
  p->sum = p->sum + d;
  p->count++;
}

/* single thread usage */
void perf_output_progress(perf_entry_t * p) {
  if (p->count > 0) {
    printf(" *** %-10s : count %d, sum %f, avg %f, min %f, max %f\n", p->name, p->count, p->sum, p->sum/p->count, p->min, p->max); 
  }
}

/* main thread usage */
void perf_global_init() {
  int i = 0;
  for (i = 0; i < PERF_MAX_ID; i++) {
    report.C[i] = NULL;
  }
  pthread_mutex_init(&(report.mutex), NULL);
}

/* main thread usage */
void perf_global_free() {
  int i = 0;
  pthread_mutex_lock(&(report.mutex));
  for (i = 0; i < PERF_MAX_ID; i++) {
    if (report.C[i] != NULL) {
      free(report.C[i]);
      report.C[i] = NULL;
    }
  }
  pthread_mutex_unlock(&(report.mutex));
}

/* multiple thread usage */
void perf_submit(perf_entry_t * p) {
  pthread_mutex_lock(&(report.mutex));
  report.C[p->id] = p;
  pthread_mutex_unlock(&(report.mutex));  
}

/* main thread usage */
void _perf_output_report(int id, uint8_t depth) {
  int i;
  perf_entry_t * n = report.C[id];
  
  if (n != NULL) {
    // output entry
    for (i = 0; i < depth; i++) {
      printf("  ");
    }
    printf(" *** ");
    printf("%-10s : count %d, sum %f, avg %f, min %f, max %f\n", n->name, n->count, n->sum, n->sum/n->count, n->min, n->max); 
    
    // find children
    for (i = 0; i < PERF_MAX_ID; i++) {
      if (report.C[i] != NULL && report.C[i]->parent_id == n->id) {
	// child
	_perf_output_report(i, depth+1);
      }
    }
  }
}

void perf_output_report(int id) {
  _perf_output_report(id, 0);
}
