#ifndef _HEADER_PERF_H_
#define _HEADER_PERF_H_
#include <time.h>
#include <pthread.h>
#include <stdint.h>

#define PERF_MAX_ID 99

typedef struct {
  int id;
  int parent_id;
  char name [256];
  double last_update_time;
  double sum;
  double max;
  double min;
  int count;
} perf_entry_t;

/* Global entry for collecting data
   This is initialised in perf.c
*/
typedef struct {
  pthread_mutex_t mutex;
  perf_entry_t * C [PERF_MAX_ID];
} perf_global_t;

extern perf_global_t report;

/* Functions for single thread use */
perf_entry_t * perf_create(char * name, int id, int parent_id);
void perf_update_start(perf_entry_t * p);
void perf_update_tick(perf_entry_t * p); // may be called more than once
void perf_output_progress(perf_entry_t * p); // output local progress

/* Functions for multiple thread use */
void perf_global_init();
void perf_submit(perf_entry_t * p);
void perf_output_report(int id);
void perf_global_free();

#endif
