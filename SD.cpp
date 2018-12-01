#ifndef SD_
#define SD_
#include "../gqfast_executor.hpp"

#include <atomic>
#define NUM_THREADS 4

using namespace std;

static args_threading arguments[NUM_THREADS];


static uint32_t* R;
static int* RC;

static uint64_t** dt1_col0_buffer;
static uint64_t** dt2_col0_buffer;

extern inline void SD_dt1_col0_decode_UA(uint32_t* dt1_col0_ptr, uint32_t dt1_col0_bytes, uint32_t & dt1_fragment_size) __attribute__((always_inline));

void* pthread_SD_worker(void* arguments);

extern inline void SD_dt2_col0_decode_UA_threaded(int thread_id, uint32_t* dt2_col0_ptr, uint32_t dt2_col0_bytes, uint32_t & dt2_fragment_size) __attribute__((always_inline));

void SD_dt1_col0_decode_UA(uint32_t* dt1_col0_ptr, uint32_t dt1_col0_bytes, uint32_t & dt1_fragment_size) {

	dt1_fragment_size = dt1_col0_bytes/4;

	for (uint32_t i=0; i<dt1_fragment_size; i++) {
		dt1_col0_buffer[0][i] = *dt1_col0_ptr++;
	}
}

void* pthread_SD_worker(void* arguments) {

	args_threading* args = (args_threading *) arguments;

	uint32_t dt1_it = args->start;
	uint32_t dt1_fragment_size = args->end;
	int thread_id = args->thread_id;

	for (; dt1_it < dt1_fragment_size; dt1_it++) {

		uint32_t dt1_col0_element = dt1_col0_buffer[0][dt1_it];

		uint32_t* row_dt2 = idx[4]->index_map[dt1_col0_element];
		uint32_t dt2_col0_bytes = idx[4]->index_map[dt1_col0_element+1][0] - row_dt2[0];
		if(dt2_col0_bytes) {

			uint32_t* dt2_col0_ptr = reinterpret_cast<uint32_t *>(&(idx[4]->fragment_data[0][row_dt2[0]]));
			uint32_t dt2_fragment_size = 0;
			SD_dt2_col0_decode_UA_threaded(thread_id, dt2_col0_ptr, dt2_col0_bytes, dt2_fragment_size);

			for (uint32_t dt2_it = 0; dt2_it < dt2_fragment_size; dt2_it++) {
				uint32_t dt2_col0_element = dt2_col0_buffer[thread_id][dt2_it];

				RC[dt2_col0_element] = 1;

				pthread_spin_lock(&r_spin_locks[dt2_col0_element]);
				R[dt2_col0_element] += 1;
				pthread_spin_unlock(&r_spin_locks[dt2_col0_element]);

			}
		}
	}
	return nullptr;
}

void SD_dt2_col0_decode_UA_threaded(int thread_id, uint32_t* dt2_col0_ptr, uint32_t dt2_col0_bytes, uint32_t & dt2_fragment_size) {

	dt2_fragment_size = dt2_col0_bytes/4;

	for (uint32_t i=0; i<dt2_fragment_size; i++) {
		dt2_col0_buffer[thread_id][i] = *dt2_col0_ptr++;
	}
}

extern "C" uint32_t* SD(int** null_checks) {

	benchmark_t1 = chrono::steady_clock::now();

	dt1_col0_buffer = new uint64_t*[NUM_THREADS];
	for (int i=0; i<NUM_THREADS; i++) {
		dt1_col0_buffer[i] = new uint64_t[133];
	}
	dt2_col0_buffer = new uint64_t*[NUM_THREADS];
	for (int i=0; i<NUM_THREADS; i++) {
		dt2_col0_buffer[i] = new uint64_t[753];
	}

	RC = new int[5001]();
	R = new uint32_t[5001]();


	uint64_t d0_list[1];
	d0_list[0] = query_parameters[0];

	for (int d0_it = 0; d0_it<1; d0_it++) {

		uint64_t d0_col0_element = d0_list[d0_it];

		uint32_t* row_dt1 = idx[3]->index_map[d0_col0_element];
		uint32_t dt1_col0_bytes = idx[3]->index_map[d0_col0_element+1][0] - row_dt1[0];
		if(dt1_col0_bytes) {

			uint32_t* dt1_col0_ptr = reinterpret_cast<uint32_t *>(&(idx[3]->fragment_data[0][row_dt1[0]]));
			uint32_t dt1_fragment_size = 0;
			SD_dt1_col0_decode_UA(dt1_col0_ptr, dt1_col0_bytes, dt1_fragment_size);

			uint32_t thread_size = dt1_fragment_size/NUM_THREADS;
			uint32_t position = 0;

			for (int i=0; i<NUM_THREADS; i++) {
				arguments[i].start = position;
				position += thread_size;
				arguments[i].end = position;
				arguments[i].thread_id = i;
			}
			arguments[NUM_THREADS-1].end = dt1_fragment_size;

			for (int i=0; i<NUM_THREADS; i++) {
				pthread_create(&threads[i], NULL, &pthread_SD_worker, (void *) &arguments[i]);
			}

			for (int i=0; i<NUM_THREADS; i++) {
				pthread_join(threads[i], NULL);
			}
		}
	}


	for (int i=0; i<NUM_THREADS; i++) {
		delete[] dt1_col0_buffer[i];
	}
	delete[] dt1_col0_buffer;
	for (int i=0; i<NUM_THREADS; i++) {
		delete[] dt2_col0_buffer[i];
	}
	delete[] dt2_col0_buffer;


	*null_checks = RC;
	return R;

}

#endif

