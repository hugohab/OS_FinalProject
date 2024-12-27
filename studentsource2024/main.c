
#include "sbuffer.h"
#include "config.h"
#include <stdlib.h>
#include <stdio.h>
#include <pthread.h>
#include <unistd.h>
#include <stdbool.h>

#define NUM_THREADS 3 // One producer and two consumers

pthread_mutex_t csv_mutex; // Mutex for synchronizing access to the output file

// Struct to hold arguments for thread functions
typedef struct thread_parameters {
    sbuffer_t *shared_buf;    // Pointer to the shared buffer
    FILE *sensor_file;        // Pointer to the sensor data file
} thread_parameters_t;

/**
 * Opens a file for reading or appending
 * @param filename The file name
 * @param append Flag to indicate append mode (true for append, false for overwrite)
 * @return FILE pointer on success, NULL on failure
 */
FILE *initialize_file(const char *filename, bool append) {
    FILE *file_ptr;
    file_ptr = fopen(filename, append ? "a" : "w");
    if (!file_ptr) {
        perror("Unable to open the specified file");
    }
    return file_ptr;
}

/**
 * Writes sensor data into a CSV file
 * @param output_file File pointer to the output CSV
 * @param sensor_id ID of the sensor
 * @param sensor_value Value recorded by the sensor
 * @param timestamp Timestamp of the sensor data
 * @return 0 if successful, -1 if an error occurs
 */
int log_sensor_data(FILE *output_file, sensor_id_t sensor_id, sensor_value_t sensor_value, sensor_ts_t timestamp) {
    if (output_file == NULL) return -1;

    if (fprintf(output_file, "%d,%.2f,%ld\n", sensor_id, sensor_value, timestamp) < 0) {
        perror("Error occurred while writing to file");
        return -1;
    }

    if (fflush(output_file) != 0) {
        perror("Failed to flush data to file");
        return -1;
    }

    return 0;
}

/**
 * Producer thread function
 * Reads sensor data from a binary file and pushes it to the shared buffer
 * @param args Pointer to the thread parameters
 * @return NULL
 */
void *producer_thread(void *args) {
    thread_parameters_t *parameters = (thread_parameters_t *)args;
    sbuffer_t *buffer = parameters->shared_buf;
    FILE *sensor_input = parameters->sensor_file;

    sensor_data_t sensor_entry;

    if (sensor_input == NULL) {
        fprintf(stderr, "Error: Input file is not available.\n");
        pthread_exit(NULL);
    }

    // Read data from file and insert into shared buffer
    while ((fread(&sensor_entry.id, sizeof(sensor_id_t), 1, sensor_input) == 1) &&
           (fread(&sensor_entry.value, sizeof(sensor_value_t), 1, sensor_input) == 1) &&
           (fread(&sensor_entry.ts, sizeof(sensor_ts_t), 1, sensor_input) == 1)) {

        if (sbuffer_insert(buffer, &sensor_entry) != SBUFFER_SUCCESS) {
            fprintf(stderr, "Buffer insertion failed for data: ID=%d\n", sensor_entry.id);
        }

        usleep(10000); // Simulate delay in data production
    }

    // Signal end-of-stream to consumers
    sensor_data_t end_signal = {0, 0.0, 0};
    for (int i = 0; i < NUM_THREADS - 1; i++) {
        sbuffer_insert(buffer, &end_signal);
    }

    pthread_exit(NULL);
}

/**
 * Consumer thread function
 * Consumes sensor data from the shared buffer and writes it to the CSV file
 * @param args Pointer to the thread parameters
 * @return NULL
 */
void *consumer_thread(void *args) {
    thread_parameters_t *parameters = (thread_parameters_t *)args;
    sbuffer_t *buffer = parameters->shared_buf;
    FILE *output_csv = parameters->sensor_file;

    sensor_data_t retrieved_data;

    while (true) {
        int status = sbuffer_remove(buffer, &retrieved_data);

        if (status == SBUFFER_FAILURE) {
            fprintf(stderr, "Buffer read encountered an error.\n");
            continue;
        }

        if (retrieved_data.id == 0) {
            break; // End-of-stream signal
        }

        // Protect file write with mutex
        pthread_mutex_lock(&csv_mutex);

        if (log_sensor_data(output_csv, retrieved_data.id, retrieved_data.value, retrieved_data.ts) != 0) {
            fprintf(stderr, "Failed to log data: ID=%d\n", retrieved_data.id);
        } else {
            printf("Logged: SensorID=%d, Value=%.2f, Timestamp=%ld\n", 
                   retrieved_data.id, retrieved_data.value, retrieved_data.ts);
        }

        pthread_mutex_unlock(&csv_mutex);

        usleep(25000); // Simulate data processing time
    }

    pthread_exit(NULL);
}

/**
 * Main function
 * Sets up the shared buffer, threads, and synchronization primitives
 */
int main() {
    // Initialize the mutex for file access
    pthread_mutex_init(&csv_mutex, NULL);

    // Open the input and output files
    FILE *sensor_data_file = fopen("sensor_data", "rb");
    FILE *csv_output_file = initialize_file("sensor_data_out.csv", false);

    if (!sensor_data_file || !csv_output_file) {
        fprintf(stderr, "Error: Could not open required files.\n");
        exit(EXIT_FAILURE);
    }

    // Initialize the shared buffer
    sbuffer_t *shared_buffer;
    if (sbuffer_init(&shared_buffer) != SBUFFER_SUCCESS) {
        fprintf(stderr, "Error: Failed to initialize shared buffer.\n");
        exit(EXIT_FAILURE);
    }

    // Prepare thread arguments
    thread_parameters_t producer_args = {shared_buffer, sensor_data_file};
    thread_parameters_t consumer_args = {shared_buffer, csv_output_file};

    // Create threads
    pthread_t threads[NUM_THREADS];
    pthread_create(&threads[0], NULL, producer_thread, &producer_args); // Producer thread
    pthread_create(&threads[1], NULL, consumer_thread, &consumer_args); // Consumer thread 1
    pthread_create(&threads[2], NULL, consumer_thread, &consumer_args); // Consumer thread 2

    // Wait for all threads to complete
    for (int i = 0; i < NUM_THREADS; i++) {
        pthread_join(threads[i], NULL);
    }

    // Cleanup resources
    if (sbuffer_free(&shared_buffer) != SBUFFER_SUCCESS) {
        fprintf(stderr, "Error: Could not free the shared buffer.\n");
        exit(EXIT_FAILURE);
    }
    fclose(sensor_data_file);
    fclose(csv_output_file);

    // Destroy the mutex
    pthread_mutex_destroy(&csv_mutex);

    return 0;
}