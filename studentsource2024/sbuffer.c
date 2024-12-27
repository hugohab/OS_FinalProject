/**
 * \author {AUTHOR}
 */

#include <stdlib.h>
#include <stdio.h>
#include "sbuffer.h"
#include <pthread.h>

/**
 * basic node for the buffer, these nodes are linked together to create the buffer
 */
typedef struct sbuffer_node {
    struct sbuffer_node *next;  /**< a pointer to the next node*/
    sensor_data_t data;         /**< a structure containing the data */
} sbuffer_node_t;

/**
 * a structure to keep track of the buffer
 */
struct sbuffer {
    sbuffer_node_t *head;       /**< a pointer to the first node in the buffer */
    sbuffer_node_t *tail;       /**< a pointer to the last node in the buffer */
    //The project involves multi-threading, where multiple threads (the producer and consumers) 
    //access shared resources like the buffer and the CSV file. Without proper synchronization, data races and inconsistent states could occur.
    pthread_mutex_t mutex;    // Mutex
    pthread_cond_t condition;  // Condition variable
};

int sbuffer_init(sbuffer_t **buffer) {
    *buffer = malloc(sizeof(sbuffer_t));
    if (*buffer == NULL) return SBUFFER_FAILURE;
    (*buffer)->head = NULL;
    (*buffer)->tail = NULL;

    //We have to verify whether the mutex or condition variable were successfully initialized.
    // Check and intialize mutex
    if (pthread_mutex_init(&(*buffer)->mutex, NULL) != 0) {
        free(*buffer);
        return SBUFFER_FAILURE; // Mutex initialization failed
    }

    // Check and initialize condition variable
    if (pthread_cond_init(&(*buffer)->condition, NULL) != 0) {
        pthread_mutex_destroy(&(*buffer)->mutex);
        free(*buffer);
        return SBUFFER_FAILURE; // Condition variable initialization failed
    }

    return SBUFFER_SUCCESS;
}

int sbuffer_free(sbuffer_t **buffer) {
    sbuffer_node_t *dummy;
    if ((buffer == NULL) || (*buffer == NULL)) {
        return SBUFFER_FAILURE;
    }
    while ((*buffer)->head) {
        dummy = (*buffer)->head;
        (*buffer)->head = (*buffer)->head->next;
        free(dummy);
    }

    //Destroy synchronization variables
    pthread_mutex_destroy(&(*buffer)->mutex);
    pthread_cond_destroy(&(*buffer)->condition);
    free(*buffer);
    *buffer = NULL;
    return SBUFFER_SUCCESS;
}

int sbuffer_remove(sbuffer_t *buffer, sensor_data_t *data) {
    sbuffer_node_t *dummy;

    if (buffer == NULL) return SBUFFER_FAILURE;

    //Makes sure of exclusive access to the buffer
    pthread_mutex_lock(&buffer->mutex);

    
    while (buffer->head == NULL) { // Wait if the buffer is empty
        pthread_cond_wait(&buffer->condition, &buffer->mutex); // Block until data is available
    }    

    //Check if the head node contains the end-of-stream marker (id == 0).
    if (buffer->head->data.id == 0) {
        pthread_mutex_unlock(&buffer->mutex);
        printf("No further valid data in the buffer(empty).\n");
        return SBUFFER_NO_DATA; // No valid data
    }

    //if (buffer->head == NULL) return SBUFFER_NO_DATA;
    *data = buffer->head->data;
    dummy = buffer->head;

    if (buffer->head == buffer->tail) // buffer has only one node
    {
        buffer->head = buffer->tail = NULL;
    } else  // buffer has many nodes empty
    {
        buffer->head = buffer->head->next;
    }
    free(dummy);

    pthread_mutex_unlock(&buffer->mutex);
    return SBUFFER_SUCCESS;
}

int sbuffer_insert(sbuffer_t *buffer, sensor_data_t *data) {
    sbuffer_node_t *dummy;

    if (buffer == NULL) return SBUFFER_FAILURE;

    dummy = malloc(sizeof(sbuffer_node_t));
    if (dummy == NULL) return SBUFFER_FAILURE;

    dummy->data = *data;
    dummy->next = NULL;
    
    pthread_mutex_lock(&buffer->mutex);
    if (buffer->tail == NULL) // buffer empty (buffer->head should also be NULL
    {
        buffer->head = buffer->tail = dummy;
    } else // buffer not empty
    {
        buffer->tail->next = dummy;
        buffer->tail = buffer->tail->next;
    }

    //Signal new data is ready to be removed
    pthread_cond_signal(&buffer->condition);
    pthread_mutex_unlock(&buffer->mutex);

    return SBUFFER_SUCCESS;
}
