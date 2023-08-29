// Haim Heger 318424900
// option 2
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>
#include <time.h>
#include <semaphore.h>

#define CATEGORIES_NUM 3
#define LAST -1
#define MAX_LINE_LENGTH 200

enum category {SPORTS, NEWS, WEATHER};
typedef enum category category;

char* msg[CATEGORIES_NUM] = {"SPORTS", "NEWS", "WEATHER"};

struct Node{
    int producerId;
    int nodeId;
    int category;
    struct Node* next;
};
typedef struct Node Node;


struct Q{
    int id;
    int limit;
    int overall;
    int current;
    int counters[CATEGORIES_NUM];
    sem_t empty;
    sem_t full;
    pthread_mutex_t lock;
    Node* head;
    Node* tail;
};
typedef struct Q Q;

struct dispatchArgs {
    int numOfOrigins;
    Q* origin;
    Q* dest;
};
typedef struct dispatchArgs dispatchArgs;

struct dispatchStruct {
    int numOfOrigins;
    Q** origins;
    Q* dest1;
    Q* dest2;
    Q* dest3;
};
typedef struct dispatchStruct dispatchStruct;

// print a node
void printNode(Node* n) {
    printf("producer %d %s %d\n",n->producerId, msg[n->category], n->nodeId);
}

// amke a queue
Q* initQ(int id, int overall, int limit) {
    Q* q = (Q*)malloc(sizeof(Q));
    if(!q) {
        perror("malloc failed\n");
        return NULL;
    }
    q->id = id;
    q->head = NULL;
    q->tail = NULL;
    q->overall = overall;
    q->limit = limit;
    q->current = 0;
    int i = 0;
    for(i; i < CATEGORIES_NUM; i++) {
        q->counters[i] = 0;
    }
    sem_init(&q->empty, 0, (unsigned)limit);
    sem_init(&q->full, 0, 0);
    pthread_mutex_init(&q->lock, NULL);
    return q;
}

// add to queue
void enqueue(Q* q, Node* n) {
    n->next = NULL;
    if(q->current == 0) {
        q->head = n;
        q->tail = n;
    } else if(q->current == 1) {
        q->head->next = n;
        q->tail = n;
    } else {
        q->tail->next = n;
        q->tail = n;
    }
    q->current++;
}

// get from quue
Node* dequeue(Q* q) {
    Node* n = q->head;
    q->head = q->head->next;
    q->current--;
    return n;
}

// producer function
void* produce(void* args) {
    dispatchArgs* ms = (dispatchArgs*)args;
    Q* q = ms->dest;
    int count = 0;
    while (count < q->overall) {
        // Produce
        int x = rand() % CATEGORIES_NUM;
        Node* n = (Node*)malloc(sizeof(Node));
        if(!n) {
            perror("malloc failed\n");
            return NULL;
        }
        n->category = x;
        n->producerId = q->id;
        sleep(1);

        // Add to the buffer
        sem_wait(&q->empty);
        pthread_mutex_lock(&q->lock);
        n->nodeId = ++q->counters[n->category];
        enqueue(q, n);
        pthread_mutex_unlock(&q->lock);
        sem_post(&q->full);
        count++;
    }
}

// consumer function (screen manager)
void* consumer(void* args) {
    sleep(1);
    dispatchArgs* ms = (dispatchArgs*)args;
    Q* q = ms->origin;
    int count = 0;
    while (count < CATEGORIES_NUM) {
        
        Node* y;

        // Remove from the buffer
        sem_wait(&q->full);
        pthread_mutex_lock(&q->lock);
        y = dequeue(q);
       
        pthread_mutex_unlock(&q->lock);
        sem_post(&q->empty);

        // Consume

        if(y->nodeId == LAST) {
            count++;
        } else {
            printNode(y);
        }
        free(y);
    }
    printf("DONE\n");
}

struct CoEditor {
    Q* origin;
    Q* shared;
};
typedef struct CoEditor CoEditor;

// dispatcher function
void* alt_dispatch(void* args) {
    dispatchStruct* ds = (dispatchStruct*)args;
    
    Q* dest;
    int count1 = 0;
    // counter array;
    int* count2 = (int*)malloc(sizeof(int) * ds->numOfOrigins);
    if(!count2) {
        perror("malloc failed\n");
        return NULL;
    }
    int k = 0;
    for(k; k < ds->numOfOrigins; k++) {
        count2[k] = 0;
    }
    int i = 0;
    while(count1 < ds->numOfOrigins) {
        Q* q = ds->origins[i];
        Node* y;
        if(q->overall <= count2[i]) {
            i = (i + 1) % ds->numOfOrigins;
            continue;
        }
        sem_wait(&q->full);
        pthread_mutex_lock(&q->lock);
        y = dequeue(q);
        pthread_mutex_unlock(&q->lock);
        sem_post(&q->empty);  
        switch (y->category)
        {
        case SPORTS:
            dest = ds->dest1;
            break;
        case NEWS:
            dest = ds->dest2;
            break;
        case WEATHER:
            dest = ds->dest3;
            break;
        }
        // Add to the buffer
        sem_wait(&dest->empty);
        pthread_mutex_lock(&dest->lock);
        enqueue(dest, y);
        pthread_mutex_unlock(&dest->lock);
        sem_post(&dest->full);
        count2[i]++;
        if(q->overall <= count2[i]) {
            count1++;
        }
        // next queue
        i = (i + 1) % ds->numOfOrigins;
        
    }
    free(count2);
    int j = 0;
    for (j; j < CATEGORIES_NUM; j++) {
        Q* q;
        switch (j)
        {
        case SPORTS:
            q = ds->dest1;
            break;
        case NEWS:
            q = ds->dest2;
            break;
        case WEATHER:
            q = ds->dest3;
            break;
        }
        Node* n1 = (Node*)malloc(sizeof(Node));
        if(!n1) {
            perror("malloc failed\n");
            return NULL;
        }
        n1->category = j;
        n1->producerId = LAST;
        n1->nodeId = LAST;
        sem_wait(&q->empty);
        pthread_mutex_lock(&q->lock);
        enqueue(q, n1);
        pthread_mutex_unlock(&q->lock);
        sem_post(&q->full);
    } 
}

// co editor function
void* alt_co_editor(void* args) {
    dispatchArgs* ms = (dispatchArgs*)args;
    Q* dest = ms->dest;
    Q* q = ms->origin; 
    Node* y;
    int i;
    int count2 = 0;
    while(count2 < 1) {
	sleep(1);
        sem_wait(&q->full);
        pthread_mutex_lock(&q->lock);
        y = dequeue(q);
        pthread_mutex_unlock(&q->lock);
        sem_post(&q->empty); 
         
        if(y->producerId == LAST || y->nodeId == LAST) {
            count2++;
        }
        // Add to the buffer
        sem_wait(&dest->empty);
        pthread_mutex_lock(&dest->lock);
        enqueue(dest, y);
        pthread_mutex_unlock(&dest->lock);
        sem_post(&dest->full);        
    }
}


void freeQ(Q* q) {
    sem_destroy(&q->empty);
    sem_destroy(&q->full);
    pthread_mutex_destroy(&q->lock);
    free(q);
}

void freeQarr(Q** arr, int size) {
    int i;
    for(i = 0; i < size; i++) {
        freeQ(arr[i]);
    }
    free(arr);
}

int main(int argc, char* argv[]) {
    
    srand(time(NULL));
    // not enough arguments
    if(argc < 2) {
        exit(0);
    }

    char *path;
    char line[MAX_LINE_LENGTH] = {0};
    char preLine[MAX_LINE_LENGTH] = {0};
    unsigned int line_count = 0;
    path = argv[1];
    
    /* Open file */
    FILE *file = fopen(path, "r");
    if (!file)
    {
        perror(path);
        return EXIT_FAILURE;
    }
    
    // array of producer queues
    Q** arr = NULL;
    int proId, items, qSize, sum, count;

    /* Get each line until there are none left */
    while (fgets(line, MAX_LINE_LENGTH, file))
    {
        if(!strcmp(line, "\n")) {
            continue;
        }
        strcpy(preLine, line);
        line_count++;
        switch (line_count % 3)
        {
            case 0: {
                qSize = atoi(line);
                count++;
                arr = (Q**)realloc(arr, sizeof(Q*) * count);
                if(!arr) {
                    perror("realloc() failed\n");
	            exit(1);
                }
                arr[count - 1] = initQ(proId, items, qSize);
            } break;

            case 1: {
                proId = atoi(line);
            } break;

            case 2: {
                items = atoi(line);
                sum += items;
            } break;
        }
    }

    qSize = atoi(preLine);
    /* Close file */
    if (fclose(file))
    {	
	freeQarr(arr, count);
        return EXIT_FAILURE;
        perror(path);
    }

    // make the data structurs needed for the functions
    int threads = count + 2 + CATEGORIES_NUM;
    pthread_t* tid = (pthread_t*)malloc(sizeof(pthread_t) * threads);
    if(!tid) {
        perror("malloc failed\n");
        freeQarr(arr, count);
        exit(1);
    }
    Q* screen = initQ(-2, sum + CATEGORIES_NUM, qSize);
    Q* c1 = initQ(-2, sum + count, sum + count + 1);
    Q* c2 = initQ(-2, sum + count, sum + count + 1);
    Q* c3 = initQ(-2, sum + count, sum + count + 1);

    if(!screen || !c1 || !c2 || !c3) {
        perror("malloc failed\n");
        freeQarr(arr, count);
        free(tid);
        if(screen) {
            freeQ(screen);
        }
        if(c1) {
            freeQ(c1);
        }
        if(c2) {
            freeQ(c2);
        }
        if(c3){
            freeQ(c3);
        }
        exit(1);
    }

    dispatchArgs coEditorArgs1;
    coEditorArgs1.numOfOrigins = count;
    coEditorArgs1.origin = c1;
    coEditorArgs1.dest = screen;

    dispatchArgs coEditorArgs2;
    coEditorArgs2.numOfOrigins = count;
    coEditorArgs2.origin = c2;
    coEditorArgs2.dest = screen;

    dispatchArgs coEditorArgs3;
    coEditorArgs3.numOfOrigins = count;
    coEditorArgs3.origin = c3;
    coEditorArgs3.dest = screen;

    dispatchArgs* producerArgs = (dispatchArgs*)malloc(sizeof(dispatchArgs) * count);
    if(!producerArgs) {
        perror("malloc failed\n");
        freeQarr(arr, count);
        freeQ(c1);
        freeQ(c2);   
        freeQ(c3);
        freeQ(screen);
        free(tid);
        exit(1);
    }
    int i;
    for(i = 0; i < count; i++) {
        producerArgs[i].origin = NULL;
        producerArgs[i].dest = arr[i];
    }

    dispatchStruct disArgs;
    disArgs.numOfOrigins = count;
    disArgs.origins = arr;
    disArgs.dest1 = c1;
    disArgs.dest2 = c2;
    disArgs.dest3 = c3;

    dispatchArgs screenArgs;
    screenArgs.origin = screen;
    screenArgs.dest = NULL;

    // create threads
    for(i = 0; i < count; i++) {
        if(pthread_create(&tid[i], NULL, &produce, &producerArgs[i]) != 0) {
            perror("Error in pthread_create()\n");
        }
    }

    int k = count;
    if(pthread_create(&tid[k++], NULL, &alt_dispatch, &disArgs) != 0) {
        perror("Error in pthread_create()\n");
    }
    if(pthread_create(&tid[k++], NULL, &alt_co_editor, &coEditorArgs1) != 0) {
        perror("Error in pthread_create()\n");
    }
    if(pthread_create(&tid[k++], NULL, &alt_co_editor, &coEditorArgs2) != 0) {
        perror("Error in pthread_create()\n");
    }
    if(pthread_create(&tid[k++], NULL, &alt_co_editor, &coEditorArgs3) != 0) {
        perror("Error in pthread_create()\n");
    }
    if(pthread_create(&tid[k++], NULL, &consumer, &screenArgs) != 0) {
        perror("Error in pthread_create()\n");
    }

    // join threads
    for(i = 0; i < threads; i++) {
        if(pthread_join(tid[i], NULL) != 0) {
            perror("Error in pthread_join()\n");
        }
    }


    // free allocated memory and destroy mutex and sem
    freeQarr(arr, count);
    freeQ(c1);
    freeQ(c2);   
    freeQ(c3);
    freeQ(screen);

    free(tid);
    free(producerArgs);
    return 0;
}
