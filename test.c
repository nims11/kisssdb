#include "kisssdb.h"
#include <string.h>
#include <malloc.h>
#include <stdlib.h>
#include <assert.h>
#define BUFSIZE 4000
char buffer[BUFSIZE];
int inp_line(char **str, int *cur_size){
    int len = 0;
    while(fgets(buffer, sizeof(buffer), stdin)){
        int buf_len = strlen(buffer);
        if(buf_len + len + 1 > *cur_size){
            *cur_size += BUFSIZE;
            char *extra = (char*)realloc(*str, *cur_size);
            *str = extra;
        }
        strcpy((*str)+len, buffer);
        len += buf_len;
        (*str)[len] = 0;
        if((*str)[len-1] == '\n'){
            (*str)[len-1] = 0;
            break;
        }
    }
    return len;
}

int main(int argc, char **argv){
    DB db;
    int cur_key_size = 1000;
    int cur_val_size = 1000000;
    char *key = malloc(cur_key_size);
    char *val = malloc(cur_val_size);
    if(argc == 3){
        DB_create(argv[1], atoi(argv[2]));
        DB_open(&db, argv[1]);
        while(inp_line(&key, &cur_key_size)){
            inp_line(&val, &cur_val_size);
            DB_add(&db, key, strlen(key)+1, val, strlen(val)+1);
        }
        DB_close(&db);
    }else if(argc == 2){
        DB_open(&db, argv[1]);
        while(inp_line(&key, &cur_key_size)){
            fflush(stdin);
            if(DB_get(&db, key, strlen(key)+1, val)){
                printf("\n");
            }else
                printf("%s\n", val);
        }
        DB_close(&db);
    }else {
        printf("Testing...\n");
        printf("Creating test.db with size 13\n");
        DB_create("test.db", 23);
        DB_open(&db, "test.db");
        int N = 100;
        printf("Starting to add %d random entries\n", N);
        char keys[100][1000];
        char vals[100][1000];
        for(int i = 0;i<N;i++){
            int len = (rand() % 1000) + 1;
            for(int j = 0;j<len-1;j++){
                keys[i][j] = (rand() % 100) + 1;
            }
            keys[i][len-1] = 0;
            len = (rand() % 1000) + 1;
            for(int j = 0;j<len-1;j++){
                vals[i][j] = (rand() % 100) + 1;
            }
            vals[i][len-1] = 0;
            DB_add(&db, keys[i], strlen(keys[i])+1, vals[i], strlen(vals[i])+1);
        }

        DB_close(&db);
        printf("Finished adding. Beginning verification...\n");
        DB_open(&db, "test.db");
        for(int i = 0;i<N;i++){
            DB_get(&db, keys[i], strlen(keys[i])+1, val);
            assert(strcmp(val, vals[i]) == 0);
        }
        DB_close(&db);
        printf("Tests were successful!\n");
    }
    free(key);
    free(val);
    return 0;
}
