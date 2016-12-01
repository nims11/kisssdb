#include "kisssdb.h"

#include <malloc.h>
#include <string.h>
#include <stdlib.h>
/* djb2 hash function
 * Copied from https://github.com/adamierymenko/kissdb/
 */
static uint64_t DB_hash(const void *b,unsigned long len){
    unsigned long i;
    uint64_t hash = 5381;
    for(i=0;i<len;++i)
        hash = ((hash << 5) + hash) + (uint64_t)(((const uint8_t *)b)[i]);
    return hash;
}

int DB_create(const char *path, uint32_t hash_table_size){
    FILE *f = fopen(path, "wb");
    uint32_t iter;
    if(!f){
        return DB_IO_ERROR;
    }
    if(fseeko(f, 0, SEEK_END)){
        fclose(f);
        return DB_IO_ERROR;
    }
    if(!hash_table_size){
        fclose(f);
        return DB_INVALID_PARAMETER_ERROR;
    }

    if(fseeko(f, 0, SEEK_SET)){fclose(f); return DB_IO_ERROR;}
    else{
        Header header = {magic, hash_table_size};
        if(fwrite(&header, sizeof(Header), 1, f) != 1){fclose(f); return DB_IO_ERROR;}
        for(iter = 0;iter < hash_table_size; iter++){
            Record record = 0;
            if(fwrite(&record, sizeof(Record), 1, f) != 1){fclose(f); return DB_IO_ERROR;}
        }
    }

    fclose(f);
    return 0;
}

int DB_open(DB *db, const char *path){
    FILE *f = fopen(path, "r+b");
    db->f = f;
    if(!f){
        return DB_IO_ERROR;
    }
    if(fseeko(f, 0, SEEK_END)){
        fclose(f);
        return DB_IO_ERROR;
    }
    if(fseeko(f, 0, SEEK_SET)){fclose(f); return DB_IO_ERROR;}
    else{
        Header header;
        fread(&header, sizeof(Header), 1, f);
        db->size = header.size;
    }
    return 0;
}

int DB_add(DB *db, const void *key, uint32_t keysize, const void *value, uint32_t valsize){
    EntryHeader entry_header = {keysize, valsize};
    Record record;
    uint64_t hash = DB_hash(key, keysize) % db->size;
    uint64_t offset = sizeof(Header) + sizeof(Record)*hash;
    uint64_t prev_offset;
    if(fseeko(db->f, offset, SEEK_SET)) return DB_IO_ERROR;
    prev_offset = ftello(db->f);
    fread(&record, sizeof(Record), 1, db->f);
    while(record > 0){
        if(fseeko(db->f, record, SEEK_SET)) return DB_IO_ERROR;
        prev_offset = ftello(db->f);
        fread(&record, sizeof(Record), 1, db->f);
    }
    fseeko(db->f, 0, SEEK_END);
    offset = ftello(db->f);
    record = 0;
    fwrite(&record, sizeof(Record), 1, db->f);
    fwrite(&entry_header, sizeof(EntryHeader), 1, db->f);
    fwrite(key, keysize, 1, db->f);
    fwrite(value, valsize, 1, db->f);
    fseeko(db->f, prev_offset, SEEK_SET);
    record = offset;
    fwrite(&record, sizeof(Record), 1, db->f);
    return 0;
}

int DB_get(DB *db, const void *key, uint32_t keysize, void *value){
    EntryHeader entry_header;
    uint64_t iter = 0;
    Record record;
    uint64_t hash = DB_hash(key, keysize) % db->size;
    uint64_t offset = sizeof(Header) + sizeof(Record)*hash;
    if(fseeko(db->f, offset, SEEK_SET)) return DB_IO_ERROR;
    fread(&record, sizeof(Record), 1, db->f);
    while(record > 0){
        int match = 1;
        if(fseeko(db->f, record, SEEK_SET)) return DB_IO_ERROR;
        fread(&record, sizeof(Record), 1, db->f);
        fread(&entry_header, sizeof(EntryHeader), 1, db->f);
        if(keysize != entry_header.key_size)continue;
        for(iter = 0 ;iter < keysize && match; iter++){
            char ch = getc(db->f);
            if(ch != ((char*)key)[iter]) match = 0;
        }
        if(!match)
            continue;
        for(iter = 0; iter < entry_header.val_size; iter++){
            ((char*) value)[iter] = getc(db->f);
        }
        return 0;
    }
    return DB_KEY_NOT_FOUND;
}

void DB_close(DB *db){
    fclose(db->f);
}

int main(int argc, char **argv){
    if(argc == 3){
        DB db;
        char key[1000000];
        char val[1000000];
        DB_create(argv[1], atoi(argv[2]));
        DB_open(&db, argv[1]);
        while(fgets(key, sizeof(key), stdin)){
            fgets(val, sizeof(val), stdin);
            key[strcspn(key, "\r\n")] = 0;
            val[strcspn(val, "\r\n")] = 0;
            DB_add(&db, key, strlen(key)+1, val, strlen(val)+1);
        }
        DB_close(&db);
    }else if(argc == 2){
        DB db;
        char key[1000000];
        char val[1000000];
        DB_open(&db, argv[1]);
        while(fgets(key, sizeof(key), stdin)){
            key[strcspn(key, "\r\n")] = 0;
            if(DB_get(&db, key, strlen(key)+1, val)){
                printf("\n");
            }else
                printf("%s\n", val);
        }
        DB_close(&db);
    }else{
        DB db;
        char command[10];
        char key[1000000];
        char val[1000000];
        DB_create("/tmp/x.db", 1000081);
        DB_open(&db, "/tmp/x.db");
        while(fgets(command, sizeof(command), stdin)){
            command[strcspn(command, "\r\n")] = 0;
            if(!strcmp("ADD", command)){
                fgets(key, sizeof(key), stdin);
                fgets(val, sizeof(val), stdin);
                key[strcspn(key, "\r\n")] = 0;
                val[strcspn(val, "\r\n")] = 0;
                DB_add(&db, key, strlen(key)+1, val, strlen(val)+1);
            }else if(!strcmp("GET", command)){
                fgets(key, sizeof(key), stdin);
                key[strcspn(key, "\r\n")] = 0;
                if(DB_get(&db, key, strlen(key)+1, val)){
                    printf("\n");
                }else
                    printf("%s\n", val);
            }
        }
        DB_close(&db);
    }
}
