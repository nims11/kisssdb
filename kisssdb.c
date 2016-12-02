#include "kisssdb.h"

/* djb2 hash function
 * Copied from https://github.com/adamierymenko/kissdb/
 */

const uint32_t magic = ('k' << 24) | ('s' << 16) | ('d' << 8) | ('b');
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
        if(fread(&header, sizeof(Header), 1, f) != 1){
            fclose(f);
            return DB_IO_ERROR;
        }
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
    if(fread(&record, sizeof(Record), 1, db->f) != 1) return DB_IO_ERROR;
    while(record > 0){
        if(fseeko(db->f, record, SEEK_SET)) return DB_IO_ERROR;
        prev_offset = ftello(db->f);
        if(fread(&record, sizeof(Record), 1, db->f) != 1) return DB_IO_ERROR;
    }
    fseeko(db->f, 0, SEEK_END);
    offset = ftello(db->f);
    record = 0;
    if(
        fwrite(&record, sizeof(Record), 1, db->f) != 1 || 
        fwrite(&entry_header, sizeof(EntryHeader), 1, db->f) != 1 || 
        fwrite(key, keysize, 1, db->f) != 1 || 
        fwrite(value, valsize, 1, db->f) != 1 || 
        fseeko(db->f, prev_offset, SEEK_SET) != 0
      ) return DB_IO_ERROR;
    record = offset;
    if(fwrite(&record, sizeof(Record), 1, db->f) != 1) return DB_IO_ERROR;
    fflush(db->f);
    return 0;
}

int DB_get(DB *db, const void *key, uint32_t keysize, void *value){
    EntryHeader entry_header;
    uint64_t iter = 0;
    Record record;
    uint64_t hash = DB_hash(key, keysize) % db->size;
    uint64_t offset = sizeof(Header) + sizeof(Record)*hash;
    if(fseeko(db->f, offset, SEEK_SET)) return DB_IO_ERROR;
    if(fread(&record, sizeof(Record), 1, db->f) != 1) return DB_IO_ERROR;
    while(record > 0){
        int match = 1;
        if(fseeko(db->f, record, SEEK_SET)) return DB_IO_ERROR;
        if(fread(&record, sizeof(Record), 1, db->f) != 1) return DB_IO_ERROR;
        if(fread(&entry_header, sizeof(EntryHeader), 1, db->f) != 1) return DB_IO_ERROR;
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
