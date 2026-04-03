#ifndef COMMIT_ENGINE_H
#define COMMIT_ENGINE_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct CeEngine CeEngine;

typedef struct {
    uint16_t shard_id;
    uint32_t wal_segment_id;
    uint64_t wal_offset;
} CeCursor;

typedef enum {
    CE_READ_VISIBLE = 0,
    CE_READ_DURABLE = 1,
} CeReadConsistency;

typedef enum {
    CE_CHECKPOINT_VISIBLE = 0,
    CE_CHECKPOINT_DURABLE = 1,
} CeCheckpointKind;

typedef enum {
    CE_RECORD_PUT_INLINE = 0,
    CE_RECORD_PUT_BLOB = 1,
    CE_RECORD_TOMBSTONE = 2,
} CeRecordType;

typedef enum {
    CE_VALUE_INLINE = 0,
    CE_VALUE_BLOB = 1,
    CE_VALUE_TOMBSTONE = 2,
} CeValueKind;

typedef enum {
    CE_DURABILITY_ULTRAFAST = 0,
    CE_DURABILITY_BATCH = 1,
    CE_DURABILITY_STRICT = 2,
} CeDurability;

typedef struct {
    uint16_t shard_id;
    CeCursor start;
    CeCursor end;
    CeCursor visible;
    CeCursor durable;
    uint8_t has_durable;
    size_t record_count;
    uint64_t first_seqno;
    uint64_t last_seqno;
} CeCommitWindow;

typedef struct {
    uint8_t *ptr;
    size_t len;
} CeBuffer;

typedef struct {
    uint64_t seqno;

    uint16_t cursor_shard_id;
    uint16_t reserved0;
    uint32_t cursor_wal_segment_id;
    uint64_t cursor_wal_offset;

    uint8_t record_type;
    uint8_t value_kind;
    uint8_t durability;
    uint8_t reserved1;

    const uint8_t *key_ptr;
    size_t key_len;

    const uint8_t *value_ptr;
    size_t value_len;
} CeStreamItem;

typedef struct {
    CeStreamItem *items;
    size_t item_count;

    uint16_t next_shard_id;
    uint16_t reserved0;
    uint32_t next_wal_segment_id;
    uint64_t next_wal_offset;

    void *internal;
} CeReadResult;

typedef struct {
    uint64_t seqno;

    uint16_t source_shard_id;
    uint16_t reserved0;
    uint32_t source_wal_segment_id;
    uint64_t source_wal_offset;

    uint8_t record_type;
    uint8_t value_kind;
    uint8_t durability;
    uint8_t reserved1;

    const uint8_t *key_ptr;
    size_t key_len;

    const uint8_t *value_ptr;
    size_t value_len;
} CeMergedStreamItem;

typedef struct {
    CeMergedStreamItem *items;
    size_t item_count;

    CeCursor *next_cursors;
    size_t next_cursor_count;

    void *internal;
} CeMergedReadResult;

typedef struct {
    uint32_t version;
    uint16_t shard_count;
    uint16_t reserved0;
    uint8_t kind;
    uint8_t reserved1[7];
    uint64_t created_at_unix_ns;
    uint64_t high_seqno;
    CeCursor *cursors;
    size_t cursor_count;
    void *internal;
} CeCheckpoint;

enum {
    CE_OK = 0,
    CE_NULL_ARG = 1,
    CE_INIT_FAILED = 2,
    CE_INVALID_SHARD = 3,
    CE_NOT_FOUND = 4,
    CE_OPERATION_FAILED = 5,
};

CeEngine *ce_engine_open_default(void);
CeEngine *ce_engine_open(uint16_t shard_count, uint32_t inline_max);
void ce_engine_close(CeEngine *engine);

int ce_put(CeEngine *engine, const uint8_t *key_ptr, size_t key_len, const uint8_t *value_ptr, size_t value_len, uint8_t await_durable, CeCommitWindow *out_window);
int ce_delete(CeEngine *engine, const uint8_t *key_ptr, size_t key_len, uint8_t await_durable, CeCommitWindow *out_window);
int ce_get(CeEngine *engine, const uint8_t *key_ptr, size_t key_len, CeBuffer *out_buf);
int ce_get_at(CeEngine *engine, CeCursor cursor, const uint8_t *key_ptr, size_t key_len, CeBuffer *out_buf);

int ce_read_from(CeEngine *engine, CeCursor start, size_t limit, CeReadResult *out_result);
int ce_read_from_consistent(CeEngine *engine, CeCursor start, size_t limit, CeReadConsistency consistency, CeReadResult *out_result);
void ce_read_result_free(CeReadResult *result);

int ce_read_from_all_merged(CeEngine *engine, const CeCursor *start_cursors, size_t start_cursor_count, size_t limit, CeMergedReadResult *out_result);
int ce_read_from_all_merged_consistent(CeEngine *engine, const CeCursor *start_cursors, size_t start_cursor_count, size_t limit, CeReadConsistency consistency, CeMergedReadResult *out_result);
void ce_merged_read_result_free(CeMergedReadResult *result);

int ce_capture_checkpoint(CeEngine *engine, CeCheckpointKind kind, CeCheckpoint *out_checkpoint);
int ce_checkpoint_write_file(const CeCheckpoint *checkpoint, const char *path);
int ce_checkpoint_read_file(const char *path, CeCheckpoint *out_checkpoint);
int ce_checkpoint_to_merged_cursors(const CeCheckpoint *checkpoint, CeCursor **out_cursors, size_t *out_count);
void ce_checkpoint_cursor_array_free(CeCursor *ptr, size_t count);
void ce_checkpoint_free(CeCheckpoint *checkpoint);

void ce_buffer_free(CeBuffer *buf);
const char *ce_status_string(int status);

#ifdef __cplusplus
}
#endif

#endif
