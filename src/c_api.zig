const std = @import("std");
const hybrid = @import("lib.zig");

const c_allocator = std.heap.c_allocator;

pub const CeEngine = opaque {};

pub const CeCursor = extern struct {
    shard_id: u16,
    wal_segment_id: u32,
    wal_offset: u64,
};

pub const CeCommitWindow = extern struct {
    shard_id: u16,
    start: CeCursor,
    end: CeCursor,
    visible: CeCursor,
    durable: CeCursor,
    has_durable: u8,
    record_count: usize,
    first_seqno: u64,
    last_seqno: u64,
};

pub const CeBuffer = extern struct {
    ptr: ?*u8,
    len: usize,
};

pub const CeReadConsistency = enum(c_int) {
    visible = 0,
    durable = 1,
};

pub const CeCheckpointKind = enum(c_int) {
    visible = 0,
    durable = 1,
};

pub const CeCheckpoint = extern struct {
    version: u32,
    shard_count: u16,
    reserved0: u16,
    kind: u8,
    reserved1: [7]u8,
    created_at_unix_ns: u64,
    high_seqno: u64,
    cursors: ?[*]CeCursor,
    cursor_count: usize,
    internal: ?*anyopaque,
};

const CeOwnedCheckpoint = struct {
    cursors: []CeCursor,
};

pub const CeRecordType = enum(u8) {
    put_inline = 0,
    put_blob = 1,
    tombstone = 2,
};

pub const CeValueKind = enum(u8) {
    @"inline" = 0,
    blob = 1,
    tombstone = 2,
};

pub const CeDurability = enum(u8) {
    ultrafast = 0,
    batch = 1,
    strict = 2,
};

pub const CeStreamItem = extern struct {
    seqno: u64,
    cursor_shard_id: u16,
    reserved0: u16,
    cursor_wal_segment_id: u32,
    cursor_wal_offset: u64,
    record_type: u8,
    value_kind: u8,
    durability: u8,
    reserved1: u8,
    key_ptr: ?[*]const u8,
    key_len: usize,
    value_ptr: ?[*]const u8,
    value_len: usize,
};

pub const CeReadResult = extern struct {
    items: ?[*]CeStreamItem,
    item_count: usize,
    next_shard_id: u16,
    reserved0: u16,
    next_wal_segment_id: u32,
    next_wal_offset: u64,
    internal: ?*anyopaque,
};

const CeOwnedReadResult = struct {
    items: []CeStreamItem,
};

pub const CeMergedStreamItem = extern struct {
    seqno: u64,
    source_shard_id: u16,
    reserved0: u16,
    source_wal_segment_id: u32,
    source_wal_offset: u64,
    record_type: u8,
    value_kind: u8,
    durability: u8,
    reserved1: u8,
    key_ptr: ?[*]const u8,
    key_len: usize,
    value_ptr: ?[*]const u8,
    value_len: usize,
};

pub const CeMergedReadResult = extern struct {
    items: ?[*]CeMergedStreamItem,
    item_count: usize,
    next_cursors: ?[*]CeCursor,
    next_cursor_count: usize,
    internal: ?*anyopaque,
};

const CeOwnedMergedReadResult = struct {
    items: []CeMergedStreamItem,
    next_cursors: []CeCursor,
};

pub const CeStatus = enum(c_int) {
    ok = 0,
    null_arg = 1,
    init_failed = 2,
    invalid_shard = 3,
    not_found = 4,
    operation_failed = 5,
};

fn unwrapEngine(handle: ?*CeEngine) ?*hybrid.Engine {
    const raw = handle orelse return null;
    return @ptrCast(@alignCast(raw));
}

fn toCursor(shard_id: u16, cursor: hybrid.ShardCursor) CeCursor {
    return .{
        .shard_id = shard_id,
        .wal_segment_id = cursor.wal_segment_id,
        .wal_offset = cursor.wal_offset,
    };
}

fn toEngineCursor(cursor: CeCursor) hybrid.types.Cursor {
    return .{
        .shard_id = cursor.shard_id,
        .wal_segment_id = cursor.wal_segment_id,
        .wal_offset = cursor.wal_offset,
    };
}

fn fillWindow(out: *CeCommitWindow, window: hybrid.CommitWindow) void {
    out.* = .{
        .shard_id = window.shard_id,
        .start = toCursor(window.shard_id, window.start),
        .end = toCursor(window.shard_id, window.end),
        .visible = toCursor(window.shard_id, window.visible),
        .durable = if (window.durable) |d|
            toCursor(window.shard_id, d)
        else
            .{
                .shard_id = window.shard_id,
                .wal_segment_id = 0,
                .wal_offset = 0,
            },
        .has_durable = if (window.durable != null) 1 else 0,
        .record_count = window.record_count,
        .first_seqno = window.first_seqno,
        .last_seqno = window.last_seqno,
    };
}

fn statusFromError(err: anyerror) CeStatus {
    return switch (err) {
        error.InvalidShardId => .invalid_shard,
        else => .operation_failed,
    };
}

fn mapConsistency(consistency: CeReadConsistency) hybrid.types.ReadConsistency {
    return switch (consistency) {
        .visible => .visible,
        .durable => .durable,
    };
}

fn mapRecordType(record_type: hybrid.types.RecordType) u8 {
    return switch (record_type) {
        .put_inline => @intFromEnum(CeRecordType.put_inline),
        .put_blob => @intFromEnum(CeRecordType.put_blob),
        .tombstone => @intFromEnum(CeRecordType.tombstone),
    };
}

fn mapDurability(durability: hybrid.types.Durability) u8 {
    return switch (durability) {
        .ultrafast => @intFromEnum(CeDurability.ultrafast),
        .batch => @intFromEnum(CeDurability.batch),
        .strict => @intFromEnum(CeDurability.strict),
    };
}

fn mapCheckpointKind(kind: CeCheckpointKind) hybrid.types.CheckpointKind {
    return switch (kind) {
        .visible => .visible,
        .durable => .durable,
    };
}

fn zeroCheckpoint(out: *CeCheckpoint) void {
    out.* = .{
        .version = 0,
        .shard_count = 0,
        .reserved0 = 0,
        .kind = 0,
        .reserved1 = [_]u8{0} ** 7,
        .created_at_unix_ns = 0,
        .high_seqno = 0,
        .cursors = null,
        .cursor_count = 0,
        .internal = null,
    };
}

fn fillCheckpoint(out: *CeCheckpoint, checkpoint: hybrid.types.EngineCheckpoint) !void {
    zeroCheckpoint(out);

    const owner = try c_allocator.create(CeOwnedCheckpoint);
    errdefer c_allocator.destroy(owner);
    owner.cursors = try c_allocator.alloc(CeCursor, checkpoint.shard_cursors.len);
    errdefer c_allocator.free(owner.cursors);

    for (checkpoint.shard_cursors, 0..) |cursor, i| {
        owner.cursors[i] = .{
            .shard_id = @intCast(i),
            .wal_segment_id = cursor.wal_segment_id,
            .wal_offset = cursor.wal_offset,
        };
    }

    out.* = .{
        .version = checkpoint.version,
        .shard_count = checkpoint.shard_count,
        .reserved0 = checkpoint.reserved0,
        .kind = @intFromEnum(checkpoint.kind),
        .reserved1 = checkpoint.reserved1,
        .created_at_unix_ns = checkpoint.created_at_unix_ns,
        .high_seqno = checkpoint.high_seqno,
        .cursors = if (owner.cursors.len == 0) null else owner.cursors.ptr,
        .cursor_count = owner.cursors.len,
        .internal = owner,
    };
}

fn fillReadResult(out: *CeReadResult, result: hybrid.types.ReadFromResult) !void {
    out.* = .{
        .items = null,
        .item_count = 0,
        .next_shard_id = result.next_cursor.shard_id,
        .reserved0 = 0,
        .next_wal_segment_id = result.next_cursor.wal_segment_id,
        .next_wal_offset = result.next_cursor.wal_offset,
        .internal = null,
    };

    const owner = try c_allocator.create(CeOwnedReadResult);
    errdefer c_allocator.destroy(owner);

    const c_items = try c_allocator.alloc(CeStreamItem, result.items.len);
    errdefer c_allocator.free(c_items);

    for (c_items) |*slot| {
        slot.* = .{
            .seqno = 0,
            .cursor_shard_id = 0,
            .reserved0 = 0,
            .cursor_wal_segment_id = 0,
            .cursor_wal_offset = 0,
            .record_type = 0,
            .value_kind = 0,
            .durability = 0,
            .reserved1 = 0,
            .key_ptr = null,
            .key_len = 0,
            .value_ptr = null,
            .value_len = 0,
        };
    }

    errdefer {
        for (c_items) |item| {
            if (item.key_ptr) |key_ptr| c_allocator.free(@constCast(key_ptr)[0..item.key_len]);
            if (item.value_ptr) |value_ptr| c_allocator.free(@constCast(value_ptr)[0..item.value_len]);
        }
    }

    for (result.items, 0..) |item, i| {
        const key_copy = try c_allocator.dupe(u8, item.key);
        var value_ptr: ?[*]const u8 = null;
        var value_len: usize = 0;
        var value_kind: u8 = undefined;

        switch (item.value) {
            .@"inline" => |v| {
                const value_copy = try c_allocator.dupe(u8, v);
                value_ptr = value_copy.ptr;
                value_len = value_copy.len;
                value_kind = @intFromEnum(CeValueKind.@"inline");
            },
            .blob => |v| {
                const value_copy = try c_allocator.dupe(u8, v);
                value_ptr = value_copy.ptr;
                value_len = value_copy.len;
                value_kind = @intFromEnum(CeValueKind.blob);
            },
            .tombstone => {
                value_ptr = null;
                value_len = 0;
                value_kind = @intFromEnum(CeValueKind.tombstone);
            },
        }

        c_items[i] = .{
            .seqno = item.seqno,
            .cursor_shard_id = item.cursor.shard_id,
            .reserved0 = 0,
            .cursor_wal_segment_id = item.cursor.wal_segment_id,
            .cursor_wal_offset = item.cursor.wal_offset,
            .record_type = mapRecordType(item.record_type),
            .value_kind = value_kind,
            .durability = mapDurability(item.durability),
            .reserved1 = 0,
            .key_ptr = key_copy.ptr,
            .key_len = key_copy.len,
            .value_ptr = value_ptr,
            .value_len = value_len,
        };
    }

    owner.items = c_items;
    out.* = .{
        .items = if (c_items.len == 0) null else c_items.ptr,
        .item_count = c_items.len,
        .next_shard_id = result.next_cursor.shard_id,
        .reserved0 = 0,
        .next_wal_segment_id = result.next_cursor.wal_segment_id,
        .next_wal_offset = result.next_cursor.wal_offset,
        .internal = owner,
    };
}

fn fillMergedReadResult(out: *CeMergedReadResult, result: hybrid.types.GlobalMergeReadFromResult) !void {
    out.* = .{
        .items = null,
        .item_count = 0,
        .next_cursors = null,
        .next_cursor_count = 0,
        .internal = null,
    };

    const owner = try c_allocator.create(CeOwnedMergedReadResult);
    errdefer c_allocator.destroy(owner);

    const c_items = try c_allocator.alloc(CeMergedStreamItem, result.items.len);
    errdefer c_allocator.free(c_items);

    const c_next = try c_allocator.alloc(CeCursor, result.next_cursor.shard_cursors.len);
    errdefer c_allocator.free(c_next);

    for (result.next_cursor.shard_cursors, 0..) |cursor, i| {
        c_next[i] = .{
            .shard_id = @intCast(i),
            .wal_segment_id = cursor.wal_segment_id,
            .wal_offset = cursor.wal_offset,
        };
    }

    for (c_items) |*slot| {
        slot.* = .{
            .seqno = 0,
            .source_shard_id = 0,
            .reserved0 = 0,
            .source_wal_segment_id = 0,
            .source_wal_offset = 0,
            .record_type = 0,
            .value_kind = 0,
            .durability = 0,
            .reserved1 = 0,
            .key_ptr = null,
            .key_len = 0,
            .value_ptr = null,
            .value_len = 0,
        };
    }

    errdefer {
        for (c_items) |item| {
            if (item.key_ptr) |p| c_allocator.free(@constCast(p)[0..item.key_len]);
            if (item.value_ptr) |p| c_allocator.free(@constCast(p)[0..item.value_len]);
        }
    }

    for (result.items, 0..) |item, i| {
        const key_copy = try c_allocator.dupe(u8, item.key);
        var value_ptr: ?[*]const u8 = null;
        var value_len: usize = 0;
        var value_kind: u8 = undefined;

        switch (item.value) {
            .@"inline" => |v| {
                const copy = try c_allocator.dupe(u8, v);
                value_ptr = copy.ptr;
                value_len = copy.len;
                value_kind = @intFromEnum(CeValueKind.@"inline");
            },
            .blob => |v| {
                const copy = try c_allocator.dupe(u8, v);
                value_ptr = copy.ptr;
                value_len = copy.len;
                value_kind = @intFromEnum(CeValueKind.blob);
            },
            .tombstone => {
                value_ptr = null;
                value_len = 0;
                value_kind = @intFromEnum(CeValueKind.tombstone);
            },
        }

        c_items[i] = .{
            .seqno = item.seqno,
            .source_shard_id = item.cursor.shard_id,
            .reserved0 = 0,
            .source_wal_segment_id = item.cursor.wal_segment_id,
            .source_wal_offset = item.cursor.wal_offset,
            .record_type = mapRecordType(item.record_type),
            .value_kind = value_kind,
            .durability = mapDurability(item.durability),
            .reserved1 = 0,
            .key_ptr = key_copy.ptr,
            .key_len = key_copy.len,
            .value_ptr = value_ptr,
            .value_len = value_len,
        };
    }

    owner.items = c_items;
    owner.next_cursors = c_next;

    out.* = .{
        .items = if (c_items.len == 0) null else c_items.ptr,
        .item_count = c_items.len,
        .next_cursors = if (c_next.len == 0) null else c_next.ptr,
        .next_cursor_count = c_next.len,
        .internal = owner,
    };
}

export fn ce_capture_checkpoint(
    handle: ?*CeEngine,
    kind: CeCheckpointKind,
    out_checkpoint: ?*CeCheckpoint,
) c_int {
    const engine = unwrapEngine(handle) orelse return @intFromEnum(CeStatus.null_arg);
    const out = out_checkpoint orelse return @intFromEnum(CeStatus.null_arg);
    zeroCheckpoint(out);

    const checkpoint = engine.captureCheckpoint(c_allocator, mapCheckpointKind(kind)) catch {
        return @intFromEnum(CeStatus.operation_failed);
    };
    defer hybrid.Engine.freeCheckpoint(c_allocator, checkpoint);

    fillCheckpoint(out, checkpoint) catch {
        ce_checkpoint_free(out);
        return @intFromEnum(CeStatus.operation_failed);
    };
    return @intFromEnum(CeStatus.ok);
}

export fn ce_checkpoint_write_file(
    checkpoint: ?*const CeCheckpoint,
    path: ?[*:0]const u8,
) c_int {
    const cp = checkpoint orelse return @intFromEnum(CeStatus.null_arg);
    const path_z = path orelse return @intFromEnum(CeStatus.null_arg);
    const src = cp.cursors orelse return @intFromEnum(CeStatus.null_arg);

    const shard_cursors = c_allocator.alloc(hybrid.types.ShardCursor, cp.cursor_count) catch return @intFromEnum(CeStatus.operation_failed);
    defer c_allocator.free(shard_cursors);
    for (src[0..cp.cursor_count], 0..) |cursor, i| {
        shard_cursors[i] = .{ .wal_segment_id = cursor.wal_segment_id, .wal_offset = cursor.wal_offset };
    }

    const kind = std.meta.intToEnum(hybrid.types.CheckpointKind, cp.kind) catch return @intFromEnum(CeStatus.operation_failed);
    const engine_checkpoint: hybrid.types.EngineCheckpoint = .{
        .version = cp.version,
        .shard_count = cp.shard_count,
        .reserved0 = cp.reserved0,
        .kind = kind,
        .reserved1 = cp.reserved1,
        .created_at_unix_ns = cp.created_at_unix_ns,
        .high_seqno = cp.high_seqno,
        .shard_cursors = shard_cursors,
    };

    hybrid.Engine.writeCheckpointFileFromValue(std.mem.span(path_z), engine_checkpoint) catch {
        return @intFromEnum(CeStatus.operation_failed);
    };
    return @intFromEnum(CeStatus.ok);
}

export fn ce_checkpoint_read_file(
    path: ?[*:0]const u8,
    out_checkpoint: ?*CeCheckpoint,
) c_int {
    const path_z = path orelse return @intFromEnum(CeStatus.null_arg);
    const out = out_checkpoint orelse return @intFromEnum(CeStatus.null_arg);
    zeroCheckpoint(out);

    const checkpoint = hybrid.Engine.readCheckpointFile(c_allocator, std.mem.span(path_z)) catch {
        return @intFromEnum(CeStatus.operation_failed);
    };
    defer hybrid.Engine.freeCheckpoint(c_allocator, checkpoint);

    fillCheckpoint(out, checkpoint) catch {
        ce_checkpoint_free(out);
        return @intFromEnum(CeStatus.operation_failed);
    };
    return @intFromEnum(CeStatus.ok);
}

export fn ce_checkpoint_to_merged_cursors(
    checkpoint: ?*const CeCheckpoint,
    out_cursors: ?*?[*]CeCursor,
    out_count: ?*usize,
) c_int {
    const cp = checkpoint orelse return @intFromEnum(CeStatus.null_arg);
    const cursors_out = out_cursors orelse return @intFromEnum(CeStatus.null_arg);
    const count_out = out_count orelse return @intFromEnum(CeStatus.null_arg);
    const src = cp.cursors orelse return @intFromEnum(CeStatus.null_arg);

    const duped = c_allocator.alloc(CeCursor, cp.cursor_count) catch return @intFromEnum(CeStatus.operation_failed);
    @memcpy(duped, src[0..cp.cursor_count]);
    cursors_out.* = if (duped.len == 0) null else duped.ptr;
    count_out.* = duped.len;
    return @intFromEnum(CeStatus.ok);
}

export fn ce_checkpoint_cursor_array_free(ptr: ?[*]CeCursor, count: usize) void {
    const many = ptr orelse return;
    c_allocator.free(many[0..count]);
}

export fn ce_checkpoint_free(checkpoint: ?*CeCheckpoint) void {
    const out = checkpoint orelse return;
    const internal = out.internal orelse {
        zeroCheckpoint(out);
        return;
    };

    const owner: *CeOwnedCheckpoint = @ptrCast(@alignCast(internal));
    c_allocator.free(owner.cursors);
    c_allocator.destroy(owner);
    zeroCheckpoint(out);
}

export fn ce_engine_open_default() ?*CeEngine {
    const engine_ptr = c_allocator.create(hybrid.Engine) catch return null;
    engine_ptr.* = hybrid.Engine.init(c_allocator, .{}) catch {
        c_allocator.destroy(engine_ptr);
        return null;
    };
    return @ptrCast(engine_ptr);
}

export fn ce_engine_open(shard_count: u16, inline_max: u32) ?*CeEngine {
    const engine_ptr = c_allocator.create(hybrid.Engine) catch return null;
    engine_ptr.* = hybrid.Engine.init(c_allocator, .{
        .shard_count = shard_count,
        .inline_max = inline_max,
    }) catch {
        c_allocator.destroy(engine_ptr);
        return null;
    };
    return @ptrCast(engine_ptr);
}

export fn ce_engine_close(handle: ?*CeEngine) void {
    const engine = unwrapEngine(handle) orelse return;
    engine.deinit();
    c_allocator.destroy(engine);
}

export fn ce_put(
    handle: ?*CeEngine,
    key_ptr: ?[*]const u8,
    key_len: usize,
    value_ptr: ?[*]const u8,
    value_len: usize,
    await_durable: u8,
    out_window: ?*CeCommitWindow,
) c_int {
    const engine = unwrapEngine(handle) orelse return @intFromEnum(CeStatus.null_arg);
    const key_many = key_ptr orelse return @intFromEnum(CeStatus.null_arg);
    const value_many = value_ptr orelse return @intFromEnum(CeStatus.null_arg);
    const out = out_window orelse return @intFromEnum(CeStatus.null_arg);

    const key = key_many[0..key_len];
    const value = value_many[0..value_len];
    const shard_id = engine.shardIdForKey(key);
    const ops = [_]hybrid.types.BatchOp{
        .{ .put = .{ .key = key, .value = value } },
    };

    const result = engine.appendBatchAndCommit(shard_id, &ops, .{
        .durability = if (await_durable != 0) .strict else .batch,
        .await_durable = await_durable != 0,
    }) catch |err| return @intFromEnum(statusFromError(err));

    fillWindow(out, result.window);
    return @intFromEnum(CeStatus.ok);
}

export fn ce_delete(
    handle: ?*CeEngine,
    key_ptr: ?[*]const u8,
    key_len: usize,
    await_durable: u8,
    out_window: ?*CeCommitWindow,
) c_int {
    const engine = unwrapEngine(handle) orelse return @intFromEnum(CeStatus.null_arg);
    const key_many = key_ptr orelse return @intFromEnum(CeStatus.null_arg);
    const out = out_window orelse return @intFromEnum(CeStatus.null_arg);

    const key = key_many[0..key_len];
    const shard_id = engine.shardIdForKey(key);
    const ops = [_]hybrid.types.BatchOp{
        .{ .del = .{ .key = key } },
    };

    const result = engine.appendBatchAndCommit(shard_id, &ops, .{
        .durability = if (await_durable != 0) .strict else .batch,
        .await_durable = await_durable != 0,
    }) catch |err| return @intFromEnum(statusFromError(err));

    fillWindow(out, result.window);
    return @intFromEnum(CeStatus.ok);
}

export fn ce_read_from_all_merged(
    handle: ?*CeEngine,
    start_cursors: ?[*]const CeCursor,
    start_cursor_count: usize,
    limit: usize,
    out_result: ?*CeMergedReadResult,
) c_int {
    return ce_read_from_all_merged_consistent(
        handle,
        start_cursors,
        start_cursor_count,
        limit,
        .visible,
        out_result,
    );
}

export fn ce_read_from_all_merged_consistent(
    handle: ?*CeEngine,
    start_cursors: ?[*]const CeCursor,
    start_cursor_count: usize,
    limit: usize,
    consistency: CeReadConsistency,
    out_result: ?*CeMergedReadResult,
) c_int {
    const engine = unwrapEngine(handle) orelse return @intFromEnum(CeStatus.null_arg);
    const cursors_many = start_cursors orelse return @intFromEnum(CeStatus.null_arg);
    const out = out_result orelse return @intFromEnum(CeStatus.null_arg);

    out.* = .{
        .items = null,
        .item_count = 0,
        .next_cursors = null,
        .next_cursor_count = 0,
        .internal = null,
    };

    if (start_cursor_count != engine.shards.len) {
        return @intFromEnum(CeStatus.operation_failed);
    }

    const in_slice = cursors_many[0..start_cursor_count];
    const shard_cursors = c_allocator.alloc(hybrid.ShardCursor, in_slice.len) catch {
        return @intFromEnum(CeStatus.operation_failed);
    };
    defer c_allocator.free(shard_cursors);

    for (in_slice, 0..) |cursor, i| {
        if (cursor.shard_id != i) {
            return @intFromEnum(CeStatus.operation_failed);
        }
        shard_cursors[i] = .{
            .wal_segment_id = cursor.wal_segment_id,
            .wal_offset = cursor.wal_offset,
        };
    }

    const result = engine.readFromAllMergedConsistent(
        c_allocator,
        .{ .shard_cursors = shard_cursors },
        limit,
        mapConsistency(consistency),
    ) catch {
        return @intFromEnum(CeStatus.operation_failed);
    };
    defer hybrid.Engine.freeGlobalMergeReadFromResult(c_allocator, result);

    fillMergedReadResult(out, result) catch {
        ce_merged_read_result_free(out);
        return @intFromEnum(CeStatus.operation_failed);
    };

    return @intFromEnum(CeStatus.ok);
}

export fn ce_merged_read_result_free(result: ?*CeMergedReadResult) void {
    const out = result orelse return;
    const internal = out.internal orelse {
        out.* = .{
            .items = null,
            .item_count = 0,
            .next_cursors = null,
            .next_cursor_count = 0,
            .internal = null,
        };
        return;
    };

    const owner: *CeOwnedMergedReadResult = @ptrCast(@alignCast(internal));
    for (owner.items) |item| {
        if (item.key_ptr) |p| c_allocator.free(@constCast(p)[0..item.key_len]);
        if (item.value_ptr) |p| c_allocator.free(@constCast(p)[0..item.value_len]);
    }

    c_allocator.free(owner.items);
    c_allocator.free(owner.next_cursors);
    c_allocator.destroy(owner);

    out.* = .{
        .items = null,
        .item_count = 0,
        .next_cursors = null,
        .next_cursor_count = 0,
        .internal = null,
    };
}

export fn ce_get(
    handle: ?*CeEngine,
    key_ptr: ?[*]const u8,
    key_len: usize,
    out_buf: ?*CeBuffer,
) c_int {
    const engine = unwrapEngine(handle) orelse return @intFromEnum(CeStatus.null_arg);
    const key_many = key_ptr orelse return @intFromEnum(CeStatus.null_arg);
    const out = out_buf orelse return @intFromEnum(CeStatus.null_arg);

    out.* = .{ .ptr = null, .len = 0 };
    const key = key_many[0..key_len];
    const shard_id = engine.shardIdForKey(key);

    const value = engine.shards[shard_id].readValue(c_allocator, key) catch |err| {
        return @intFromEnum(statusFromError(err));
    };

    if (value == null) return @intFromEnum(CeStatus.not_found);
    out.* = .{
        .ptr = if (value.?.len == 0) null else @ptrCast(value.?.ptr),
        .len = value.?.len,
    };
    return @intFromEnum(CeStatus.ok);
}

export fn ce_get_at(
    handle: ?*CeEngine,
    cursor: CeCursor,
    key_ptr: ?[*]const u8,
    key_len: usize,
    out_buf: ?*CeBuffer,
) c_int {
    const engine = unwrapEngine(handle) orelse return @intFromEnum(CeStatus.null_arg);
    const key_many = key_ptr orelse return @intFromEnum(CeStatus.null_arg);
    const out = out_buf orelse return @intFromEnum(CeStatus.null_arg);

    out.* = .{ .ptr = null, .len = 0 };

    const value = engine.getAt(c_allocator, .{
        .shard_id = cursor.shard_id,
        .wal_segment_id = cursor.wal_segment_id,
        .wal_offset = cursor.wal_offset,
    }, key_many[0..key_len]) catch |err| {
        return @intFromEnum(statusFromError(err));
    };

    if (value == null) return @intFromEnum(CeStatus.not_found);
    out.* = .{
        .ptr = if (value.?.len == 0) null else @ptrCast(value.?.ptr),
        .len = value.?.len,
    };
    return @intFromEnum(CeStatus.ok);
}

export fn ce_read_from(
    handle: ?*CeEngine,
    start: CeCursor,
    limit: usize,
    out_result: ?*CeReadResult,
) c_int {
    return ce_read_from_consistent(handle, start, limit, .visible, out_result);
}

export fn ce_read_from_consistent(
    handle: ?*CeEngine,
    start: CeCursor,
    limit: usize,
    consistency: CeReadConsistency,
    out_result: ?*CeReadResult,
) c_int {
    const engine = unwrapEngine(handle) orelse return @intFromEnum(CeStatus.null_arg);
    const out = out_result orelse return @intFromEnum(CeStatus.null_arg);

    out.* = .{
        .items = null,
        .item_count = 0,
        .next_shard_id = start.shard_id,
        .reserved0 = 0,
        .next_wal_segment_id = start.wal_segment_id,
        .next_wal_offset = start.wal_offset,
        .internal = null,
    };

    if (start.shard_id >= engine.shards.len) {
        return @intFromEnum(CeStatus.invalid_shard);
    }

    const result = engine.readFromConsistent(
        c_allocator,
        toEngineCursor(start),
        limit,
        mapConsistency(consistency),
    ) catch |err| {
        return @intFromEnum(statusFromError(err));
    };
    defer hybrid.Shard.freeReadFromResult(c_allocator, result);

    fillReadResult(out, result) catch {
        ce_read_result_free(out);
        return @intFromEnum(CeStatus.operation_failed);
    };

    return @intFromEnum(CeStatus.ok);
}

export fn ce_read_result_free(result: ?*CeReadResult) void {
    const out = result orelse return;
    const internal = out.internal orelse {
        out.* = .{
            .items = null,
            .item_count = 0,
            .next_shard_id = 0,
            .reserved0 = 0,
            .next_wal_segment_id = 0,
            .next_wal_offset = 0,
            .internal = null,
        };
        return;
    };

    const owner: *CeOwnedReadResult = @ptrCast(@alignCast(internal));
    for (owner.items) |item| {
        if (item.key_ptr) |key_ptr| c_allocator.free(@constCast(key_ptr)[0..item.key_len]);
        if (item.value_ptr) |value_ptr| c_allocator.free(@constCast(value_ptr)[0..item.value_len]);
    }

    c_allocator.free(owner.items);
    c_allocator.destroy(owner);

    out.* = .{
        .items = null,
        .item_count = 0,
        .next_shard_id = 0,
        .reserved0 = 0,
        .next_wal_segment_id = 0,
        .next_wal_offset = 0,
        .internal = null,
    };
}

export fn ce_buffer_free(buf: ?*CeBuffer) void {
    const out = buf orelse return;
    const ptr = out.ptr orelse {
        out.* = .{ .ptr = null, .len = 0 };
        return;
    };
    const slice = @as([*]u8, @ptrCast(ptr))[0..out.len];
    c_allocator.free(slice);
    out.* = .{ .ptr = null, .len = 0 };
}

export fn ce_status_string(status: c_int) [*:0]const u8 {
    const s: CeStatus = @enumFromInt(status);
    return switch (s) {
        .ok => "ok",
        .null_arg => "null_arg",
        .init_failed => "init_failed",
        .invalid_shard => "invalid_shard",
        .not_found => "not_found",
        .operation_failed => "operation_failed",
    };
}
