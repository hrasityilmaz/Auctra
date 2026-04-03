const std = @import("std");
const hybrid = @import("hybrid_engine");

const types = hybrid.types;

fn cleanupTestFiles() void {
    var i: usize = 0;
    while (i < 8) : (i += 1) {
        var wal_name_buf: [32]u8 = undefined;
        var blob_name_buf: [32]u8 = undefined;

        const wal_name = std.fmt.bufPrint(&wal_name_buf, "wal-{d:0>3}.log", .{i}) catch unreachable;
        const blob_name = std.fmt.bufPrint(&blob_name_buf, "blob-{d:0>3}.log", .{i}) catch unreachable;

        std.fs.cwd().deleteFile(wal_name) catch {};
        std.fs.cwd().deleteFile(blob_name) catch {};
    }
}

test "put inline and get" {
    cleanupTestFiles();
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();

    const allocator = gpa.allocator();

    var engine = try hybrid.Engine.init(allocator, .{
        .shard_count = 2,
        .inline_max = 32,
    });
    defer engine.deinit();

    _ = try engine.put(.ultrafast, "alpha", "one");
    try engine.commitPending();

    const v = engine.get("alpha");
    try std.testing.expect(v != null);

    switch (v.?) {
        .@"inline" => |x| {
            try std.testing.expectEqual(@as(u64, 1), x.seqno);
        },
        else => return error.ExpectedInlineValue,
    }
}

test "put blob and get" {
    cleanupTestFiles();
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();

    const allocator = gpa.allocator();

    var engine = try hybrid.Engine.init(allocator, .{
        .shard_count = 2,
        .inline_max = 4,
    });
    defer engine.deinit();

    _ = try engine.put(.batch, "beta", "0123456789abcdef");
    try engine.commitPending();

    const v = engine.get("beta");
    try std.testing.expect(v != null);

    switch (v.?) {
        .blob => |x| {
            try std.testing.expect(x.blob.value_len > 4);
        },
        else => return error.ExpectedBlobValue,
    }
}

test "delete becomes tombstone" {
    cleanupTestFiles();
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();

    const allocator = gpa.allocator();

    var engine = try hybrid.Engine.init(allocator, .{});
    defer engine.deinit();

    _ = try engine.del(.strict, "gone");
    try engine.commitPending();

    const v = engine.get("gone");
    try std.testing.expect(v != null);

    switch (v.?) {
        .tombstone => |x| {
            try std.testing.expectEqual(@as(u64, 1), x.seqno);
        },
        else => return error.ExpectedTombstone,
    }
}

test "restart replays wal and restores index" {
    cleanupTestFiles();
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();

    const allocator = gpa.allocator();

    {
        var engine = try hybrid.Engine.init(allocator, .{
            .shard_count = 1,
            .inline_max = 16,
        });
        defer engine.deinit();

        _ = try engine.put(.ultrafast, "user:1", "hello");
        _ = try engine.put(.batch, "user:2", "this value is definitely larger than sixteen bytes");
        _ = try engine.del(.strict, "user:3");

        try engine.commitPending();
    }

    {
        var engine = try hybrid.Engine.init(allocator, .{
            .shard_count = 1,
            .inline_max = 16,
        });
        defer engine.deinit();

        const v1 = engine.get("user:1");
        try std.testing.expect(v1 != null);
        switch (v1.?) {
            .@"inline" => |x| {
                try std.testing.expectEqual(@as(u64, 1), x.seqno);
            },
            else => return error.ExpectedInlineAfterReplay,
        }

        const v2 = engine.get("user:2");
        try std.testing.expect(v2 != null);
        switch (v2.?) {
            .blob => |x| {
                try std.testing.expect(x.blob.value_len > 16);
                try std.testing.expectEqual(@as(u64, 2), x.seqno);
            },
            else => return error.ExpectedBlobAfterReplay,
        }

        const v3 = engine.get("user:3");
        try std.testing.expect(v3 != null);
        switch (v3.?) {
            .tombstone => |x| {
                try std.testing.expectEqual(@as(u64, 3), x.seqno);
            },
            else => return error.ExpectedTombstoneAfterReplay,
        }
    }
}

test "batch commit writes without immediate durable sync" {
    cleanupTestFiles();

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();

    const allocator = gpa.allocator();

    var engine = try hybrid.Engine.init(allocator, .{
        .shard_count = 1,
        .inline_max = 8,
    });
    defer engine.deinit();

    _ = try engine.put(.batch, "k1", "hello");
    try engine.commitPending();

    const shard = &engine.shards[0];

    try std.testing.expect(shard.wal_active.tail.written > 0);
    try std.testing.expectEqual(@as(u64, 0), shard.wal_active.tail.durable);

    try engine.syncBatch(1000);

    try std.testing.expectEqual(
        shard.wal_active.tail.written,
        shard.wal_active.tail.durable,
    );
}

test "strict commit forces durable sync" {
    cleanupTestFiles();

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();

    const allocator = gpa.allocator();

    var engine = try hybrid.Engine.init(allocator, .{
        .shard_count = 1,
        .inline_max = 8,
    });
    defer engine.deinit();

    _ = try engine.put(.strict, "k1", "hello");
    try engine.commitPending();

    const shard = &engine.shards[0];

    try std.testing.expect(shard.wal_active.tail.written > 0);
    try std.testing.expectEqual(
        shard.wal_active.tail.written,
        shard.wal_active.tail.durable,
    );
}

test "batch becomes durable on tick after interval" {
    cleanupTestFiles();

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();

    const allocator = gpa.allocator();

    var engine = try hybrid.Engine.init(allocator, .{
        .shard_count = 1,
        .inline_max = 8,
        .batch_sync_interval_ns = 100,
    });
    defer engine.deinit();

    _ = try engine.put(.batch, "k1", "hello");
    try engine.commitPending();

    const shard = &engine.shards[0];
    try std.testing.expect(shard.dirty);
    try std.testing.expectEqual(types.Durability.batch, shard.strongest_dirty_durability.?);
    try std.testing.expectEqual(@as(u64, 0), shard.wal_active.tail.durable);

    try engine.tick(shard.dirty_since_ns + 50);
    try std.testing.expectEqual(@as(u64, 0), shard.wal_active.tail.durable);

    try engine.tick(shard.dirty_since_ns + 100);
    try std.testing.expectEqual(
        shard.wal_active.tail.written,
        shard.wal_active.tail.durable,
    );
}

test "ultrafast alone does not sync on tick" {
    cleanupTestFiles();

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();

    const allocator = gpa.allocator();

    var engine = try hybrid.Engine.init(allocator, .{
        .shard_count = 1,
        .inline_max = 8,
        .batch_sync_interval_ns = 100,
    });
    defer engine.deinit();

    _ = try engine.put(.ultrafast, "k1", "hello");
    try engine.commitPending();

    const shard = &engine.shards[0];
    try std.testing.expect(shard.dirty);
    try std.testing.expectEqual(types.Durability.ultrafast, shard.strongest_dirty_durability.?);
    try std.testing.expectEqual(@as(u64, 0), shard.wal_active.tail.durable);

    try engine.tick(shard.dirty_since_ns + 1000);

    try std.testing.expect(shard.dirty);
    try std.testing.expectEqual(@as(u64, 0), shard.wal_active.tail.durable);
}

test "batch sync window starts from dirty_since_ns" {
    cleanupTestFiles();

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();

    const allocator = gpa.allocator();

    var engine = try hybrid.Engine.init(allocator, .{
        .shard_count = 1,
        .inline_max = 8,
        .batch_sync_interval_ns = 100,
    });
    defer engine.deinit();

    _ = try engine.put(.batch, "k1", "hello");
    try engine.commitPending();

    const shard = &engine.shards[0];
    try std.testing.expect(shard.dirty);
    try std.testing.expectEqual(types.Durability.batch, shard.strongest_dirty_durability.?);
    try std.testing.expectEqual(@as(u64, 0), shard.wal_active.tail.durable);

    try engine.tick(shard.dirty_since_ns + 50);
    try std.testing.expectEqual(@as(u64, 0), shard.wal_active.tail.durable);

    try engine.tick(shard.dirty_since_ns + 100);
    try std.testing.expectEqual(
        shard.wal_active.tail.written,
        shard.wal_active.tail.durable,
    );
}

test "batch sync triggers on dirty byte threshold" {
    cleanupTestFiles();

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();

    const allocator = gpa.allocator();

    var engine = try hybrid.Engine.init(allocator, .{
        .shard_count = 1,
        .inline_max = 8,
        .batch_sync_interval_ns = 1_000_000_000,
        .batch_sync_max_dirty_bytes = 1,
        .batch_sync_max_dirty_ops = 1024,
    });
    defer engine.deinit();

    _ = try engine.put(.batch, "k1", "hello");
    try engine.commitPending();

    const shard = &engine.shards[0];
    try std.testing.expect(shard.dirty);
    try std.testing.expect(shard.dirty_bytes > 0);
    try std.testing.expectEqual(@as(u64, 0), shard.wal_active.tail.durable);

    try engine.tick(shard.dirty_since_ns);

    try std.testing.expect(!shard.dirty);
    try std.testing.expectEqual(
        shard.wal_active.tail.written,
        shard.wal_active.tail.durable,
    );
}

test "batch sync triggers on dirty op threshold" {
    cleanupTestFiles();

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();

    const allocator = gpa.allocator();

    var engine = try hybrid.Engine.init(allocator, .{
        .shard_count = 1,
        .inline_max = 8,
        .batch_sync_interval_ns = 1_000_000_000,
        .batch_sync_max_dirty_bytes = 1_000_000,
        .batch_sync_max_dirty_ops = 1,
    });
    defer engine.deinit();

    _ = try engine.put(.batch, "k1", "hello");
    try engine.commitPending();

    const shard = &engine.shards[0];
    try std.testing.expect(shard.dirty);
    try std.testing.expectEqual(@as(u32, 1), shard.dirty_ops);
    try std.testing.expectEqual(@as(u64, 0), shard.wal_active.tail.durable);

    try engine.tick(shard.dirty_since_ns);

    try std.testing.expect(!shard.dirty);
    try std.testing.expectEqual(
        shard.wal_active.tail.written,
        shard.wal_active.tail.durable,
    );
}

test "background flusher makes batch writes durable" {
    cleanupTestFiles();

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();

    const allocator = gpa.allocator();

    var engine = try hybrid.Engine.init(allocator, .{
        .shard_count = 1,
        .inline_max = 8,
        .batch_sync_interval_ns = 100_000,
        .batch_sync_max_dirty_bytes = 1_000_000,
        .batch_sync_max_dirty_ops = 1024,
        .flusher_sleep_ns = 50_000,
    });
    defer engine.deinit();

    try engine.startFlusher();
    defer engine.stopFlusher();

    _ = try engine.put(.batch, "k1", "hello");
    try engine.commitPending();

    const shard = &engine.shards[0];
    try std.testing.expect(shard.dirty);

    std.Thread.sleep(500_000);

    engine.mutex.lock();
    const dirty = shard.dirty;
    const written = shard.wal_active.tail.written;
    const durable = shard.wal_active.tail.durable;
    engine.mutex.unlock();

    try std.testing.expect(!dirty);
    try std.testing.expectEqual(written, durable);
}

test "engine initializes with pwrite backend" {
    cleanupTestFiles();

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();

    const allocator = gpa.allocator();

    var engine = try hybrid.Engine.init(allocator, .{
        .shard_count = 1,
        .io_backend = .pwrite,
    });
    defer engine.deinit();

    _ = try engine.put(.ultrafast, "k1", "hello");
    try engine.commitPending();

    const v = try engine.readValue(allocator, "k1");
    try std.testing.expect(v != null);
    defer allocator.free(v.?);
    try std.testing.expectEqualStrings("hello", v.?);
}

test "engine initializes with io_uring backend and reads values on linux" {
    if (@import("builtin").os.tag != .linux) return;

    cleanupTestFiles();

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();

    const allocator = gpa.allocator();

    var engine = try hybrid.Engine.init(allocator, .{
        .shard_count = 1,
        .inline_max = 8,
        .io_backend = .io_uring,
        .io_uring_entries = 64,
    });
    defer engine.deinit();

    _ = try engine.put(.ultrafast, "k1", "hello");
    _ = try engine.put(.batch, "k2", "0123456789abcdef");
    try engine.commitPending();

    const v1 = try engine.readValue(allocator, "k1");
    try std.testing.expect(v1 != null);
    defer allocator.free(v1.?);
    try std.testing.expectEqualStrings("hello", v1.?);

    const v2 = try engine.readValue(allocator, "k2");
    try std.testing.expect(v2 != null);
    defer allocator.free(v2.?);
    try std.testing.expectEqualStrings("0123456789abcdef", v2.?);
}

test "engine compaction preserves latest state and survives restart" {
    std.fs.cwd().deleteFile("wal-000.log") catch {};
    std.fs.cwd().deleteFile("blob-000.log") catch {};

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    {
        var engine = try hybrid.Engine.init(allocator, .{});
        defer engine.deinit();

        _ = try engine.put(.strict, "k1", "hello");
        _ = try engine.put(.strict, "k2", "this value is definitely larger than sixteen bytes");
        _ = try engine.put(.strict, "k3", "small");

        try engine.commitPending();

        const before_k1 = try engine.readValue(allocator, "k1");
        defer if (before_k1) |v| allocator.free(v);
        try std.testing.expect(before_k1 != null);
        try std.testing.expectEqualStrings("hello", before_k1.?);

        const before_k2 = try engine.readValue(allocator, "k2");
        defer if (before_k2) |v| allocator.free(v);
        try std.testing.expect(before_k2 != null);
        try std.testing.expectEqualStrings(
            "this value is definitely larger than sixteen bytes",
            before_k2.?,
        );

        _ = try engine.del(.strict, "k1");
        _ = try engine.put(.strict, "k2", "this value is definitely larger than sixteen bytes v2");
        try engine.commitPending();

        try engine.compactAll();

        const after_compact_k1 = try engine.readValue(allocator, "k1");
        defer if (after_compact_k1) |v| allocator.free(v);
        try std.testing.expect(after_compact_k1 == null);

        const after_compact_k2 = try engine.readValue(allocator, "k2");
        defer if (after_compact_k2) |v| allocator.free(v);
        try std.testing.expect(after_compact_k2 != null);
        try std.testing.expectEqualStrings(
            "this value is definitely larger than sixteen bytes v2",
            after_compact_k2.?,
        );

        const after_compact_k3 = try engine.readValue(allocator, "k3");
        defer if (after_compact_k3) |v| allocator.free(v);
        try std.testing.expect(after_compact_k3 != null);
        try std.testing.expectEqualStrings("small", after_compact_k3.?);
    }

    {
        var reopened = try hybrid.Engine.init(allocator, .{});
        defer reopened.deinit();

        const after_restart_k1 = try reopened.readValue(allocator, "k1");
        defer if (after_restart_k1) |v| allocator.free(v);
        try std.testing.expect(after_restart_k1 == null);

        const after_restart_k2 = try reopened.readValue(allocator, "k2");
        defer if (after_restart_k2) |v| allocator.free(v);
        try std.testing.expect(after_restart_k2 != null);
        try std.testing.expectEqualStrings(
            "this value is definitely larger than sixteen bytes v2",
            after_restart_k2.?,
        );

        const after_restart_k3 = try reopened.readValue(allocator, "k3");
        defer if (after_restart_k3) |v| allocator.free(v);
        try std.testing.expect(after_restart_k3 != null);
        try std.testing.expectEqualStrings("small", after_restart_k3.?);
    }
}

test "compaction shrinks storage after overwrites and deletes" {
    cleanupTestFiles();

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const big_v1 = "this value is definitely larger than sixteen bytes - version 1";
    const big_v2 = "this value is definitely larger than sixteen bytes - version 2";
    const big_v3 = "this value is definitely larger than sixteen bytes - version 3";
    const big_v4 = "this value is definitely larger than sixteen bytes - version 4";

    var wal_before: u64 = 0;
    var blob_before: u64 = 0;
    var wal_after: u64 = 0;
    var blob_after: u64 = 0;

    {
        var engine = try hybrid.Engine.init(allocator, .{
            .shard_count = 1,
            .inline_max = 8,
        });
        defer engine.deinit();

        _ = try engine.put(.strict, "k1", "a");
        _ = try engine.put(.strict, "k2", big_v1);
        _ = try engine.put(.strict, "k3", "small");
        try engine.commitPending();

        _ = try engine.put(.strict, "k1", "b");
        _ = try engine.put(.strict, "k2", big_v2);
        _ = try engine.put(.strict, "k3", "small-2");
        try engine.commitPending();

        _ = try engine.put(.strict, "k1", "c");
        _ = try engine.put(.strict, "k2", big_v3);
        _ = try engine.del(.strict, "k3");
        try engine.commitPending();

        _ = try engine.put(.strict, "k2", big_v4);
        try engine.commitPending();

        const wal_stat_before = try std.fs.cwd().statFile("wal-000.log");
        const blob_stat_before = try std.fs.cwd().statFile("blob-000.log");
        wal_before = wal_stat_before.size;
        blob_before = blob_stat_before.size;

        try std.testing.expect(wal_before > 0);
        try std.testing.expect(blob_before > 0);

        try engine.compactAll();

        const wal_stat_after = try std.fs.cwd().statFile("wal-000.log");
        const blob_stat_after = try std.fs.cwd().statFile("blob-000.log");
        wal_after = wal_stat_after.size;
        blob_after = blob_stat_after.size;

        try std.testing.expect(wal_after > 0);
        try std.testing.expect(blob_after > 0);

        try std.testing.expect(wal_after < wal_before);
        try std.testing.expect(blob_after < blob_before);

        const k1 = try engine.readValue(allocator, "k1");
        defer if (k1) |v| allocator.free(v);
        try std.testing.expect(k1 != null);
        try std.testing.expectEqualStrings("c", k1.?);

        const k2 = try engine.readValue(allocator, "k2");
        defer if (k2) |v| allocator.free(v);
        try std.testing.expect(k2 != null);
        try std.testing.expectEqualStrings(big_v4, k2.?);

        const k3 = try engine.readValue(allocator, "k3");
        defer if (k3) |v| allocator.free(v);
        try std.testing.expect(k3 == null);
    }

    {
        var reopened = try hybrid.Engine.init(allocator, .{
            .shard_count = 1,
            .inline_max = 8,
        });
        defer reopened.deinit();

        const wal_stat_reopened = try std.fs.cwd().statFile("wal-000.log");
        const blob_stat_reopened = try std.fs.cwd().statFile("blob-000.log");

        try std.testing.expectEqual(wal_after, wal_stat_reopened.size);
        try std.testing.expectEqual(blob_after, blob_stat_reopened.size);

        const k1 = try reopened.readValue(allocator, "k1");
        defer if (k1) |v| allocator.free(v);
        try std.testing.expect(k1 != null);
        try std.testing.expectEqualStrings("c", k1.?);

        const k2 = try reopened.readValue(allocator, "k2");
        defer if (k2) |v| allocator.free(v);
        try std.testing.expect(k2 != null);
        try std.testing.expectEqualStrings(big_v4, k2.?);

        const k3 = try reopened.readValue(allocator, "k3");
        defer if (k3) |v| allocator.free(v);
        try std.testing.expect(k3 == null);
    }
}

test "read_from basic sequential" {
    cleanupTestFiles();

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var engine = try hybrid.Engine.init(allocator, .{
        .shard_count = 1,
        .inline_max = 8,
    });
    defer engine.deinit();

    _ = try engine.put(.strict, "k1", "hello");
    _ = try engine.put(.strict, "k2", "0123456789abcdef");
    _ = try engine.del(.strict, "k3");
    try engine.commitPending();

    const res = try engine.readFrom(
        allocator,
        engine.cursorForShard(0),
        10,
    );
    defer hybrid.Shard.freeReadFromResult(allocator, res);

    try std.testing.expectEqual(@as(usize, 3), res.items.len);

    try std.testing.expectEqualStrings("k1", res.items[0].key);
    switch (res.items[0].value) {
        .@"inline" => |v| try std.testing.expectEqualStrings("hello", v),
        else => return error.ExpectedInlineStreamValue,
    }

    try std.testing.expectEqualStrings("k2", res.items[1].key);
    switch (res.items[1].value) {
        .blob => |v| try std.testing.expectEqualStrings("0123456789abcdef", v),
        else => return error.ExpectedBlobStreamValue,
    }

    try std.testing.expectEqualStrings("k3", res.items[2].key);
    switch (res.items[2].value) {
        .tombstone => {},
        else => return error.ExpectedTombstoneStreamValue,
    }
}

test "read_from_all walks all shards deterministically" {
    cleanupTestFiles();

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var engine = try hybrid.Engine.init(allocator, .{
        .shard_count = 2,
        .inline_max = 8,
    });
    defer engine.deinit();

    var key0: ?[]const u8 = null;
    var key1: ?[]const u8 = null;

    const candidates = [_][]const u8{
        "k0", "k1", "k2",  "k3",  "k4",  "k5",  "k6",  "k7",
        "k8", "k9", "k10", "k11", "k12", "k13", "k14",
    };

    for (candidates) |k| {
        const shard = engine.shardForKey(k).id;
        if (shard == 0 and key0 == null) key0 = k;
        if (shard == 1 and key1 == null) key1 = k;
    }

    try std.testing.expect(key0 != null);
    try std.testing.expect(key1 != null);

    _ = try engine.put(.strict, key0.?, "one");
    _ = try engine.put(.strict, key1.?, "0123456789abcdef");
    _ = try engine.del(.strict, "gamma");
    try engine.commitPending();

    const res = try engine.readFromAll(allocator, .{
        .shard_id = 0,
        .wal_segment_id = 0,
        .wal_offset = 0,
    }, 10);
    defer hybrid.Engine.freeGlobalReadFromResult(allocator, res);

    try std.testing.expect(res.items.len >= 3);

    var saw_key0 = false;
    var saw_key1 = false;
    var saw_gamma = false;

    var saw_shard_0 = false;
    var saw_shard_1 = false;

    for (res.items) |item| {
        if (std.mem.eql(u8, item.key, key0.?)) saw_key0 = true;
        if (std.mem.eql(u8, item.key, key1.?)) saw_key1 = true;
        if (std.mem.eql(u8, item.key, "gamma")) saw_gamma = true;

        if (item.cursor.shard_id == 0) saw_shard_0 = true;
        if (item.cursor.shard_id == 1) saw_shard_1 = true;
    }

    try std.testing.expect(saw_key0);
    try std.testing.expect(saw_key1);
    try std.testing.expect(saw_gamma);
    try std.testing.expect(saw_shard_0);
    try std.testing.expect(saw_shard_1);
}

test "read_from_all_merged returns items in seqno order across shards" {
    cleanupTestFiles();

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var engine = try hybrid.Engine.init(allocator, .{
        .shard_count = 2,
        .inline_max = 8,
    });
    defer engine.deinit();

    var key0: ?[]const u8 = null;
    var key1: ?[]const u8 = null;

    const candidates = [_][]const u8{
        "k0", "k1", "k2",  "k3",  "k4",  "k5",  "k6",  "k7",
        "k8", "k9", "k10", "k11", "k12", "k13", "k14",
    };

    for (candidates) |k| {
        const shard = engine.shardForKey(k).id;
        if (shard == 0 and key0 == null) key0 = k;
        if (shard == 1 and key1 == null) key1 = k;
    }

    try std.testing.expect(key0 != null);
    try std.testing.expect(key1 != null);

    _ = try engine.put(.strict, key0.?, "one");
    try engine.commitPending();

    _ = try engine.put(.strict, key1.?, "two");
    try engine.commitPending();

    _ = try engine.del(.strict, "gamma");
    try engine.commitPending();

    const cursor = try engine.initGlobalMergeCursor(allocator);
    defer hybrid.Engine.freeGlobalMergeCursor(allocator, cursor);

    const res = try engine.readFromAllMerged(allocator, cursor, 10);
    defer hybrid.Engine.freeGlobalMergeReadFromResult(allocator, res);

    try std.testing.expect(res.items.len >= 3);

    var i: usize = 1;
    while (i < res.items.len) : (i += 1) {
        try std.testing.expect(res.items[i - 1].seqno <= res.items[i].seqno);
    }

    var saw_key0 = false;
    var saw_key1 = false;
    var saw_gamma = false;

    for (res.items) |item| {
        if (std.mem.eql(u8, item.key, key0.?)) saw_key0 = true;
        if (std.mem.eql(u8, item.key, key1.?)) saw_key1 = true;
        if (std.mem.eql(u8, item.key, "gamma")) saw_gamma = true;
    }

    try std.testing.expect(saw_key0);
    try std.testing.expect(saw_key1);
    try std.testing.expect(saw_gamma);
}

test "read_from_all_merged paginates without duplicates" {
    cleanupTestFiles();

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var engine = try hybrid.Engine.init(allocator, .{
        .shard_count = 2,
        .inline_max = 8,
    });
    defer engine.deinit();

    var key0: ?[]const u8 = null;
    var key1: ?[]const u8 = null;

    const candidates = [_][]const u8{
        "k0", "k1", "k2",  "k3",  "k4",  "k5",  "k6",  "k7",
        "k8", "k9", "k10", "k11", "k12", "k13", "k14",
    };

    for (candidates) |k| {
        const shard = engine.shardForKey(k).id;
        if (shard == 0 and key0 == null) key0 = k;
        if (shard == 1 and key1 == null) key1 = k;
    }

    try std.testing.expect(key0 != null);
    try std.testing.expect(key1 != null);

    _ = try engine.put(.strict, key0.?, "one");
    try engine.commitPending();

    _ = try engine.put(.strict, key1.?, "two");
    try engine.commitPending();

    _ = try engine.del(.strict, "gamma");
    try engine.commitPending();

    const cursor0 = try engine.initGlobalMergeCursor(allocator);
    defer hybrid.Engine.freeGlobalMergeCursor(allocator, cursor0);

    const page1 = try engine.readFromAllMerged(allocator, cursor0, 1);
    const page2 = try engine.readFromAllMerged(allocator, page1.next_cursor, 1);
    const page3 = try engine.readFromAllMerged(allocator, page2.next_cursor, 1);
    const page4 = try engine.readFromAllMerged(allocator, page3.next_cursor, 1);
    const page5 = try engine.readFromAllMerged(allocator, page4.next_cursor, 1);

    defer hybrid.Engine.freeGlobalMergeReadFromResult(allocator, page5);
    defer hybrid.Engine.freeGlobalMergeReadFromResult(allocator, page4);
    defer hybrid.Engine.freeGlobalMergeReadFromResult(allocator, page3);
    defer hybrid.Engine.freeGlobalMergeReadFromResult(allocator, page2);
    defer hybrid.Engine.freeGlobalMergeReadFromResult(allocator, page1);

    try std.testing.expectEqual(@as(usize, 1), page1.items.len);
    try std.testing.expectEqual(@as(usize, 1), page2.items.len);
    try std.testing.expectEqual(@as(usize, 1), page3.items.len);
    try std.testing.expectEqual(@as(usize, 0), page4.items.len);
    try std.testing.expectEqual(@as(usize, 0), page5.items.len);

    try std.testing.expectEqual(page4.next_cursor.shard_cursors.len, page5.next_cursor.shard_cursors.len);

    var shard_idx: usize = 0;
    while (shard_idx < page4.next_cursor.shard_cursors.len) : (shard_idx += 1) {
        try std.testing.expectEqual(
            page4.next_cursor.shard_cursors[shard_idx].wal_segment_id,
            page5.next_cursor.shard_cursors[shard_idx].wal_segment_id,
        );
        try std.testing.expectEqual(
            page4.next_cursor.shard_cursors[shard_idx].wal_offset,
            page5.next_cursor.shard_cursors[shard_idx].wal_offset,
        );
    }

    const merged = [_]hybrid.StreamItem{
        page1.items[0],
        page2.items[0],
        page3.items[0],
    };

    var i: usize = 1;
    while (i < merged.len) : (i += 1) {
        try std.testing.expect(merged[i - 1].seqno <= merged[i].seqno);
    }

    var saw_key0 = false;
    var saw_key1 = false;
    var saw_gamma = false;

    for (merged) |item| {
        if (std.mem.eql(u8, item.key, key0.?)) saw_key0 = true;
        if (std.mem.eql(u8, item.key, key1.?)) saw_key1 = true;
        if (std.mem.eql(u8, item.key, "gamma")) saw_gamma = true;
    }

    try std.testing.expect(saw_key0);
    try std.testing.expect(saw_key1);
    try std.testing.expect(saw_gamma);
}

test "read_from_all_merged with zero limit returns empty page and same cursor shape" {
    cleanupTestFiles();

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var engine = try hybrid.Engine.init(allocator, .{
        .shard_count = 2,
        .inline_max = 8,
    });
    defer engine.deinit();

    const cursor0 = try engine.initGlobalMergeCursor(allocator);
    defer hybrid.Engine.freeGlobalMergeCursor(allocator, cursor0);

    const res = try engine.readFromAllMerged(allocator, cursor0, 0);
    defer hybrid.Engine.freeGlobalMergeReadFromResult(allocator, res);

    try std.testing.expectEqual(@as(usize, 0), res.items.len);
    try std.testing.expectEqual(@as(usize, 2), res.next_cursor.shard_cursors.len);

    var i: usize = 0;
    while (i < res.next_cursor.shard_cursors.len) : (i += 1) {
        try std.testing.expectEqual(@as(u32, 0), res.next_cursor.shard_cursors[i].wal_segment_id);
        try std.testing.expectEqual(@as(u64, 0), res.next_cursor.shard_cursors[i].wal_offset);
    }
}

test "read_from_all_merged on empty engine returns empty and stable eof" {
    cleanupTestFiles();

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var engine = try hybrid.Engine.init(allocator, .{
        .shard_count = 2,
        .inline_max = 8,
    });
    defer engine.deinit();

    const cursor0 = try engine.initGlobalMergeCursor(allocator);
    defer hybrid.Engine.freeGlobalMergeCursor(allocator, cursor0);

    const page1 = try engine.readFromAllMerged(allocator, cursor0, 10);
    const page2 = try engine.readFromAllMerged(allocator, page1.next_cursor, 10);

    defer hybrid.Engine.freeGlobalMergeReadFromResult(allocator, page2);
    defer hybrid.Engine.freeGlobalMergeReadFromResult(allocator, page1);

    try std.testing.expectEqual(@as(usize, 0), page1.items.len);
    try std.testing.expectEqual(@as(usize, 0), page2.items.len);

    try std.testing.expectEqual(page1.next_cursor.shard_cursors.len, page2.next_cursor.shard_cursors.len);

    var i: usize = 0;
    while (i < page1.next_cursor.shard_cursors.len) : (i += 1) {
        try std.testing.expectEqual(
            page1.next_cursor.shard_cursors[i].wal_segment_id,
            page2.next_cursor.shard_cursors[i].wal_segment_id,
        );
        try std.testing.expectEqual(
            page1.next_cursor.shard_cursors[i].wal_offset,
            page2.next_cursor.shard_cursors[i].wal_offset,
        );
    }
}

test "read_all_merged returns all items in merged order" {
    cleanupTestFiles();

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var engine = try hybrid.Engine.init(allocator, .{
        .shard_count = 2,
        .inline_max = 8,
    });
    defer engine.deinit();

    var key0: ?[]const u8 = null;
    var key1: ?[]const u8 = null;

    const candidates = [_][]const u8{
        "k0", "k1", "k2",  "k3",  "k4",  "k5",  "k6",  "k7",
        "k8", "k9", "k10", "k11", "k12", "k13", "k14",
    };

    for (candidates) |k| {
        const shard = engine.shardForKey(k).id;
        if (shard == 0 and key0 == null) key0 = k;
        if (shard == 1 and key1 == null) key1 = k;
    }

    try std.testing.expect(key0 != null);
    try std.testing.expect(key1 != null);

    _ = try engine.put(.strict, key0.?, "one");
    try engine.commitPending();

    _ = try engine.put(.strict, key1.?, "two");
    try engine.commitPending();

    _ = try engine.del(.strict, "gamma");
    try engine.commitPending();

    const items = try engine.readAllMerged(allocator);
    defer hybrid.Engine.freeMergedItems(allocator, items);

    try std.testing.expect(items.len >= 3);

    var saw_key0 = false;
    var saw_key1 = false;
    var saw_gamma = false;

    var i: usize = 1;
    while (i < items.len) : (i += 1) {
        try std.testing.expect(items[i - 1].seqno <= items[i].seqno);
    }

    for (items) |item| {
        if (std.mem.eql(u8, item.key, key0.?)) saw_key0 = true;
        if (std.mem.eql(u8, item.key, key1.?)) saw_key1 = true;
        if (std.mem.eql(u8, item.key, "gamma")) saw_gamma = true;
    }

    try std.testing.expect(saw_key0);
    try std.testing.expect(saw_key1);
    try std.testing.expect(saw_gamma);
}

test "readValue resolves through state_index" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();

    const allocator = gpa.allocator();

    var shard = try hybrid.Shard.init(allocator, 0, .{
        .shard_count = 1,
        .inline_max = 16,
    });
    defer shard.deinit();

    _ = try shard.enqueuePut(.strict, "user:1", "hello");

    var backend = try hybrid.io_backend.Backend.init(.pwrite, 128);
    defer backend.deinit();

    try shard.commitPendingAsWritten(0, &backend);

    const value = try shard.readValue(allocator, "user:1");
    defer if (value) |v| allocator.free(v);

    try std.testing.expect(value != null);
    try std.testing.expectEqualStrings("hello", value.?);
}

test "engine API basic get/put/delete" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();

    const allocator = gpa.allocator();

    var engine = try hybrid.Engine.init(allocator, .{
        .shard_count = 1,
        .inline_max = 16,
    });
    defer engine.deinit();

    try engine.put(.strict, "user:1", "hello");
    try engine.commit();

    const v1 = try engine.readValue(allocator, "user:1");
    defer if (v1) |v| allocator.free(v);

    try std.testing.expectEqualStrings("hello", v1.?);

    try engine.del(.strict, "user:1");
    try engine.commit();

    const v2 = try engine.readValue(allocator, "user:1");
    try std.testing.expect(v2 == null);
}

test "engine appendBatch commit and syncUntilToken" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();

    const allocator = gpa.allocator();

    var engine = try hybrid.Engine.init(allocator, .{
        .shard_count = 1,
        .inline_max = 16,
    });
    defer engine.deinit();

    const ops = [_]types.BatchOp{
        .{ .put = .{ .key = "a", .value = "one" } },
        .{ .put = .{ .key = "b", .value = "two" } },
        .{ .del = .{ .key = "c" } },
    };

    const token = try engine.appendBatch(allocator, .strict, &ops);
    defer hybrid.Engine.freeGlobalCommitToken(allocator, token);

    try engine.commit();
    try engine.syncUntilGlobalToken(token);

    const a = try engine.readValue(allocator, "a");
    defer if (a) |v| allocator.free(v);
    try std.testing.expect(a != null);
    try std.testing.expectEqualStrings("one", a.?);

    const b = try engine.readValue(allocator, "b");
    defer if (b) |v| allocator.free(v);
    try std.testing.expect(b != null);
    try std.testing.expectEqualStrings("two", b.?);

    const c = try engine.readValue(allocator, "c");
    try std.testing.expect(c == null);
}

test "engine appendBatch commit and syncUntilGlobalToken" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();

    const allocator = gpa.allocator();

    var engine = try hybrid.Engine.init(allocator, .{
        .shard_count = 4,
        .inline_max = 16,
    });
    defer engine.deinit();

    const ops = [_]types.BatchOp{
        .{ .put = .{ .key = "a", .value = "one" } },
        .{ .put = .{ .key = "b", .value = "two" } },
        .{ .put = .{ .key = "c", .value = "three" } },
        .{ .del = .{ .key = "d" } },
    };

    const token = try engine.appendBatch(allocator, .strict, &ops);
    defer hybrid.Engine.freeGlobalCommitToken(allocator, token);

    try engine.commit();
    try engine.syncUntilGlobalToken(token);

    const a = try engine.readValue(allocator, "a");
    defer if (a) |v| allocator.free(v);
    try std.testing.expect(a != null);
    try std.testing.expectEqualStrings("one", a.?);

    const b = try engine.readValue(allocator, "b");
    defer if (b) |v| allocator.free(v);
    try std.testing.expect(b != null);
    try std.testing.expectEqualStrings("two", b.?);

    const c = try engine.readValue(allocator, "c");
    defer if (c) |v| allocator.free(v);
    try std.testing.expect(c != null);
    try std.testing.expectEqualStrings("three", c.?);

    const d = try engine.readValue(allocator, "d");
    try std.testing.expect(d == null);
}

test "engine frontier API reflects visible and durable progress" {
    cleanupTestFiles();
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();

    const allocator = gpa.allocator();

    var engine = try hybrid.Engine.init(allocator, .{
        .shard_count = 1,
        .inline_max = 16,
    });
    defer engine.deinit();

    const before_visible = engine.visibleFrontier(0);
    const before_durable = engine.durableFrontier(0);

    try std.testing.expectEqual(@as(u32, 0), before_visible.wal_segment_id);
    try std.testing.expectEqual(@as(u64, 0), before_visible.wal_offset);
    try std.testing.expectEqual(@as(u32, 0), before_durable.wal_segment_id);
    try std.testing.expectEqual(@as(u64, 0), before_durable.wal_offset);

    _ = try engine.put(.batch, "k1", "hello");
    try engine.commit();

    const after_commit_visible = engine.visibleFrontier(0);
    const after_commit_durable = engine.durableFrontier(0);

    try std.testing.expect(after_commit_visible.wal_offset > 0);
    try std.testing.expectEqual(@as(u64, 0), after_commit_durable.wal_offset);

    try engine.syncBatch(1000);

    const after_sync_visible = engine.visibleFrontier(0);
    const after_sync_durable = engine.durableFrontier(0);

    try std.testing.expectEqual(after_commit_visible.wal_segment_id, after_sync_visible.wal_segment_id);
    try std.testing.expectEqual(after_commit_visible.wal_offset, after_sync_visible.wal_offset);

    try std.testing.expectEqual(after_sync_visible.wal_segment_id, after_sync_durable.wal_segment_id);
    try std.testing.expectEqual(after_sync_visible.wal_offset, after_sync_durable.wal_offset);
}

test "global commit token is bounded by durable frontier after sync" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();

    const allocator = gpa.allocator();

    var engine = try hybrid.Engine.init(allocator, .{
        .shard_count = 4,
        .inline_max = 16,
    });
    defer engine.deinit();

    const ops = [_]types.BatchOp{
        .{ .put = .{ .key = "a", .value = "one" } },
        .{ .put = .{ .key = "b", .value = "two" } },
        .{ .put = .{ .key = "c", .value = "three" } },
    };

    const token = try engine.appendBatch(allocator, .strict, &ops);
    defer hybrid.Engine.freeGlobalCommitToken(allocator, token);

    try engine.commit();
    try engine.syncUntilGlobalToken(token);

    for (token.tokens) |t| {
        const durable = engine.durableFrontier(t.shard_id);
        try std.testing.expect(durable.wal_segment_id > t.wal_segment_id or
            (durable.wal_segment_id == t.wal_segment_id and durable.wal_offset >= t.wal_offset));
    }
}
