const std = @import("std");
const engine_mod = @import("engine.zig");
const auctra_api = @import("auctra_api.zig");
const auctra_cli = @import("auctra_cli.zig");
const Engine = engine_mod.Engine;
const server = @import("server/server.zig");

fn printDurationNs(label: []const u8, ns: u64) void {
    const us = @divTrunc(ns, std.time.ns_per_us);
    const ms = @divTrunc(ns, std.time.ns_per_ms);
    std.debug.print("   {s}: {d} ns ({d} us, {d} ms)\n", .{ label, ns, us, ms });
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var args = std.process.args();
    _ = args.next();

    const cmd = args.next() orelse {
        auctra_cli.printUsage();
        return;
    };

    if (std.mem.eql(u8, cmd, "server")) {
        try server.run(allocator, "127.0.0.1");
        return;
    }

    if (std.mem.eql(u8, cmd, "demo")) {
        try runDemo(allocator);
        return;
    }

    var db = try auctra_api.AuctraDb.init(allocator, .{});
    defer db.deinit();

    if (try auctra_cli.handleTopLevel(&db, allocator, cmd, &args)) return;

    const engine = &db.engine;

    if (std.mem.eql(u8, cmd, "stats")) {
        try printStats(engine, allocator);
    } else if (std.mem.eql(u8, cmd, "frontier")) {
        try printFrontier(engine);
    } else if (std.mem.eql(u8, cmd, "inspect")) {
        try printInspect(engine);
    } else if (std.mem.eql(u8, cmd, "compact")) {
        try handleCompact(engine, &args);
    } else if (std.mem.eql(u8, cmd, "checkpoint")) {
        try handleCheckpoint(engine, allocator, &args);
    } else if (std.mem.eql(u8, cmd, "snapshot")) {
        try handleSnapshot(&db, allocator, &args);
    } else {
        std.debug.print("unknown command: {s}\n", .{cmd});
        auctra_cli.printUsage();
    }
}

fn runDemo(allocator: std.mem.Allocator) !void {
    const snapshot_path = "demo-snapshot.bin";

    var db = try auctra_api.AuctraDb.init(allocator, .{});
    defer db.deinit();

    std.debug.print("Auctra demo\n", .{});
    std.debug.print("==================\n\n", .{});
    std.debug.print("This demo shows append, snapshot, restore, and rough operation timings.\n\n", .{});

    std.debug.print("1) create user:1 => Alice\n", .{});
    var t0: i128 = std.time.nanoTimestamp();
    try db.put("user:1", "Alice");
    var elapsed: u64 = @intCast(std.time.nanoTimestamp() - t0);
    std.debug.print("   current value visible immediately\n", .{});
    printDurationNs("put(user:1=Alice)", elapsed);
    {
        const value = try db.get("user:1");
        defer value.deinit(allocator);
        std.debug.print("   read-after-write: {s}\n\n", .{value.bytes});
    }

    std.debug.print("2) update user:1 => Bob\n", .{});
    t0 = std.time.nanoTimestamp();
    try db.put("user:1", "Bob");
    elapsed = @intCast(std.time.nanoTimestamp() - t0);
    printDurationNs("put(user:1=Bob)", elapsed);
    {
        const value = try db.get("user:1");
        defer value.deinit(allocator);
        std.debug.print("   current value: {s}\n\n", .{value.bytes});
    }

    std.debug.print("3) create user:2 => Carol\n", .{});
    t0 = std.time.nanoTimestamp();
    try db.put("user:2", "Carol");
    elapsed = @intCast(std.time.nanoTimestamp() - t0);
    printDurationNs("put(user:2=Carol)", elapsed);
    std.debug.print("   exists user:2 = {s}\n\n", .{if (db.exists("user:2")) "true" else "false"});

    std.debug.print("4) save snapshot => {s}\n", .{snapshot_path});
    t0 = std.time.nanoTimestamp();
    try db.snapshot(snapshot_path);
    elapsed = @intCast(std.time.nanoTimestamp() - t0);
    std.debug.print("   snapshot saved\n", .{});
    printDurationNs("snapshot(save)", elapsed);
    std.debug.print("\n", .{});

    std.debug.print("5) mutate state after snapshot\n", .{});
    t0 = std.time.nanoTimestamp();
    try db.put("user:1", "Eve");
    const put_after_snapshot_ns: u64 = @intCast(std.time.nanoTimestamp() - t0);

    t0 = std.time.nanoTimestamp();
    const deleted = try db.delete("user:2");
    const delete_after_snapshot_ns: u64 = @intCast(std.time.nanoTimestamp() - t0);

    std.debug.print("   user:1 overwritten with Eve\n", .{});
    printDurationNs("put(user:1=Eve)", put_after_snapshot_ns);
    std.debug.print("   deleted user:2 = {s}\n", .{if (deleted) "true" else "false"});
    printDurationNs("delete(user:2)", delete_after_snapshot_ns);
    std.debug.print("\n", .{});

    std.debug.print("6) restore snapshot\n", .{});
    t0 = std.time.nanoTimestamp();
    try db.restore(snapshot_path);
    elapsed = @intCast(std.time.nanoTimestamp() - t0);
    std.debug.print("   restore complete\n", .{});
    printDurationNs("snapshot(restore)", elapsed);
    std.debug.print("\n", .{});

    std.debug.print("7) verify restored state\n", .{});
    t0 = std.time.nanoTimestamp();
    const value1 = try db.get("user:1");
    defer value1.deinit(allocator);
    const get_user1_ns: u64 = @intCast(std.time.nanoTimestamp() - t0);

    const exists_user2 = db.exists("user:2");

    std.debug.print("   user:1 = {s}\n", .{value1.bytes});
    printDurationNs("get(user:1)", get_user1_ns);
    std.debug.print("   exists user:2 = {s}\n", .{if (exists_user2) "true" else "false"});

    const ok_user1 = std.mem.eql(u8, value1.bytes, "Bob");
    const ok_user2 = exists_user2;

    if (ok_user1 and ok_user2) {
        std.debug.print("\nstate correct ✅\n", .{});
    } else {
        std.debug.print("\nstate incorrect ❌\n", .{});
        return error.DemoVerificationFailed;
    }

    std.fs.cwd().deleteFile(snapshot_path) catch {};
}

fn handleSnapshot(
    db: *auctra_api.AuctraDb,
    allocator: std.mem.Allocator,
    args: *std.process.ArgIterator,
) !void {
    _ = allocator;
    const sub = args.next() orelse {
        std.debug.print("usage: snapshot [save|inspect|restore] <path>\n", .{});
        return;
    };

    if (std.mem.eql(u8, sub, "save")) {
        const path = args.next() orelse {
            std.debug.print("usage: snapshot save <path> [visible|durable]\n", .{});
            return;
        };

        const kind = auctra_cli.parseCheckpointKind(args.next());
        try db.snapshotWithKind(path, kind);
        std.debug.print("snapshot saved: {s} ({s})\n", .{ path, @tagName(kind) });
        return;
    }

    if (std.mem.eql(u8, sub, "inspect")) {
        const path = args.next() orelse {
            std.debug.print("usage: snapshot inspect <path>\n", .{});
            return;
        };

        const info = try db.inspectSnapshot(path);
        std.debug.print("=== SNAPSHOT ===\n\n", .{});
        std.debug.print("path: {s}\n", .{path});
        std.debug.print("version: {d}\n", .{info.version});
        std.debug.print("shard_count: {d}\n", .{info.shard_count});
        std.debug.print("kind: {s}\n", .{@tagName(info.kind)});
        std.debug.print("created_at_unix_ns: {d}\n", .{info.created_at_unix_ns});
        std.debug.print("high_seqno: {d}\n", .{info.high_seqno});
        std.debug.print("entry_count: {d}\n\n", .{info.entry_count});
        return;
    }

    if (std.mem.eql(u8, sub, "restore")) {
        const path = args.next() orelse {
            std.debug.print("usage: snapshot restore <path>\n", .{});
            return;
        };

        try db.restore(path);
        std.debug.print("snapshot restored: {s}\n", .{path});
        return;
    }

    std.debug.print("unknown snapshot command: {s}\n", .{sub});
    std.debug.print("usage: snapshot [save|inspect|restore] <path>\n", .{});
}

fn printStats(engine: *Engine, allocator: std.mem.Allocator) !void {
    const stats = try engine.captureStats(allocator);
    defer engine_mod.Engine.freeStats(allocator, stats);

    std.debug.print("=== STATS ===\n\n", .{});
    std.debug.print("shard_count: {d}\n\n", .{stats.shard_count});

    for (stats.visible_offsets, 0..) |visible, i| {
        const durable = stats.durable_offsets[i];
        std.debug.print(
            "shard {d}: visible=(seg={d} off={d}) durable=(seg={d} off={d})\n",
            .{ i, visible.wal_segment_id, visible.wal_offset, durable.wal_segment_id, durable.wal_offset },
        );
    }

    std.debug.print("\n", .{});
}

fn printFrontier(engine: *Engine) !void {
    std.debug.print("=== FRONTIER ===\n\n", .{});

    for (engine.shards, 0..) |*shard, i| {
        const visible = shard.readLimit(.visible);
        const durable = shard.readLimit(.durable);

        std.debug.print("shard {d}:\n", .{i});
        std.debug.print("  visible: seg={d} off={d}\n", .{ visible.wal_segment_id, visible.wal_offset });
        std.debug.print("  durable: seg={d} off={d}\n", .{ durable.wal_segment_id, durable.wal_offset });
        std.debug.print("  pending: {d}\n\n", .{shard.pending.items.len});
    }
}

fn printInspect(engine: *Engine) !void {
    std.debug.print("=== INSPECT ===\n\n", .{});
    std.debug.print("shards: {d}\n\n", .{engine.shards.len});

    for (engine.shards, 0..) |*shard, i| {
        std.debug.print("shard {d}:\n", .{i});
        std.debug.print("  state_index: {d}\n", .{shard.state_index.count()});
        std.debug.print("  pending: {d}\n", .{shard.pending.items.len});
        std.debug.print("  next_seqno: {d}\n\n", .{shard.next_seqno});
    }
}

fn handleCompact(engine: *Engine, args: *std.process.ArgIterator) !void {
    const sub = args.next() orelse {
        std.debug.print("usage: compact [all|shard <id>]\n", .{});
        return;
    };

    if (std.mem.eql(u8, sub, "all")) {
        try engine.compactAll();
        std.debug.print("compaction complete: all shards\n", .{});
        return;
    }

    if (std.mem.eql(u8, sub, "shard")) {
        const shard_arg = args.next() orelse {
            std.debug.print("usage: compact shard <id>\n", .{});
            return;
        };
        const shard_id = try std.fmt.parseInt(u16, shard_arg, 10);
        try engine.compactShard(shard_id);
        std.debug.print("compaction complete: shard={d}\n", .{shard_id});
        return;
    }

    std.debug.print("usage: compact [all|shard <id>]\n", .{});
}

fn handleCheckpoint(
    engine: *Engine,
    allocator: std.mem.Allocator,
    args: *std.process.ArgIterator,
) !void {
    const sub = args.next() orelse {
        std.debug.print("usage: checkpoint [save|inspect] <path>\n", .{});
        return;
    };

    if (std.mem.eql(u8, sub, "save")) {
        const path = args.next() orelse {
            std.debug.print("usage: checkpoint save <path> [visible|durable]\n", .{});
            return;
        };

        const kind = auctra_cli.parseCheckpointKind(args.next());
        try engine.writeCheckpointFile(allocator, path, kind);
        std.debug.print("checkpoint saved: {s} ({s})\n", .{ path, @tagName(kind) });
        return;
    }

    if (std.mem.eql(u8, sub, "inspect")) {
        const path = args.next() orelse {
            std.debug.print("usage: checkpoint inspect <path>\n", .{});
            return;
        };

        const checkpoint = try engine_mod.Engine.readCheckpointFile(allocator, path);
        defer engine_mod.Engine.freeCheckpoint(allocator, checkpoint);

        std.debug.print("=== CHECKPOINT ===\n\n", .{});
        std.debug.print("path: {s}\n", .{path});
        std.debug.print("version: {d}\n", .{checkpoint.version});
        std.debug.print("shard_count: {d}\n", .{checkpoint.shard_count});
        std.debug.print("kind: {s}\n", .{@tagName(checkpoint.kind)});
        std.debug.print("created_at_unix_ns: {d}\n", .{checkpoint.created_at_unix_ns});
        std.debug.print("high_seqno: {d}\n", .{checkpoint.high_seqno});

        for (checkpoint.shard_cursors, 0..) |cursor, i| {
            std.debug.print("shard {d}: seg={d} off={d}\n", .{ i, cursor.wal_segment_id, cursor.wal_offset });
        }
        return;
    }

    std.debug.print("unknown checkpoint command: {s}\n", .{sub});
    std.debug.print("usage: checkpoint [save|inspect] <path>\n", .{});
}
