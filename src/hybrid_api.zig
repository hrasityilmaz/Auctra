const std = @import("std");
const engine_mod = @import("engine.zig");
const types = @import("types.zig");

pub const KeyNotFound = error{KeyNotFound};

pub const ValueView = struct {
    bytes: []u8,

    pub fn deinit(self: ValueView, allocator: std.mem.Allocator) void {
        allocator.free(self.bytes);
    }
};

pub const OpenOptions = struct {
    config: types.EngineConfig = .{},
    default_durability: types.Durability = .strict,
    default_snapshot_kind: types.CheckpointKind = .durable,
};

pub const HybridDb = struct {
    allocator: std.mem.Allocator,
    engine: engine_mod.Engine,
    default_durability: types.Durability,
    default_snapshot_kind: types.CheckpointKind,

    pub fn init(allocator: std.mem.Allocator, options: OpenOptions) !HybridDb {
        return .{
            .allocator = allocator,
            .engine = try engine_mod.Engine.init(allocator, options.config),
            .default_durability = options.default_durability,
            .default_snapshot_kind = options.default_snapshot_kind,
        };
    }

    pub fn deinit(self: *HybridDb) void {
        self.engine.deinit();
    }

    pub fn commit(self: *HybridDb) !void {
        try self.engine.commitPending();
    }

    pub fn sync(self: *HybridDb) !void {
        try self.engine.syncNow();
    }

    pub fn put(self: *HybridDb, key: []const u8, value: []const u8) !void {
        return self.putVisible(self.default_durability, key, value);
    }

    pub fn putWithDurability(
        self: *HybridDb,
        durability: types.Durability,
        key: []const u8,
        value: []const u8,
    ) !void {
        if (key.len == 0) return error.EmptyKey;
        try self.engine.put(durability, key, value);
    }

    pub fn putVisible(
        self: *HybridDb,
        durability: types.Durability,
        key: []const u8,
        value: []const u8,
    ) !void {
        try self.putWithDurability(durability, key, value);
        try self.commit();
    }

    pub fn get(self: *HybridDb, key: []const u8) !ValueView {
        if (key.len == 0) return error.EmptyKey;

        const maybe = try self.engine.getOrNull(self.allocator, key);
        if (maybe == null) return KeyNotFound.KeyNotFound;
        return .{ .bytes = maybe.? };
    }

    pub fn exists(self: *HybridDb, key: []const u8) bool {
        if (key.len == 0) return false;
        return self.engine.get(key) != null;
    }

    pub fn delete(self: *HybridDb, key: []const u8) !bool {
        return self.deleteVisible(self.default_durability, key);
    }

    pub fn deleteWithDurability(
        self: *HybridDb,
        durability: types.Durability,
        key: []const u8,
    ) !bool {
        if (key.len == 0) return error.EmptyKey;
        const existed = self.exists(key);
        try self.engine.del(durability, key);
        return existed;
    }

    pub fn deleteVisible(
        self: *HybridDb,
        durability: types.Durability,
        key: []const u8,
    ) !bool {
        const existed = try self.deleteWithDurability(durability, key);
        try self.commit();
        return existed;
    }

    pub fn snapshot(self: *HybridDb, path: []const u8) !void {
        return self.snapshotWithKind(path, self.default_snapshot_kind);
    }

    pub fn snapshotWithKind(
        self: *HybridDb,
        path: []const u8,
        kind: types.CheckpointKind,
    ) !void {
        try self.sync();
        try self.engine.writeStateSnapshotFile(self.allocator, path, kind);
    }

    pub fn restore(self: *HybridDb, path: []const u8) !void {
        const snap = try engine_mod.Engine.readStateSnapshotFile(self.allocator, path);
        defer engine_mod.Engine.freeStateSnapshot(self.allocator, snap);
        try self.engine.restoreStateSnapshot(self.allocator, snap);
        try self.sync();
    }

    pub fn inspectSnapshot(self: *HybridDb, path: []const u8) !types.SnapshotInspectInfo {
        return try engine_mod.Engine.inspectStateSnapshotFile(self.allocator, path);
    }

    pub fn saveCheckpoint(
        self: *HybridDb,
        path: []const u8,
        kind: types.CheckpointKind,
    ) !void {
        try self.sync();
        try self.engine.writeCheckpointFile(self.allocator, path, kind);
    }
};
