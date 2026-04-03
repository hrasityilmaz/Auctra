const std = @import("std");

pub const Wal = struct {
    file: std.fs.File,
    path: []const u8,

    pub fn openOrCreate(allocator: std.mem.Allocator, path: []const u8) !Wal {
        const owned_path = try allocator.dupe(u8, path);
        errdefer allocator.free(owned_path);

        const cwd = std.fs.cwd();

        _ = cwd.createFile(path, .{
            .read = true,
            .truncate = false,
            .exclusive = true,
        }) catch |err| switch (err) {
            error.PathAlreadyExists => {},
            else => return err,
        };

        const file = try cwd.openFile(path, .{ .mode = .read_write });

        return .{
            .file = file,
            .path = owned_path,
        };
    }

    pub fn openShardSegment(
        allocator: std.mem.Allocator,
        shard_id: u16,
        segment_id: u32,
    ) !Wal {
        const path = if (segment_id == 0)
            try std.fmt.allocPrint(allocator, "wal-{d:0>3}.log", .{shard_id})
        else
            try std.fmt.allocPrint(allocator, "wal-{d:0>3}-{d:0>3}.log", .{ shard_id, segment_id });
        defer allocator.free(path);
        return openOrCreate(allocator, path);
    }

    pub fn openShard(allocator: std.mem.Allocator, shard_id: u16) !Wal {
        return openShardSegment(allocator, shard_id, 0);
    }

    pub fn close(self: *Wal, allocator: std.mem.Allocator) void {
        self.file.close();
        allocator.free(self.path);
    }

    pub fn writeAt(self: *Wal, offset: u64, data: []const u8) !void {
        var written: usize = 0;
        while (written < data.len) {
            const n = try self.file.pwrite(data[written..], offset + written);
            written += n;
        }
    }

    pub fn syncData(self: *Wal) !void {
        try self.file.sync();
    }
};
