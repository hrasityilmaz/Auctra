const std = @import("std");
const builtin = @import("builtin");
const uring_mod = @import("uring.zig");

pub const BackendKind = enum {
    pwrite,
    io_uring,
};

pub const WriteOp = struct {
    file: *std.fs.File,
    offset: u64,
    data: []const u8,
};

pub const Backend = struct {
    kind: BackendKind,
    uring: ?uring_mod.UringWriter = null,

    pub fn init(kind: BackendKind, entries: u16) !Backend {
        return switch (kind) {
            .pwrite => .{
                .kind = .pwrite,
                .uring = null,
            },
            .io_uring => blk: {
                if (builtin.os.tag != .linux) return error.UnsupportedPlatform;

                break :blk .{
                    .kind = .io_uring,
                    .uring = try uring_mod.UringWriter.init(entries),
                };
            },
        };
    }

    pub fn deinit(self: *Backend) void {
        if (self.uring) |*u| u.deinit();
        self.* = undefined;
    }

    pub fn writeAt(
        self: *Backend,
        file: *std.fs.File,
        offset: u64,
        data: []const u8,
    ) !void {
        const one = [_]WriteOp{
            .{
                .file = file,
                .offset = offset,
                .data = data,
            },
        };
        try self.writeBatch(&one);
    }

    pub fn writeBatch(self: *Backend, ops: []const WriteOp) !void {
        if (ops.len == 0) return;

        switch (self.kind) {
            .pwrite => {
                for (ops) |op| {
                    var written: usize = 0;
                    while (written < op.data.len) {
                        const n = try op.file.pwrite(op.data[written..], op.offset + written);
                        written += n;
                    }
                }
            },
            .io_uring => {
                try self.uring.?.writeBatch(ops);
            },
        }
    }
};
