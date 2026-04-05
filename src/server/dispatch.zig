const std = @import("std");
const hybrid_api = @import("../hybrid_api.zig");
const wire = @import("../wire/protocol.zig");

pub fn handleFrame(
    allocator: std.mem.Allocator,
    state: anytype,
    reader: anytype,
    writer: anytype,
    header: wire.FrameHeader,
) !void {
    if (header.version != wire.protocol_version) {
        const opcode = wire.Opcode.fromInt(header.opcode) orelse .ping;
        try wire.writeErrorResponse(writer, header.request_id, opcode, .unsupported_version);
        return;
    }

    if (header.frame_len > wire.default_max_frame_size) {
        return error.FrameTooLarge;
    }

    const opcode = wire.Opcode.fromInt(header.opcode) orelse {
        if (header.frame_len > 0) {
            try wire.discardExact(reader, header.frame_len);
        }
        try wire.writeErrorResponse(writer, header.request_id, .ping, .invalid_opcode);
        return;
    };

    switch (opcode) {
        .ping => try handlePing(reader, writer, header),
        .get => try handleGet(allocator, state, reader, writer, header),
        .append => try handleAppend(allocator, state, reader, writer, header),
        else => {
            if (header.frame_len > 0) {
                try wire.discardExact(reader, header.frame_len);
            }
            try wire.writeErrorResponse(writer, header.request_id, opcode, .invalid_opcode);
        },
    }
}

fn handlePing(reader: anytype, writer: anytype, header: wire.FrameHeader) !void {
    if (header.frame_len != 0) {
        try wire.discardExact(reader, header.frame_len);
        try wire.writeErrorResponse(writer, header.request_id, .ping, .bad_request);
        return;
    }

    var payload_buf: [wire.PingResponse.encoded_len]u8 = undefined;
    var fbs = std.io.fixedBufferStream(&payload_buf);

    const resp = wire.PingResponse{
        .major = 0,
        .minor = 1,
        .patch = 0,
    };
    try resp.write(fbs.writer());

    try wire.writeHeaderAndPayload(
        writer,
        .ping,
        header.request_id,
        .ok,
        fbs.getWritten(),
    );
}

fn handleGet(
    allocator: std.mem.Allocator,
    state: anytype,
    reader: anytype,
    writer: anytype,
    header: wire.FrameHeader,
) !void {
    const payload = try wire.readPayloadAlloc(
        allocator,
        reader,
        header.frame_len,
        wire.default_max_frame_size,
    );
    defer allocator.free(payload);

    var req = wire.GetRequest.decode(allocator, payload) catch {
        try wire.writeErrorResponse(writer, header.request_id, .get, .bad_request);
        return;
    };
    defer req.deinit(allocator);

    const value_result = state.db.get(req.key) catch |err| {
        switch (err) {
            error.KeyNotFound => {
                const resp = wire.GetResponse{
                    .found = false,
                    .value = "",
                };
                const encoded = try resp.encode(allocator);
                defer allocator.free(encoded);

                try wire.writeHeaderAndPayload(
                    writer,
                    .get,
                    header.request_id,
                    .ok,
                    encoded,
                );
                return;
            },
            else => return err,
        }
    };
    defer value_result.deinit(allocator);

    const resp = wire.GetResponse{
        .found = true,
        .value = value_result.bytes,
    };
    const encoded = try resp.encode(allocator);
    defer allocator.free(encoded);

    try wire.writeHeaderAndPayload(
        writer,
        .get,
        header.request_id,
        .ok,
        encoded,
    );
}

fn handleAppend(
    allocator: std.mem.Allocator,
    state: anytype,
    reader: anytype,
    writer: anytype,
    header: wire.FrameHeader,
) !void {
    const payload = try wire.readPayloadAlloc(
        allocator,
        reader,
        header.frame_len,
        wire.default_max_frame_size,
    );
    defer allocator.free(payload);

    var req = wire.AppendRequest.decode(allocator, payload) catch {
        try wire.writeErrorResponse(writer, header.request_id, .append, .bad_request);
        return;
    };
    defer req.deinit(allocator);

    switch (req.durability) {
        .ultrafast, .batch, .strict => {
            // şimdilik wire seviyesinde parse ediyoruz
            // core tarafına sonra durability mapleriz
        },
    }

    try state.db.put(req.key, req.value);

    const resp = wire.AppendResponse{
        .commit_token = 0,
    };
    const encoded = try resp.encode(allocator);
    defer allocator.free(encoded);

    try wire.writeHeaderAndPayload(
        writer,
        .append,
        header.request_id,
        .ok,
        encoded,
    );
}
