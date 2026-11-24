// Copyright 2024 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "rustfs-cuobject-sys/src/sdk.rs.h"
#include <cuobjserver.h>
#include <array>
#include <cerrno>
#include <limits>
#include <stdexcept>
#include <system_error>
#include <sys/mman.h>

namespace rustfs::cuobject {

class Server {
public:
    Server(std::string address, std::uint16_t port, int channels, std::uint32_t depth)
        : address_(std::move(address)) {
        cuObjRDMATunable options;
        if (channels > 0) options.setNumDcis(channels);
        if (depth > 0) options.setCqDepth(depth);
        sdk = std::make_unique<cuObjServer>(address_.c_str(), port, CUOBJ_PROTO_RDMA_DC_V1, options);
    }
    // The SDK may retain the address string through server teardown.
    const std::string address_;
    std::unique_ptr<cuObjServer> sdk;
};

std::uint32_t header_version() noexcept {
    return CUOBJ_SERVER_MAJOR_VERSION * 1000 + CUOBJ_SERVER_MINOR_VERSION;
}

std::shared_ptr<Server> open_server(rust::Str addr, std::uint16_t port, int channels, std::uint32_t depth) {
    return std::make_shared<Server>(std::string(addr), port, channels, depth);
}

class Buffer {
public:
    std::shared_ptr<Server> owner;
    std::shared_ptr<Buffer> base;
    rdma_buffer* handle = nullptr;
    void* mapping = MAP_FAILED;
    std::size_t length = 0;

    static void destroy(Buffer* buffer) noexcept {
        try {
            if (buffer->handle) {
                if (buffer->base) buffer->owner->sdk->putRDMABufferFromSgList(buffer->handle);
                else buffer->owner->sdk->deRegisterBuffer(buffer->handle);
            }
        } catch (...) {
            // The SDK may still reference the mapping and its owning server.
            return;
        }
        if (buffer->mapping != MAP_FAILED) munmap(buffer->mapping, buffer->length);
        delete buffer;
    }
};

std::shared_ptr<Buffer> register_buffer(std::shared_ptr<Server> server, std::size_t length) {
    if (!server || !length) throw std::invalid_argument("invalid host registration");
    auto buffer = std::shared_ptr<Buffer>(new Buffer, Buffer::destroy);
    buffer->owner = std::move(server);
    buffer->length = length;
    buffer->mapping = mmap(nullptr, length, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (buffer->mapping == MAP_FAILED) throw std::system_error(errno, std::generic_category(), "mmap");
    buffer->handle = buffer->owner->sdk->registerBuffer(buffer->mapping, length);
    if (!buffer->handle) throw std::runtime_error("NVIDIA memory registration failed");
    return buffer;
}

std::shared_ptr<Buffer> scatter_list(std::shared_ptr<Buffer> base, rust::Slice<const Range> ranges) {
    if (!base || base->base || ranges.empty() || ranges.size() > 10) throw std::invalid_argument("invalid scatter list");
    auto list = std::shared_ptr<Buffer>(new Buffer, Buffer::destroy);
    list->owner = base->owner;
    list->base = std::move(base);
    std::vector<cuObjScatterGatherEntryWithOffset_t> entries;
    entries.reserve(ranges.size());
    for (auto range : ranges) {
        if (!range.size || range.offset > list->base->length || range.size > list->base->length - range.offset ||
            range.offset > std::numeric_limits<loff_t>::max() || range.size > SIZE_MAX - list->length)
            throw std::invalid_argument("invalid scatter range");
        entries.push_back({static_cast<loff_t>(range.offset), range.size});
        list->length += range.size;
    }
    list->handle = list->owner->sdk->getRDMABufferFromSgList(list->base->handle, std::move(entries));
    if (!list->handle) throw std::runtime_error("NVIDIA scatter list registration failed");
    return list;
}

std::uint8_t* data(const Buffer& buffer) noexcept { return static_cast<std::uint8_t*>(buffer.mapping); }
std::size_t size(const Buffer& buffer) noexcept { return buffer.length; }

std::uint16_t lease_channel(const Server& server) { return server.sdk->allocateChannelId(); }
void release_channel(const Server& server, std::uint16_t channel) noexcept {
    try { server.sdk->freeChannelId(channel); } catch (...) { /* SDK owns the channel. */ }
}
int completion_fd(const Server& server, std::uint16_t channel) noexcept {
    try { return server.sdk->getCompletionFd(channel); } catch (...) { return -EIO; }
}

int drain(const Server& server, int fd, rust::Slice<Completion> out) noexcept {
    std::array<cuObjAsyncEvent_t, 8> events{};
    if (out.empty() || out.size() > events.size()) return -EINVAL;
    try {
        const int count = server.sdk->processCompletions(events.data(), static_cast<int>(out.size()), fd);
        if (count > static_cast<int>(out.size())) return -EIO;
        for (int i = 0; i < count; ++i) {
            out[i] = {reinterpret_cast<std::uintptr_t>(events[i].async_handle), events[i].status};
        }
        return count;
    } catch (...) { return -EIO; }
}

Outcome transfer(const Server& server, const Buffer& buffer, const Operation& op) noexcept {
    if (buffer.owner.get() != &server || !op.length || op.offset > buffer.length ||
        op.length > buffer.length - op.offset || (buffer.base && (op.offset || op.length != buffer.length)))
        return {-EINVAL, 0, false};
    ibv_wc_status verbs = IBV_WC_SUCCESS;
    bool submitted_to_sdk = false;
    try {
        const std::string key(op.key), token(op.token);
        void* handle = reinterpret_cast<void*>(op.handle);
        auto& sdk = *server.sdk;
        submitted_to_sdk = true;
        const ssize_t moved = op.put
            ? sdk.handlePutObject(key, buffer.handle, op.address, op.length, token, op.channel, op.offset, &verbs, handle)
            : sdk.handleGetObject(key, buffer.handle, op.address, op.length, token, op.channel, op.offset, &verbs, handle);
        return {moved, static_cast<int>(verbs), false};
    } catch (...) { return {-EIO, static_cast<int>(verbs), submitted_to_sdk}; }
}
}
