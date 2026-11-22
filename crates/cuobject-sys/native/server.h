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

#pragma once
#include "rust/cxx.h"
#include <cstddef>
#include <cstdint>
#include <memory>

namespace rustfs::cuobject {
class Server;
class Buffer;
struct Range;
struct Completion;
struct Operation;
struct Outcome;

std::uint32_t header_version() noexcept;
std::shared_ptr<Server> open_server(rust::Str address, std::uint16_t port, int channels, std::uint32_t depth);
std::shared_ptr<Buffer> register_buffer(std::shared_ptr<Server> server, std::size_t length);
std::shared_ptr<Buffer> scatter_list(std::shared_ptr<Buffer> base, rust::Slice<const Range> ranges);
std::uint8_t* data(const Buffer& buffer) noexcept;
std::size_t size(const Buffer& buffer) noexcept;
std::uint16_t lease_channel(const Server& server);
void release_channel(const Server& server, std::uint16_t channel) noexcept;
int completion_fd(const Server& server, std::uint16_t channel) noexcept;
int drain(const Server& server, int fd, rust::Slice<Completion> events) noexcept;
Outcome transfer(const Server& server, const Buffer& buffer, const Operation& op) noexcept;
}
