#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <memory>
#include <vector>

#include "command.hpp"
#include "yqueue.hpp"

namespace zmq {
void zmq_abort(const char *message) {
    std::fprintf(stderr, "%s\n", message);
    std::abort();
}
}

int main() {
    using Queue = zmq::yqueue_t<zmq::command_t, 16>;
    std::vector<std::unique_ptr<Queue>> queues;
    std::size_t misaligned = 0;
    // Retain allocations to exercise multiple heap size classes and offsets.
    for (int index = 0; index < 256; ++index) {
        auto queue = std::make_unique<Queue>();
        for (int message = 0; message < 33; ++message) {
            queue->push();
            const auto address = reinterpret_cast<std::uintptr_t>(&queue->back());
            if (address % alignof(zmq::command_t) != 0) ++misaligned;
        }
        queues.push_back(std::move(queue));
    }
    std::printf("{\"alignment_bytes\":%zu,\"queue_items\":8448,\"misaligned_items\":%zu}\n",
                alignof(zmq::command_t), misaligned);
    return 0;
}
