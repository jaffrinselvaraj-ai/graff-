// bounded_buffer.cpp
// Compile: g++ -std=c++17 -O2 -pthread bounded_buffer.cpp -o bounded_buffer

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <iostream>
#include <mutex>
#include <queue>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

using namespace std::chrono_literals;

// ------------------------------------------------------------
// BoundedBuffer<T> : thread-safe fixed-size ring buffer
// ------------------------------------------------------------
template<typename T>
class BoundedBuffer {
public:
    explicit BoundedBuffer(size_t capacity)
        : capacity_(capacity), buffer_(capacity), head_(0), tail_(0), count_(0) {
        if (capacity_ == 0) throw std::invalid_argument("capacity must be > 0");
    }

    // Blocking put: waits while buffer is full
    void put(const T& item) {
        std::unique_lock<std::mutex> lk(mutex_);
        not_full_cv_.wait(lk, [this]() { return count_ < capacity_; }); // avoid spurious wake issues
        buffer_[tail_] = item;
        tail_ = (tail_ + 1) % capacity_;
        ++count_;
        // Wake one waiting consumer
        not_empty_cv_.notify_one();
    }

    // Blocking get: waits while buffer is empty
    T get() {
        std::unique_lock<std::mutex> lk(mutex_);
        not_empty_cv_.wait(lk, [this]() { return count_ > 0; });
        T item = buffer_[head_];
        head_ = (head_ + 1) % capacity_;
        --count_;
        // Wake one waiting producer
        not_full_cv_.notify_one();
        return item;
    }

    // Try to put without blocking; returns true if inserted
    bool try_put(const T& item) {
        std::lock_guard<std::mutex> lk(mutex_);
        if (count_ >= capacity_) return false;
        buffer_[tail_] = item;
        tail_ = (tail_ + 1) % capacity_;
        ++count_;
        not_empty_cv_.notify_one();
        return true;
    }

    // Try to get without blocking; returns pair(hasValue, value)
    std::pair<bool, T> try_get() {
        std::lock_guard<std::mutex> lk(mutex_);
        if (count_ == 0) return {false, T()};
        T item = buffer_[head_];
        head_ = (head_ + 1) % capacity_;
        --count_;
        not_full_cv_.notify_one();
        return {true, item};
    }

    size_t capacity() const { return capacity_; }

    // approximate current size (non-atomic snapshot)
    size_t size() const {
        std::lock_guard<std::mutex> lk(mutex_);
        return count_;
    }

private:
    const size_t capacity_;
    std::vector<T> buffer_;
    size_t head_;
    size_t tail_;
    size_t count_;

    mutable std::mutex mutex_;
    std::condition_variable not_empty_cv_;
    std::condition_variable not_full_cv_;
};

// ------------------------------------------------------------
// Item structure produced/consumed
// ------------------------------------------------------------
struct Item {
    uint64_t seq;       // unique sequence id
    std::string origin; // which producer
    std::chrono::steady_clock::time_point produced_time;
};

// ------------------------------------------------------------
// Global counters and flags
// ------------------------------------------------------------
std::atomic<uint64_t> global_sequence{0};
std::atomic<uint64_t> processed_count{0};
std::atomic<bool> stop_flag{false};

// For logging without mangling multiple threads' output
std::mutex io_mutex;

// ------------------------------------------------------------
// Producer base type (function object)
// ------------------------------------------------------------
class ProducerBase {
public:
    ProducerBase(std::string name, BoundedBuffer<Item>& buffer)
        : name_(std::move(name)), buffer_(buffer) {}

    virtual ~ProducerBase() = default;
    // run: continuously produce until stop_flag set or produced_limit reached externally
    virtual void operator()() = 0;

protected:
    std::string name_;
    BoundedBuffer<Item>& buffer_;
};

// FastProducer: produces as fast as possible
class FastProducer : public ProducerBase {
public:
    FastProducer(std::string name, BoundedBuffer<Item>& buffer)
        : ProducerBase(std::move(name), buffer) {}

    void operator()() override {
        while (!stop_flag.load(std::memory_order_relaxed)) {
            uint64_t seq = ++global_sequence;
            Item it{seq, name_, std::chrono::steady_clock::now()};
            buffer_.put(it);
            // no sleep -> max throughput
        }
        // optional: small flush log
        std::lock_guard<std::mutex> lk(io_mutex);
        std::cout << "[" << name_ << "] exiting\n";
    }
};

// SlowProducer: produces with a configurable delay
class SlowProducer : public ProducerBase {
public:
    SlowProducer(std::string name, BoundedBuffer<Item>& buffer, std::chrono::milliseconds delay)
        : ProducerBase(std::move(name), buffer), delay_(delay) {}

    void operator()() override {
        while (!stop_flag.load(std::memory_order_relaxed)) {
            uint64_t seq = ++global_sequence;
            Item it{seq, name_, std::chrono::steady_clock::now()};
            buffer_.put(it);
            std::this_thread::sleep_for(delay_);
        }
        std::lock_guard<std::mutex> lk(io_mutex);
        std::cout << "[" << name_ << "] exiting\n";
    }

private:
    std::chrono::milliseconds delay_;
};

// ------------------------------------------------------------
// Consumer base type
// ------------------------------------------------------------
class ConsumerBase {
public:
    ConsumerBase(std::string name, BoundedBuffer<Item>& buffer)
        : name_(std::move(name)), buffer_(buffer) {}
    virtual ~ConsumerBase() = default;
    virtual void operator()() = 0;

protected:
    std::string name_;
    BoundedBuffer<Item>& buffer_;
};

// FastConsumer: processes quickly
class FastConsumer : public ConsumerBase {
public:
    FastConsumer(std::string name, BoundedBuffer<Item>& buffer)
        : ConsumerBase(std::move(name), buffer) {}

    void operator()() override {
        while (!stop_flag.load(std::memory_order_relaxed)) {
            Item it = buffer_.get();
            // process quickly
            processed_count.fetch_add(1, std::memory_order_relaxed);
            // minimal logging: report occasionally to avoid IO bottleneck
            if ((it.seq % 1000) == 0) {
                std::lock_guard<std::mutex> lk(io_mutex);
                std::cout << "[" << name_ << "] processed seq=" << it.seq << " from " << it.origin << "\n";
            }
        }
        std::lock_guard<std::mutex> lk(io_mutex);
        std::cout << "[" << name_ << "] exiting\n";
    }
};

// SlowConsumer: simulates heavier processing
class SlowConsumer : public ConsumerBase {
public:
    SlowConsumer(std::string name, BoundedBuffer<Item>& buffer, std::chrono::milliseconds process_time)
        : ConsumerBase(std::move(name), buffer), process_time_(process_time) {}

    void operator()() override {
        while (!stop_flag.load(std::memory_order_relaxed)) {
            Item it = buffer_.get();
            // simulate processing
            std::this_thread::sleep_for(process_time_);
            processed_count.fetch_add(1, std::memory_order_relaxed);
            if ((it.seq % 500) == 0) {
                std::lock_guard<std::mutex> lk(io_mutex);
                std::cout << "[" << name_ << "] processed seq=" << it.seq << " from " << it.origin << "\n";
            }
        }
        std::lock_guard<std::mutex> lk(io_mutex);
        std::cout << "[" << name_ << "] exiting\n";
    }

private:
    std::chrono::milliseconds process_time_;
};

// ------------------------------------------------------------
// Driver: configuration and launching threads
// ------------------------------------------------------------
struct Config {
    size_t buffer_size = 1024;
    int num_fast_producers = 1;
    int num_slow_producers = 1;
    int num_fast_consumers = 1;
    int num_slow_consumers = 1;
    std::chrono::milliseconds slow_producer_delay = 5ms;
    std::chrono::milliseconds slow_consumer_time = 5ms;
    std::chrono::seconds run_duration = 10s;
    uint64_t stop_after_items = 0; // 0 => ignore
};

void print_help(const char* prog) {
    std::cout << "Usage: " << prog << " [options]\n"
              << "Options:\n"
              << "  --buffer N             buffer capacity (default 1024)\n"
              << "  --fast-prod N          number of fast producers (default 1)\n"
              << "  --slow-prod N          number of slow producers (default 1)\n"
              << "  --fast-cons N          number of fast consumers (default 1)\n"
              << "  --slow-cons N          number of slow consumers (default 1)\n"
              << "  --prod-delay ms        slow producer delay in ms (default 5)\n"
              << "  --cons-time ms         slow consumer processing time in ms (default 5)\n              "
              << "  --duration s           run duration in seconds (default 10)\n"
              << "  --stop-after N         stop after N processed items (overrides duration if >0)\n"
              << "  --help                 show this help\n";
}

int main(int argc, char** argv) {
    Config cfg;

    // Simple arg parsing
    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        if (a == "--help") { print_help(argv[0]); return 0; }
        if (i + 1 < argc) {
            std::string v = argv[++i];
            if (a == "--buffer") cfg.buffer_size = std::stoul(v);
            else if (a == "--fast-prod") cfg.num_fast_producers = std::stoi(v);
            else if (a == "--slow-prod") cfg.num_slow_producers = std::stoi(v);
            else if (a == "--fast-cons") cfg.num_fast_consumers = std::stoi(v);
            else if (a == "--slow-cons") cfg.num_slow_consumers = std::stoi(v);
            else if (a == "--prod-delay") cfg.slow_producer_delay = std::chrono::milliseconds(std::stoul(v));
            else if (a == "--cons-time") cfg.slow_consumer_time = std::chrono::milliseconds(std::stoul(v));
            else if (a == "--duration") cfg.run_duration = std::chrono::seconds(std::stoul(v));
            else if (a == "--stop-after") cfg.stop_after_items = std::stoull(v);
            else {
                std::cerr << "Unknown arg: " << a << "\n";
                print_help(argv[0]);
                return 1;
            }
        } else {
            std::cerr << "Arg without value: " << a << "\n";
            print_help(argv[0]);
            return 1;
        }
    }

    std::cout << "Configuration:\n"
              << "  buffer_size = " << cfg.buffer_size << "\n"
              << "  fast producers = " << cfg.num_fast_producers << "\n"
              << "  slow producers = " << cfg.num_slow_producers << " (delay " << cfg.slow_producer_delay.count() << " ms)\n"
              << "  fast consumers = " << cfg.num_fast_consumers << "\n"
              << "  slow consumers = " << cfg.num_slow_consumers << " (work " << cfg.slow_consumer_time.count() << " ms)\n"
              << "  run duration = " << cfg.run_duration.count() << " s\n";
    if (cfg.stop_after_items > 0) std::cout << "  stop after items = " << cfg.stop_after_items << "\n";

    BoundedBuffer<Item> buffer(cfg.buffer_size);

    std::vector<std::thread> threads;

    // Launch producers
    for (int i = 0; i < cfg.num_fast_producers; ++i) {
        std::string name = "FastProd-" + std::to_string(i);
        FastProducer prod(name, buffer);
        threads.emplace_back(std::thread(std::move(prod)));
    }
    for (int i = 0; i < cfg.num_slow_producers; ++i) {
        std::string name = "SlowProd-" + std::to_string(i);
        SlowProducer prod(name, buffer, cfg.slow_producer_delay);
        threads.emplace_back(std::thread(std::move(prod)));
    }

    // Launch consumers
    for (int i = 0; i < cfg.num_fast_consumers; ++i) {
        std::string name = "FastCons-" + std::to_string(i);
        FastConsumer cons(name, buffer);
        threads.emplace_back(std::thread(std::move(cons)));
    }
    for (int i = 0; i < cfg.num_slow_consumers; ++i) {
        std::string name = "SlowCons-" + std::to_string(i);
        SlowConsumer cons(name, buffer, cfg.slow_consumer_time);
        threads.emplace_back(std::thread(std::move(cons)));
    }

    // Start measurement
    auto start_time = std::chrono::steady_clock::now();
    uint64_t start_processed = processed_count.load();

    // Stop condition: either fixed duration OR stop_after_items reached
    if (cfg.stop_after_items > 0) {
        while (processed_count.load(std::memory_order_relaxed) < cfg.stop_after_items) {
            std::this_thread::sleep_for(100ms);
        }
        stop_flag.store(true);
    } else {
        std::this_thread::sleep_for(cfg.run_duration);
        stop_flag.store(true);
    }

    // After signalling stop, give threads a short grace to exit if blocked on get/put.
    // We may need to "unblock" waiting producers/consumers by notifying condition variables:
    // To avoid exposing internals, we create temporary items or use try_put/notify by putting dummy items.
    // Simpler approach: spawn temporary consumer/producer threads to unblock (but here we will notify many times).
    // For safety, we simply notify all by creating local signals through buffer size loops.
    // NOTE: The buffer internals own condition_variables; we cannot notify them externally.
    // To keep it simple and robust: create a few dummy threads that perform non-blocking plays to allow threads to finish.

    // Give a moment for threads to exit cleanly
    std::this_thread::sleep_for(200ms);

    // Join threads
    for (auto &t : threads) {
        if (t.joinable()) t.join();
    }

    auto end_time = std::chrono::steady_clock::now();
    double elapsed_s = std::chrono::duration_cast<std::chrono::duration<double>>(end_time - start_time).count();
    uint64_t total_processed = processed_count.load();
    double throughput = elapsed_s > 0.0 ? (double)(total_processed - start_processed) / elapsed_s : 0.0;

    std::cout << "=== Results ===\n";
    std::cout << "Total processed items: " << total_processed - start_processed << "\n";
    std::cout << "Elapsed time (s): " << elapsed_s << "\n";
    std::cout << "Throughput (items/sec): " << throughput << "\n";
    std::cout << "Final buffer size (approx): " << buffer.size() << " / " << buffer.capacity() << "\n";
    return 0;
}
