import threading
import time
import random


class BoundedBuffer:
    def __init__(self, capacity):
        self.capacity = capacity
        self.buffer = []

        # Semaphores
        self.empty_slots = threading.Semaphore(capacity)  # counts remaining space
        self.full_slots = threading.Semaphore(0)          # counts items available

        # Mutex for buffer access
        self.mutex = threading.Lock()

    def put(self, item, producer_id):
        # Wait until there is an empty slot
        self.empty_slots.acquire()

        # Enter critical section
        with self.mutex:
            self.buffer.append(item)
            print(f"[Producer {producer_id}] Produced: {item} | Buffer size: {len(self.buffer)}")

        # Signal that a full slot is available
        self.full_slots.release()

    def get(self, consumer_id):
        # Wait until an item exists
        self.full_slots.acquire()

        # Enter critical section
        with self.mutex:
            item = self.buffer.pop(0)
            print(f"[Consumer {consumer_id}] Consumed: {item} | Buffer size: {len(self.buffer)}")

        # Signal that an empty slot is available
        self.empty_slots.release()
        return item


class Producer(threading.Thread):
    def __init__(self, buffer, producer_id):
        super().__init__()
        self.buffer = buffer
        self.producer_id = producer_id

    def run(self):
        while True:
            item = random.randint(1, 100)
            time.sleep(random.random() * 0.5)
            self.buffer.put(item, self.producer_id)


class Consumer(threading.Thread):
    def __init__(self, buffer, consumer_id):
        super().__init__()
        self.buffer = buffer
        self.consumer_id = consumer_id

    def run(self):
        while True:
            time.sleep(random.random() * 1.0)
            self.buffer.get(self.consumer_id)


if __name__ == "__main__":
    # Runtime-configurable buffer size
    BUFFER_SIZE = 5
    buffer = BoundedBuffer(BUFFER_SIZE)

    producers = [Producer(buffer, i) for i in range(3)]
    consumers = [Consumer(buffer, i) for i in range(2)]

    print("Starting producer-consumer system...\n")

    for p in producers:
        p.start()

    for c in consumers:
        c.start()

    # Threads run indefinitely; stop manually (Ctrl+C)
