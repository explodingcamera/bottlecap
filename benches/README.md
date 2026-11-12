## Performance

![Per-IP rate limiting across eight threads](./key-count.svg)

![Per-IP rate limiting on one thread](./key-count-single-thread.svg)

Measured on Linux with an AMD Ryzen 7 5800X with default clocks + the same fixed-seed rapidhash.
Keys are mixed IPv4/IPv6 addresses. Every fifth request targets one hot client.
