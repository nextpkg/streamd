# Streamd 设计导读

Streamd 将输入媒体分发到播放、HLS 和录制模块，各消费者通过独立队列接收数据。服务提供转发与封装，不进行视频转码。

- [架构与数据流](architecture.md)：单机媒体管线、双节点回源、配置与录制边界。
- [内存设计](memory.md)：分块分配、载荷共享、GOP 与 HLS 生命周期。
- [关键设计取舍](design-decisions.md)：GC 压力、共享载荷、回源合并、关键帧切换、异步收尾和 HLS 切片延迟清理。
- [性能与复现](performance.md)：分配策略对照、1C2G 受限容量及稳定性结果。
- [分块大小取舍](slab-sizing.md)：64–512KiB 对 GC CPU 与保留堆的影响。
- [IO 隔离验证记录](io-isolation.md)：慢消费者隔离与断连回收的测试方法和结果。
