# 媒体处理的设计取舍

本文介绍媒体处理中的分配、等待和生命周期管理，架构总览见 [architecture.md](architecture.md)，数据见 [performance.md](performance.md)。

## 1. 小包分块分配

一条流不断产生音频包、视频包和控制消息，多路并发会放大堆分配速率；运行时分配、垃圾回收及必要的 GC assist 都可能挤占媒体处理 CPU。分块分配用于减少逐包申请的开销。

Streamd 按连接持有 slab，小包从默认 256KiB 的块里顺序切出；大于块上限的包独立申请。若剩余空间放不下当前包，但仍大于 64KiB 保留阈值，则让当前包独立申请，留着尾部给后续小包，避免一次偶发大帧浪费整段余量。

例如当前块还剩 100KiB，遇到 120KiB 包：本次独立申请，下一批 1KiB 小包仍可使用那 100KiB；若只剩 20KiB，则切换新块。旧块不会被覆盖，它会等所有切片引用消失后由 GC 回收。

Go 的 GC 频率受分配字节速率、存活堆、指针结构、GOGC 和内存限制影响。媒体字节数组不含内部指针，扫描成本与包含指针的对象不同。[等量工作测量](performance.md)中，小包分配次数和总耗时下降，累计分配字节基本相同；GC CPU 估计值下降约 8.6%，只占总耗时差的一部分。

块过大还可能让一个小切片留住整块内存，内存限额过紧时反而增加 GC 压力。选块大小应结合包长分布和真实持有时间，同时测量 CPU profile、GC CPU/周期、分配字节速率与存活堆。

代码：[`Slab.Get`](../rtmp/core/slab.go)；机制依据：[Go GC 指南](https://go.dev/doc/gc-guide)。

## 2. 共享媒体载荷，分别维护时间戳

同一帧要发给多个观众，若每个观众复制整段音视频字节，复制和分配开销会随观众数增长；但直接让所有消费者修改同一个 Packet，又会使重连、时间戳调整互相干扰。

`Funnel.processor` 先浅复制 Packet，再计算该消费者的 Baseline；Data/Media 仍引用同一份媒体载荷。各输出端使用自己的时间戳完成网络协议封装。

这要求共享载荷保持只读；编码序列头需要独立保存，完成的 HLS 切片需要隔离封装缓冲，因此这两类数据在保存时复制。

代码：[`Funnel.processor`](../rtmp/core/funnel.go)、[`SequenceHeaders.Observe`](../packet/boundary.go)；测试：[`packet/boundary_test.go`](../packet/boundary_test.go)。

## 3. 合并同一流的并发回源请求

多个观众同时请求尚未在本机建立的流，如果每个请求都拨一次上游，会在首播瞬间形成重复连接、重复媒体流和注册竞争。

```mermaid
sequenceDiagram
    participant V1 as 观众 1
    participant B as 本机 Streamd
    participant V2 as 观众 2
    participant A as 上游 Streamd
    V1->>B: 请求同一流
    B->>B: 建立按流 key 的 originFlight
    B->>A: 拨号并建立媒体源
    V2->>B: 请求同一流
    B->>B: 等待已有 flight 完成
    A-->>B: 媒体就绪
    B-->>V1: 加入同一 Stream
    B-->>V2: 加入同一 Stream
```

按流 key 的锁保护检查和 flight 登记，网络拨号放到锁外；其他请求等待 flight 完成，再检查源是否可用并加入。回源失败或节点关闭时，结束本轮等待并返回错误。

这一机制类似 singleflight，合并同一节点、同一流正在进行的回源工作；不同频道独立执行，各节点分别建立自己的回源连接。

代码：[`installOriginWriter`](../rtmp/origin.go)；测试：[`rtmp/origin_regression_test.go`](../rtmp/origin_regression_test.go)、[`scripts/e2e/origin.go`](../scripts/e2e/origin.go)。

## 4. 在关键帧处应用媒体配置

视频的帧间依赖使任意时刻切换 HLS 或录制参数容易留下不可独立解码的片段。系统先提交合法配置，媒体组件在合适的视频关键帧处应用变化；必要时回放编码头，使新输出有正确的启动信息。

配置提交后，管理状态分别报告组件正在等待或已经应用；实际生效时间取决于下一关键帧到达的时刻。

代码：[`hls/stream.go`](../hls/stream.go)、[`record/switcher.go`](../record/switcher.go)；测试：[`hls/boundary_test.go`](../hls/boundary_test.go)、[`hls/reload_test.go`](../hls/reload_test.go)。

## 5. 异步完成录制片段

录制切片停止接收数据后，还需要完成文件关闭或分片上传合并。同步等待这些操作会延迟后续录制，因此切换时将旧片段移交后台完成，并将错误写入组件状态。

录制配置切换层最多保留三个待完成的录制实例；达到上限或新旧目标存在冲突时，进入 `waiting_finalization`。等待完成期间录制可能暂停，其他播放消费者仍由独立队列处理。

组件状态报告待完成任务和错误，便于定位存储延迟与录制暂停。

代码：[`record/finalization.go`](../record/finalization.go)、[`record/storage/finalization.go`](../record/storage/finalization.go)；测试：[`record/finalization_test.go`](../record/finalization_test.go)。

## 6. 延迟清理移出 HLS 播放列表的切片

播放器先获取 m3u8，再请求其中的 TS 切片。如果切片在两次请求之间移出播放列表并被删除，第二次请求就会返回 404。Streamd 会暂时保留移出列表的切片，供已获取旧列表的播放器继续请求。

```mermaid
sequenceDiagram
    participant V as 观众
    participant H as HLS 缓存
    V->>H: 获取包含片段 N 的 m3u8
    H-->>V: 返回列表
    H->>H: N 移出列表，暂时保留
    V->>H: 请求片段 N
    H-->>V: 保留期内返回切片数据
    H->>H: 过期后清理 N
```

切片保留时间根据当前窗口时长、之前的窗口长度和切片时长计算，窗口缩小时仍为旧列表保留取片时间。完成的切片保存为独立字节快照，封装缓冲的后续复用不会修改正在发送的数据。

代价是缓存会暂时大于当前播放列表的大小，保留字节数还受码率影响；超过保留期限的片段将被清理。

代码：[`TsItemCache.trimLocked`](../hls/item/cache.go)；测试：[`hls/item/reload_test.go`](../hls/item/reload_test.go)。

## 相关实现与性能对照

SRS 官方文档已有 [merged read/write 与 GOP 缓存](https://ossrs.io/lts/en-us/docs/v5/doc/low-latency)设计；批量读写旨在摊薄系统调用，而本项目描述的读写分离强调两个方向的独立推进，两者不是同一概念。内存分块、流水线和共享缓冲也有成熟业界先例，参见 [内存设计](memory.md)。

[现有性能数据](performance.md)涵盖本项目的分配器和受限负载测试，尚未包含与 SRS 在相同环境下的性能对照。
