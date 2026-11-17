# Track D — Causal Group Chat, Mutual Exclusion & Leader Election

## Tổ chức **theo đề bài**

- `modules/student1/causal_multicast.py`: SV1 — Lamport Clock, Vector Clock, causal multicast, hold-back và causal delivery; giữ lại transport cơ bản từ bài thực hành.
- `modules/student1/chat_service.py`: SV1 — lớp dịch vụ/callback từ bài thực hành; kế thừa thuật toán causal.
- `modules/student2/failure_detector_election.py`: SV2 — heartbeat, failure detector, Bully election.
- `modules/student3/ricart_agrawala.py`: SV3 — thuật toán Ricart–Agrawala, REQUEST/REPLY, deferred replies và Critical Section.
- `app/peer_node.py`: **tầng tích hợp chung**, không thuộc riêng SV1: `CombinedNode`, adapter cho MUTEX và bộ phân tuyến gói tin qua TCP Server duy nhất. Tầng này hiện tái sử dụng TCP listener của ChatService.
- `main.py`: **entry point ngắn**, chỉ phân tích CLI và gọi `app.peer_node.main`.
- `shared_counter.txt`: tài nguyên mẫu; tất cả Node trên cùng máy phải đọc ghi đúng **một file chung**.
- `reset_counter.py`: đặt lại bộ đếm trước demo.
- `tests/test_system.py`: kiểm thử tự động bốn tiến trình.

## Chạy (terminal mở tại thư mục gốc)

```powershell
python reset_counter.py
python main.py --id 1
python main.py --id 2
python main.py --id 3
python main.py --id 4
```

Các lệnh khi chương trình chạy: `chat <text>`, `delay <id> <seconds>`, `election`, `status`, `cs [seconds]`, `inc [seconds]`, `read`, `exit`.

Chạy bộ kiểm thử khi **đã đóng** bốn Node thủ công:

```powershell
python tests/test_system.py
```

## Kiến trúc

`main.py` → `app/peer_node.py` (CombinedNode) → 3 modules.

`CombinedNode` kế thừa `CausalChatService` (SV1) và chứa `self.election` (SV2), `self.mutex` (SV3). `SharedTransportMutex` là adapter của tầng tích hợp, ghi đè cách gửi thông điệp để dùng cùng TCP Server; **không chạy** hai TCP Server riêng của SV2/SV3.

## Đối chiếu đề bài và hạn chế

- SV1: Lamport + Vector Clocks, hold-back queue và causal multicast.
- SV2: heartbeat + Bully, bầu lại Leader khi mất Node.
- SV3: Ricart–Agrawala, không dùng central lock server.
- **P2P 4 Node, một listener/Node**; đang thử trên TCP localhost chứ chưa phải nhiều máy.
- Mẫu shared whiteboard/file dùng `shared_counter.txt`, một **file trên cùng filesystem**. Đây là minh họa shared file, chưa phải whiteboard chia sẻ qua nhiều máy.
- **Không hỗ trợ** reconfiguration an toàn khi một Node chết: leader có thể thay nhưng CS của SV3 vẫn yêu cầu đủ REPLY từ tập cố định; không tự giảm số Node cấp phép.
- Mã này là bước **sắp xếp lại cấu trúc**, không phải viết lại thuật toán hay hoàn thiện các giới hạn về mất gói, partition, durability, security.

## Bản nâng cấp: file cộng tác thực tế (Ưu tiên 1)

Mỗi Node có thêm lệnh `write <nội dung>` để ghi một dòng vào `shared_whiteboard.txt`.
**Mỗi thao tác ghi bắt buộc phải xin quyền Ricart–Agrawala (REQUEST/REPLY)**.
Lệnh `board` chỉ hiển thị file để quan sát, **không phải đọc có khóa**.

**Chuẩn bị trước khi mở 4 tiến trình:**

```powershell
python reset_counter.py
python reset_board.py
```

Chạy 4 Node như trước:

```powershell
python main.py --id 1
python main.py --id 2
python main.py --id 3
python main.py --id 4
```

Trong mỗi Terminal Node, lần lượt hoặc gần đồng thời chạy:

```text
write Xin chao tu Node 1
write Ghi chu tu Node 2
write Cap nhat tu Node 3
write Tong ket tu Node 4
board
```

Các lệnh viết cần được nhập tại các Node tương ứng; mỗi lệnh `write` sử dụng
`self.mutex.request_cs(work=...)` để thực hiện thao tác ghi trong CS.
`board` có thể đọc khi Node khác đang ghi nên chỉ sử dụng để xem kết quả
**sau khi các thao tác đã hoàn thành**.

Ví dụ file (thứ tự có thể khác tùy lịch cấp khóa):

```text
[Node 1] Xin chao tu Node 1
[Node 2] Ghi chu tu Node 2
[Node 3] Cap nhat tu Node 3
[Node 4] Tong ket tu Node 4
```

Khi Node 4 đã chết, các lệnh `write` mới trên Node khác **sẽ không được thực hiện**
vì thiếu REPLY; không tự hạ yêu cầu quorum. File này được chia sẻ qua **cùng
filesystem trên một máy**, không phải một file phân tán được sao chép giữa nhiều máy.
Chức năng `inc` và `read` cũ vẫn được giữ.

**Kiểm thử:** sau khi đóng hết các Node thủ công:

```powershell
python tests/test_system.py
python tests/test_shared_file.py
```
