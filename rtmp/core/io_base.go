package core

import (
	"bufio"
	"io"

	"github.com/nextpkg/streamd/internal/errutil"
)

// ReadWriter 读写缓冲器
type ReadWriter struct {
	*bufio.ReadWriter
}

// NewReadWriter 为连接的读写函数设置缓冲, bufSize为缓冲器大小, 单位字节
func NewReadWriter(rw io.ReadWriter, bufSize int) *ReadWriter {
	// Reader 使用 bufio 的最小缓冲尺寸；Writer 在 bufSize <= 0 时使用默认尺寸。
	r := bufio.NewReaderSize(rw, bufSize)
	w := bufio.NewWriterSize(rw, bufSize)

	return &ReadWriter{
		ReadWriter: bufio.NewReadWriter(r, w),
	}
}

// Read 读数据
func (rw *ReadWriter) Read(p []byte) (int, error) {
	return io.ReadFull(rw.Reader, p)
}

// ReadUintBE 从已有的缓冲池中读取n字节的数据, 以大端的形式保存到uint32中
func (rw *ReadWriter) ReadUintBE(n int) (uint32, error) {
	// 只接收最多4个字节的大小, 以uint32存储
	if n > 4 {
		return 0, errutil.New("uint32 overflow")
	}

	// 读取n个字节, 以大端的形式保持到uint32中
	ret := uint32(0)
	for i := 0; i < n; i++ {
		b, err := rw.ReadByte()
		if err != nil {
			return 0, err
		}

		ret = ret<<8 + uint32(b)
	}

	return ret, nil
}

// ReadUintLE 从已有的缓冲池中读取n字节的数据, 以小端的形式保存到uint32中
func (rw *ReadWriter) ReadUintLE(n int) (uint32, error) {
	// 只接收最多4个字节的大小, 以uint32存储
	if n > 4 {
		return 0, errutil.New("uint32 overflow")
	}

	// 读取n个字节, 以小端的形式保持到uint32中
	ret := uint32(0)
	for i := 0; i < n; i++ {
		b, err := rw.ReadByte()
		if err != nil {
			return 0, err
		}

		ret += uint32(b) << uint32(i*8)
	}

	return ret, nil
}

// ============================================

// WriteUintBE 将v以大端的形式, 写入n个字节到缓冲中
func (rw *ReadWriter) WriteUintBE(v uint32, n int) error {
	// 因为v是uint32类型, 所以最多写入4个字节
	if n > 4 {
		return errutil.New("uint32 overflow")
	}

	// 将v以大端的形式, 写入n个字节到缓冲中
	for i := 0; i < n; i++ {
		b := byte(v>>uint32((n-i-1)<<3)) & 0xff

		err := rw.WriteByte(b)
		if err != nil {
			return errutil.Wrap(err, "write byte failed")
		}
	}

	return nil
}

// WriteUintLE 将v以小端的形式, 写入n个字节到缓冲中
func (rw *ReadWriter) WriteUintLE(v uint32, n int) error {
	// 因为v是uint32类型, 所以最多写入4个字节
	if n > 4 {
		return errutil.New("uint32 overflow")
	}

	// 将v以小端的形式, 写入n个字节到缓冲中
	for i := 0; i < n; i++ {
		b := byte(v) & 0xff

		err := rw.WriteByte(b)
		if err != nil {
			return errutil.Wrap(err, "write byte failed")
		}

		v = v >> 8
	}

	return nil
}
