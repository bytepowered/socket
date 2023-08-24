package socket

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
)

const (
	FrameLengthFieldLength1 = 1
	FrameLengthFieldLength2 = 2
	FrameLengthFieldLength4 = 4
	FrameLengthFieldLength8 = 8
)

type (
	SocketFrameDecoder func(con net.Conn, buffer *bytes.Buffer) (err error)
)

func NewLengthBasedFrameDecoder(order binary.ByteOrder,
	lengthFieldOffset int,
	lengthFieldLength uint8,
	lengthAdjustment int,
	initialBytesToStrip int,
) SocketFrameDecoder {
	switch lengthFieldLength {
	case FrameLengthFieldLength1, FrameLengthFieldLength2, FrameLengthFieldLength4, FrameLengthFieldLength8:
	default:
		panic(fmt.Sprintf("unsupported lengthFieldLength: %d, expected: 1,2,4,8", lengthFieldLength))
	}
	read := func(reader io.Reader, num int, buffer []byte) error {
		if n, err := reader.Read(buffer); err != nil {
			return fmt.Errorf("socket read length error: %w", err)
		} else if n != int(num) {
			return fmt.Errorf("socket read length not match, expect: %d, was: %d", num, n)
		}
		return nil
	}
	return func(conn net.Conn, buffer *bytes.Buffer) (err error) {
		// offset
		offsetBuf := make([]byte, lengthFieldOffset)
		if lengthFieldOffset > 0 {
			if err := read(conn, int(lengthFieldOffset), offsetBuf); err != nil {
				return err
			}
			buffer.Write(offsetBuf)
		}
		// length
		lengthBuf := make([]byte, lengthFieldLength)
		if err := read(conn, int(lengthFieldLength), lengthBuf); err != nil {
			return err
		}
		buffer.Write(lengthBuf)

		// init bytes strip
		if initialBytesToStrip > 0 {
			for i := 0; i < initialBytesToStrip; i++ {
				_, _ = buffer.ReadByte()
			}
		}

		actualLength := ReadFrameLength(order, lengthBuf, lengthFieldLength)
		if actualLength == 0 {
			return nil
		}
		if actualLength < 0 {
			return fmt.Errorf("socket read unexpected length, expected: %d, was: %d", lengthFieldLength, actualLength)
		}
		// frame
		actualFrameLength := actualLength + int64(lengthAdjustment)
		if actualFrameLength == 0 {
			return nil
		} else {
			if n, cerr := io.CopyN(buffer, conn, actualFrameLength); cerr != nil {
				return cerr
			} else if n != actualFrameLength {
				return fmt.Errorf("socket read length not match, expect: %d, was: %d", actualFrameLength, n)
			}
			return nil
		}
	}
}

func ReadFrameLength(order binary.ByteOrder, buffer []byte, length uint8) int64 {
	switch length {
	case FrameLengthFieldLength1:
		return int64(buffer[0])
	case FrameLengthFieldLength2:
		return int64(order.Uint16(buffer))
	case FrameLengthFieldLength4:
		return int64(order.Uint32(buffer))
	case FrameLengthFieldLength8:
		return int64(order.Uint64(buffer))
	default:
		return -1
	}
}
