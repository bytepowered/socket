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

func NewLengthBasedFrameDecoder(order binary.ByteOrder, lengthFieldLength uint8, lengthAdjustment int) SocketFrameDecoder {
	switch lengthFieldLength {
	case 1, 2, 4, 8:
	default:
		panic(fmt.Sprintf("unsupported lengthFieldLength: %d, expected: 1,2,4,8", lengthFieldLength))
	}
	return func(conn net.Conn, buffer *bytes.Buffer) (err error) {
		buff := make([]byte, lengthFieldLength)
		if n, err := conn.Read(buff); err != nil {
			return fmt.Errorf("socket read length error: %w", err)
		} else if n != int(lengthFieldLength) {
			return fmt.Errorf("socket read length not match, expect: %d, was: %d", lengthFieldLength, n)
		}
		length := ReadFrameLength(order, buff, lengthFieldLength)
		if length <= 0 {
			return fmt.Errorf("socket read unsupported lengthFieldLength: %d", lengthFieldLength)
		}
		frameLength := length + int64(lengthAdjustment)
		if frameLength == 0 {
			return nil
		} else {
			if n, cerr := io.CopyN(buffer, conn, frameLength); cerr != nil {
				return cerr
			} else if n != frameLength {
				return fmt.Errorf("socket read length not match, expect: %d, was: %d", frameLength, n)
			}
			return nil
		}
	}
}

func ReadFrameLength(order binary.ByteOrder, buffer []byte, length uint8) int64 {
	switch length {
	case 1:
		return int64(buffer[0])
	case 2:
		return int64(order.Uint16(buffer))
	case 4:
		return int64(order.Uint32(buffer))
	case 8:
		return int64(order.Uint64(buffer))
	default:
		return -1
	}
}
