package satellite

import (
	"bufio"
	"bytes"
	"errors"
	"io"
)

const maxFrameBytes = 1 << 20

type sseFrame struct {
	id, event string
	data      []byte
}

var errFrameTooLarge = errors.New("SSE frame exceeds the size limit")

type sseReader struct {
	reader *bufio.Reader
}

func newSSEReader(source io.Reader) *sseReader {
	return &sseReader{reader: bufio.NewReaderSize(source, 64<<10)}
}

// Follows the SSE wire format: fields until a blank line, "data" lines joined by newlines, ":" lines ignored.
func (sse *sseReader) next() (sseFrame, error) {
	frame := sseFrame{event: "message"}
	var data bytes.Buffer
	hasData, size := false, 0
	for {
		line, err := sse.reader.ReadSlice('\n')
		if errors.Is(err, bufio.ErrBufferFull) {
			return sseFrame{}, errFrameTooLarge
		}
		if err != nil && (len(line) == 0 || !errors.Is(err, io.EOF)) {
			return sseFrame{}, err
		}
		size += len(line)
		if size > maxFrameBytes {
			return sseFrame{}, errFrameTooLarge
		}
		line = bytes.TrimRight(line, "\r\n")
		if len(line) == 0 {
			if hasData {
				frame.data = data.Bytes()
				return frame, nil
			}
			frame, size = sseFrame{event: "message"}, 0
			if err != nil {
				return sseFrame{}, err
			}
			continue
		}
		if line[0] == ':' {
			continue
		}
		field, value, _ := bytes.Cut(line, []byte(":"))
		value = bytes.TrimPrefix(value, []byte(" "))
		switch string(field) {
		case "id":
			frame.id = string(value)
		case "event":
			frame.event = string(value)
		case "data":
			if hasData {
				data.WriteByte('\n')
			}
			data.Write(value)
			hasData = true
		}
		if err != nil {
			return sseFrame{}, err
		}
	}
}
