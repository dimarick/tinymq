package serializer

import "io"

type CountedReader struct {
	reader    io.Reader
	countRead int64
}

func NewCountedReader(r io.Reader) *CountedReader {
	reader := new(CountedReader)

	reader.reader = r
	reader.countRead = 0

	return reader
}

func (r *CountedReader) Read(p []byte) (n int, err error) {
	read, err := r.reader.Read(p)
	if err != nil {
		return 0, err
	}

	r.countRead += int64(read)

	return read, nil
}

func (r *CountedReader) Count() int64 {
	return r.countRead
}

func (r *CountedReader) ResetCount() {
	r.countRead = 0
}
