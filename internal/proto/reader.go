package proto

import (
	"bufio"
	"fmt"
	"io"
	"math"
	"math/big"
	"strconv"

	"github.com/go-redis/redis/v8/internal/util"
)

const (
	RespStatus    = '+' // +<string>\r\n
	RespError     = '-' // -<string>\r\n
	RespString    = '$' // $<length>\r\n<bytes>\r\n
	RespInteger   = ':' // :<number>\r\n
	RespNil       = '_' // _\r\n
	RespFloat     = ',' // ,<floating-point-number>\r\n (golang float)
	RespBool      = '#' // true: #t\r\n false: #f\r\n
	RespBlobError = '!' // !<length>\r\n<bytes>\r\n
	RespVerb      = '=' // =<length>\r\nFORMAT:<bytes>\r\n
	RespBigInt    = '(' // (<big number>\r\n
	RespArray     = '*' // *<len>\r\n... (same as resp2)
	RespMap       = '%' // %<len>\r\n(key)\r\n(value)\r\n... (golang map)
	RespSet       = '~' // ~<len>\r\n... (same as Array)
	RespAttr      = '|' // |<len>\r\n(key)\r\n(value)\r\n... + command reply
	RespPush      = '>' // ><len>\r\n... (same as Array)
)

// Not used temporarily.
// Redis has not used these two data types for the time being, and will implement them later.
// Streamed           = "EOF:"
// StreamedAggregated = '?'

//------------------------------------------------------------------------------

const Nil = RedisError("redis: nil")

type RedisError string

func (e RedisError) Error() string { return string(e) }

func (RedisError) RedisError() {}

//------------------------------------------------------------------------------

var (
	uvinf    = math.Inf(1)
	uvneginf = math.Inf(-1)
)

type (
	AggregateBulkParse func(*Reader, byte, int64) (interface{}, error)
	MultiBulkParse     func(*Reader, int64) (interface{}, error)
)

type Reader struct {
	rd *bufio.Reader
}

func NewReader(rd io.Reader) *Reader {
	return &Reader{
		rd: bufio.NewReader(rd),
	}
}

func (r *Reader) Buffered() int {
	return r.rd.Buffered()
}

func (r *Reader) Peek(n int) ([]byte, error) {
	return r.rd.Peek(n)
}

func (r *Reader) Reset(rd io.Reader) {
	r.rd.Reset(rd)
}

// PeekReplyType returns the data type of the next response without advancing the Reader,
// and discard the attribute type.
func (r *Reader) PeekReplyType() (byte, error) {
	b, err := r.rd.Peek(1)
	if err != nil {
		return 0, err
	}
	if b[0] == RespAttr {
		if err = r.Discard(nil); err != nil {
			return 0, err
		}
	}
	return b[0], nil
}

// ReadLine Return a valid reply, it will check the protocol or redis error,
// and discard the attribute type.
func (r *Reader) ReadLine() ([]byte, error) {
	line, err := r.readLine()
	if err != nil {
		return nil, err
	}
	switch line[0] {
	case RespError:
		return nil, ParseErrorReply(line)
	case RespNil:
		return nil, Nil
	case RespBlobError:
		var blobErr string
		blobErr, err = r.readStringReply(line)
		if err == nil {
			err = RedisError(blobErr)
		}
		return nil, err
	case RespAttr:
		if err = r.Discard(line); err != nil {
			return nil, err
		}
		return r.ReadLine()
	}

	// Compatible with RESP2
	if IsNilReply(line) {
		return nil, Nil
	}

	return line, nil
}

// readLine that returns an error if:
//   - there is a pending read error;
//   - or line does not end with \r\n.
func (r *Reader) readLine() ([]byte, error) {
	b, err := r.rd.ReadSlice('\n')
	if err != nil {
		if err != bufio.ErrBufferFull {
			return nil, err
		}

		full := make([]byte, len(b))
		copy(full, b)

		b, err = r.rd.ReadBytes('\n')
		if err != nil {
			return nil, err
		}

		full = append(full, b...) //nolint:makezero
		b = full
	}
	if len(b) <= 2 || b[len(b)-1] != '\n' || b[len(b)-2] != '\r' {
		return nil, fmt.Errorf("redis: invalid reply: %q", b)
	}
	return b[:len(b)-2], nil
}

// ReadSimpleReply read a single type of response, other types will return an error.
func (r *Reader) ReadSimpleReply() (interface{}, error) {
	line, err := r.ReadLine()
	if err != nil {
		return nil, err
	}

	switch line[0] {
	case RespStatus:
		return string(line[1:]), nil
	case RespInteger:
		return util.ParseInt(line[1:], 10, 64)
	case RespFloat:
		return r.readFloat(line)
	case RespBool:
		return r.readBool(line)
	case RespBigInt:
		return r.readBigInt(line)

	case RespString:
		return r.readStringReply(line)
	case RespVerb:
		return r.readVerb(line)
	}
	return nil, fmt.Errorf("redis: can't parse simple type: %q", line)
}

func (r *Reader) readFloat(line []byte) (float64, error) {
	v := string(line[1:])
	switch string(line[1:]) {
	case "inf":
		return uvinf, nil
	case "-inf":
		return uvneginf, nil
	}
	return strconv.ParseFloat(v, 64)
}

func (r *Reader) readBool(line []byte) (bool, error) {
	switch string(line[1:]) {
	case "t":
		return true, nil
	case "f":
		return false, nil
	}
	return false, fmt.Errorf("redis: can't parse bool reply: %q", line)
}

func (r *Reader) readBigInt(line []byte) (*big.Int, error) {
	i := new(big.Int)
	if i, ok := i.SetString(string(line[1:]), 10); ok {
		return i, nil
	}
	return nil, fmt.Errorf("redis: can't parse bigInt reply: %q", line)
}

func (r *Reader) readStringReply(line []byte) (string, error) {
	n, err := replyLen(line)
	if err != nil {
		return "", err
	}

	b := make([]byte, n+2)
	_, err = io.ReadFull(r.rd, b)
	if err != nil {
		return "", err
	}

	return util.BytesToString(b[:n]), nil
}

func (r *Reader) readVerb(line []byte) (string, error) {
	s, err := r.readStringReply(line)
	if err != nil {
		return "", err
	}
	if len(s) < 4 || s[3] != ':' {
		return "", fmt.Errorf("redis: can't parse verbatim string reply: %q", line)
	}
	return s[4:], nil
}

// -------------------------------

func (r *Reader) ReadInt() (int64, error) {
	line, err := r.ReadLine()
	if err != nil {
		return 0, err
	}
	switch line[0] {
	case RespInteger, RespStatus:
		return util.ParseInt(line[1:], 10, 64)
	case RespString:
		s, err := r.readStringReply(line)
		if err != nil {
			return 0, err
		}
		return util.ParseInt([]byte(s), 10, 64)
	case RespBigInt:
		b, err := r.readBigInt(line)
		if err != nil {
			return 0, err
		}
		if !b.IsInt64() {
			return 0, fmt.Errorf("bigInt(%s) value out of range", b.String())
		}
		return b.Int64(), nil
	}
	return 0, fmt.Errorf("redis: can't parse int reply: %.100q", line)
}

func (r *Reader) ReadFloat() (float64, error) {
	line, err := r.ReadLine()
	if err != nil {
		return 0, err
	}
	switch line[0] {
	case RespFloat:
		return r.readFloat(line)
	case RespStatus:
		return strconv.ParseFloat(string(line[1:]), 64)
	case RespString:
		s, err := r.readStringReply(line)
		if err != nil {
			return 0, err
		}
		return strconv.ParseFloat(s, 64)
	}
	return 0, fmt.Errorf("redis: can't parse float reply: %.100q", line)
}

func (r *Reader) ReadString() (string, error) {
	line, err := r.ReadLine()
	if err != nil {
		return "", err
	}

	switch line[0] {
	case RespStatus, RespInteger, RespFloat:
		return string(line[1:]), nil
	case RespString:
		return r.readStringReply(line)
	case RespBool:
		b, err := r.readBool(line)
		return strconv.FormatBool(b), err
	case RespVerb:
		return r.readVerb(line)
	case RespBigInt:
		b, err := r.readBigInt(line)
		if err != nil {
			return "", err
		}
		return b.String(), nil
	}
	return "", fmt.Errorf("redis: can't parse reply=%.100q reading string", line)
}

func (r *Reader) ReadBool() (bool, error) {
	s, err := r.ReadString()
	if err != nil {
		return false, err
	}
	return s == "OK" || s == "1" || s == "true", nil
}

// ReadFixedArrayLen read fixed array length.
func (r *Reader) ReadFixedArrayLen(fixedLen int) error {
	n, err := r.ReadArrayLen()
	if err != nil {
		return err
	}
	if n != fixedLen {
		return fmt.Errorf("redis: got %d elements of array length, wanted %d", n, fixedLen)
	}
	return nil
}

// ReadArrayLen Read and return the length of the array.
func (r *Reader) ReadArrayLen() (int, error) {
	line, err := r.ReadLine()
	if err != nil {
		return 0, err
	}
	switch line[0] {
	case RespArray, RespSet, RespPush:
		return replyLen(line)
	default:
		return 0, fmt.Errorf("redis: can't parse array(array/set/push) reply: %.100q", line)
	}
}

// ReadFixedMapLen read fixed map length.
func (r *Reader) ReadFixedMapLen(fixedLen int) error {
	n, err := r.ReadMapLen()
	if err != nil {
		return err
	}
	if n != fixedLen {
		return fmt.Errorf("redis: got %d elements of map length, wanted %d", n, fixedLen)
	}
	return nil
}

// ReadMapLen read the length of the map type.
// If responding to the array type (RespArray/RespSet/RespPush),
// it must be a multiple of 2 and return n/2.
// Other types will return an error.
func (r *Reader) ReadMapLen() (int, error) {
	line, err := r.ReadLine()
	if err != nil {
		return 0, err
	}
	switch line[0] {
	case RespMap:
		return replyLen(line)
	case RespArray, RespSet, RespPush:
		// Some commands and RESP2 protocol may respond to array types.
		n, err := replyLen(line)
		if err != nil {
			return 0, err
		}
		if n%2 != 0 {
			return 0, fmt.Errorf("redis: the length of the array must be a multiple of 2, got: %d", n)
		}
		return n / 2, nil
	default:
		return 0, fmt.Errorf("redis: can't parse map reply: %.100q", line)
	}
}

// Discard the data represented by line, if line is nil, read the next line.
func (r *Reader) Discard(line []byte) (err error) {
	if len(line) == 0 {
		line, err = r.readLine()
		if err != nil {
			return err
		}
	}
	switch line[0] {
	case RespStatus, RespError, RespInteger, RespNil, RespFloat, RespBool, RespBigInt:
		return nil
	}

	n, err := replyLen(line)
	if err != nil && err != Nil {
		return err
	}

	switch line[0] {
	case RespBlobError, RespString, RespVerb:
		// +\r\n
		_, err = r.rd.Discard(n + 2)
		return err
	case RespArray, RespSet, RespPush:
		for i := 0; i < n; i++ {
			if err = r.Discard(nil); err != nil {
				return err
			}
		}
		return nil
	case RespMap, RespAttr:
		// Read key & value.
		for i := 0; i < n*2; i++ {
			if err = r.Discard(nil); err != nil {
				return err
			}
		}
		return nil
	}

	return fmt.Errorf("redis: can't parse %.100q", line)
}

func replyLen(line []byte) (n int, err error) {
	n, err = util.Atoi(line[1:])
	if err != nil {
		return 0, err
	}

	if n < -1 {
		return 0, fmt.Errorf("redis: invalid reply: %q", line)
	}

	switch line[0] {
	case RespString, RespVerb, RespBlobError,
		RespArray, RespSet, RespPush, RespMap, RespAttr:
		if n == -1 {
			return 0, Nil
		}
	}
	return n, nil
}

// IsNilReply detect redis.Nil of RESP2.
func IsNilReply(line []byte) bool {
	return len(line) == 3 &&
		(line[0] == RespString || line[0] == RespArray) &&
		line[1] == '-' && line[2] == '1'
}

func ParseErrorReply(line []byte) error {
	return RedisError(line[1:])
}
