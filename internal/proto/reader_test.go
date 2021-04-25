package proto_test

import (
	"bytes"
	"github.com/go-redis/redis/v8"
	"io"
	"testing"

	"github.com/go-redis/redis/v8/internal/proto"
)

func BenchmarkReader_ParseReply_Status(b *testing.B) {
	benchmarkParseReply(b, "+OK\r\n", false)
}

func BenchmarkReader_ParseReply_Int(b *testing.B) {
	benchmarkParseReply(b, ":1\r\n", false)
}

func BenchmarkReader_ParseReply_Float(b *testing.B) {
	benchmarkParseReply(b, ",123.456\r\n", false)
}

func BenchmarkReader_ParseReply_Bool(b *testing.B) {
	benchmarkParseReply(b, "#t\r\n", false)
}

func BenchmarkReader_ParseReply_BigInt(b *testing.B) {
	benchmarkParseReply(b, "(3492890328409238509324850943850943825024385\r\n", false)
}

func BenchmarkReader_ParseReply_Error(b *testing.B) {
	benchmarkParseReply(b, "-Error message\r\n", true)
}

func BenchmarkReader_ParseReply_Nil(b *testing.B) {
	benchmarkParseReply(b, "_\r\n", true)
}

func BenchmarkReader_ParseReply_BlobError(b *testing.B) {
	benchmarkParseReply(b, "!21\r\nSYNTAX invalid syntax", true)
}

func BenchmarkReader_ParseReply_String(b *testing.B) {
	benchmarkParseReply(b, "$5\r\nhello\r\n", false)
}

func BenchmarkReader_ParseReply_Verb(b *testing.B) {
	benchmarkParseReply(b, "$9\r\ntxt:hello\r\n", false)
}

func BenchmarkReader_ParseReply_Slice(b *testing.B) {
	benchmarkParseReply(b, "*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n", false)
}

func BenchmarkReader_ParseReply_Set(b *testing.B) {
	benchmarkParseReply(b, "~2\r\n$5\r\nhello\r\n$5\r\nworld\r\n", false)
}

func BenchmarkReader_ParseReply_Push(b *testing.B) {
	benchmarkParseReply(b, ">2\r\n$5\r\nhello\r\n$5\r\nworld\r\n", false)
}

func BenchmarkReader_ParseReply_Map(b *testing.B) {
	benchmarkParseReply(b, "%2\r\n$5\r\nhello\r\n$5\r\nworld\r\n+key\r\n+value\r\n", false)
}

func BenchmarkReader_ParseReply_Attr(b *testing.B) {
	benchmarkParseReply(b, "%1\r\n+key\r\n+value\r\n+hello\r\n", false)
}

func TestReader_ReadLine(t *testing.T) {
	original := bytes.Repeat([]byte("a"), 8192)
	original[len(original)-2] = '\r'
	original[len(original)-1] = '\n'
	r := proto.NewReader(bytes.NewReader(original))
	read, err := r.ReadLine()
	if err != nil && err != io.EOF {
		t.Errorf("Should be able to read the full buffer: %v", err)
	}

	if bytes.Compare(read, original[:len(original)-2]) != 0 {
		t.Errorf("Values must be equal: %d expected %d", len(read), len(original[:len(original)-2]))
	}
}

func benchmarkParseReply(b *testing.B, reply string, wanterr bool) {
	buf := new(bytes.Buffer)
	for i := 0; i < b.N; i++ {
		buf.WriteString(reply)
	}
	p := proto.NewReader(buf)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := replyParse(p)
		if !wanterr && err != nil {
			b.Fatal(err)
		}
	}
}

func replyParse(rd *proto.Reader) (interface{}, error) {
	typ, err := rd.PeekReplyType()
	if err != nil {
		return nil, err
	}
	switch typ {
	case proto.RespArray, proto.RespSet, proto.RespPush:
		return sliceParser(rd)
	case proto.RespMap:
		return mapParse(rd)
	default:
		return rd.ReadSimpleReply()
	}
}

func sliceParser(rd *proto.Reader) ([]interface{}, error) {
	n, err := rd.ReadArrayLen()
	if err != nil {
		return nil, err
	}
	val := make([]interface{}, n)
	for i := 0; i < len(val); i++ {
		v, err := replyParse(rd)
		if err != nil {
			if err == redis.Nil {
				val[i] = nil
				continue
			}
			if err, ok := err.(proto.RedisError); ok {
				val[i] = err
				continue
			}
			return nil, err
		}
		val[i] = v
	}
	return val, nil
}

func mapParse(rd *proto.Reader) (map[interface{}]interface{}, error) {
	n, err := rd.ReadMapLen()
	if err != nil {
		return nil, err
	}
	m := make(map[interface{}]interface{}, n)
	for i := 0; i < n; i++ {
		k, err := replyParse(rd)
		if err != nil {
			return nil, err
		}
		v, err := replyParse(rd)
		if err != nil {
			if err == redis.Nil {
				m[k] = nil
				continue
			}
			if err, ok := err.(proto.RedisError); ok {
				m[k] = err
				continue
			}
			return nil, err
		}
		m[k] = v
	}
	return m, nil
}
