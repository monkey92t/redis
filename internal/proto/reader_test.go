package proto_test

import (
	"bytes"
	"io"
	"testing"

	"github.com/redis/go-redis/v9/internal/proto"
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

	if !bytes.Equal(read, original[:len(original)-2]) {
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
		_, err := p.ReadReply()
		if !wanterr && err != nil {
			b.Fatal(err)
		}
	}
}

//RespStatus    = '+' // +<string>\r\n
//RespError     = '-' // -<string>\r\n
//RespString    = '$' // $<length>\r\n<bytes>\r\n
//RespInt       = ':' // :<number>\r\n
//RespNil       = '_' // _\r\n
//RespFloat     = ',' // ,<floating-point-number>\r\n (golang float)
//RespBool      = '#' // true: #t\r\n false: #f\r\n
//RespBlobError = '!' // !<length>\r\n<bytes>\r\n
//RespVerbatim  = '=' // =<length>\r\nFORMAT:<bytes>\r\n
//RespBigInt    = '(' // (<big number>\r\n
//RespArray     = '*' // *<len>\r\n... (same as resp2)
//RespMap       = '%' // %<len>\r\n(key)\r\n(value)\r\n... (golang map)
//RespSet       = '~' // ~<len>\r\n... (same as Array)
//RespAttr      = '|' // |<len>\r\n(key)\r\n(value)\r\n... + command reply
//RespPush      = '>' // ><len>\r\n... (same as Array)

func TestReadBytes(t *testing.T) {
	// status
	testReadBytes(t, []byte("+OK\r\n"))

	// error
	testReadBytes(t, []byte("-Error message\r\n"))

	// string
	testReadBytes(t, []byte("$5\r\nhello\r\n"))

	// int
	testReadBytes(t, []byte(":1\r\n"))

	// nil
	testReadBytes(t, []byte("_\r\n"))

	// float
	testReadBytes(t, []byte(",123.456\r\n"))

	// bool
	testReadBytes(t, []byte("#t\r\n"))

	// blob error
	testReadBytes(t, []byte("!21\r\nSYNTAX invalid syntax\r\n"))

	// verbatim
	testReadBytes(t, []byte("=12\r\nFORMAT:hello\r\n"))

	// big int
	testReadBytes(t, []byte("(3492890328409238509324850943850943825024385\r\n"))

	// array
	testReadBytes(t, []byte("*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n"))

	// map
	testReadBytes(t, []byte("%2\r\n$5\r\nhello\r\n$5\r\nworld\r\n+key\r\n+value\r\n"))

	// set
	testReadBytes(t, []byte("~2\r\n$5\r\nhello\r\n$5\r\nworld\r\n"))

	// push
	testReadBytes(t, []byte(">2\r\n$5\r\nhello\r\n$5\r\nworld\r\n"))

	// attr
	testReadBytes(t, []byte("|1\r\n+key\r\n+value\r\n+hello\r\n"))

	// 复杂
	testReadBytes(t, []byte("*3\r\n*3\r\n$2\r\np2\r\n$1\r\nr\r\n$2\r\np4\r\n*2\r\n*3\r\n*3\r\n*2\r\n$2\r\nid\r\n:1\r\n*2\r\n$6\r\nlabels\r\n*1\r\n$3\r\npod\r\n*2\r\n$10\r\nproperties\r\n*5\r\n*2\r\n$2\r\nid\r\n$2\r\nx2\r\n*2\r\n$4\r\nname\r\n$4\r\npod2\r\n*2\r\n$2\r\nts\r\n:10241\r\n*2\r\n$5\r\nalive\r\n$4\r\ntrue\r\n*2\r\n$2\r\nfv\r\n$9\r\n3.1415927\r\n$15\r\n[(1), [3], (3)]\r\n*3\r\n*2\r\n$2\r\nid\r\n:3\r\n*2\r\n$6\r\nlabels\r\n*1\r\n$3\r\npod\r\n*2\r\n$10\r\nproperties\r\n*5\r\n*2\r\n$2\r\nid\r\n$2\r\nx4\r\n*2\r\n$4\r\nname\r\n$4\r\npod4\r\n*2\r\n$2\r\nts\r\n:10243\r\n*2\r\n$5\r\nalive\r\n$5\r\nfalse\r\n*2\r\n$2\r\nfv\r\n$9\r\n3.1415929\r\n*3\r\n*3\r\n*2\r\n$2\r\nid\r\n:1\r\n*2\r\n$6\r\nlabels\r\n*1\r\n$3\r\npod\r\n*2\r\n$10\r\nproperties\r\n*5\r\n*2\r\n$2\r\nid\r\n$2\r\nx2\r\n*2\r\n$4\r\nname\r\n$4\r\npod2\r\n*2\r\n$2\r\nts\r\n:10241\r\n*2\r\n$5\r\nalive\r\n$4\r\ntrue\r\n*2\r\n$2\r\nfv\r\n$9\r\n3.1415927\r\n$25\r\n[(1), [0], (0), [2], (3)]\r\n*3\r\n*2\r\n$2\r\nid\r\n:3\r\n*2\r\n$6\r\nlabels\r\n*1\r\n$3\r\npod\r\n*2\r\n$10\r\nproperties\r\n*5\r\n*2\r\n$2\r\nid\r\n$2\r\nx4\r\n*2\r\n$4\r\nname\r\n$4\r\npod4\r\n*2\r\n$2\r\nts\r\n:10243\r\n*2\r\n$5\r\nalive\r\n$5\r\nfalse\r\n*2\r\n$2\r\nfv\r\n$9\r\n3.1415929\r\n*2\r\n$19\r\nCached execution: 1\r\n$52\r\nQuery internal execution time: 0.779132 milliseconds\r\n"))
}

func testReadBytes(t *testing.T, resp []byte) {
	b, err := proto.NewReader(bytes.NewReader(resp)).ReadBytes()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(resp, b) {
		t.Fatalf("expected %q, got %q", string(resp), string(b))
	}
}
