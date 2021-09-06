package cachelog

import (
	"bytes"
	"math/rand"
	"os"
	"os/exec"
	"runtime"
	"testing"
)

func TestCache(t *testing.T) {
	bug = t.Errorf
	tmp := t.TempDir()
	c, err := Open(tmp, nil)
	if err != nil {
		t.Fatalf("Failed to initialize empty cache: %v", err)
	}
	buf := make([]byte, 1024)
	missing, err := c.Get([]byte("test.txt"), 0, buf)
	if err != nil {
		t.Errorf("Failed to Get() on empty cache: %v", err)
	}
	if len(missing) != 1 {
		t.Errorf("Get(): unexpected number of missing ranges: %d, want 1", len(missing))
	}
	if missing[0].Start != 0 || missing[0].End != 1024 {
		t.Errorf("Get(): incorrect missing Entry: [%d, %d] want [0, 1024]", missing[0].Start, missing[0].End)
	}

	for i := 0; len(buf) > i; i++ {
		buf[i] = byte(i % 256)
	}
	if err := c.Put([]byte("test.txt"), 512, buf); err != nil {
		t.Fatalf("Put() failed: %v", err)
	}
	buf = make([]byte, 1024)
	missing, err = c.Get([]byte("test.txt"), 0, buf)
	if err != nil {
		t.Errorf("Failed to Get() on empty cache: %v", err)
	}
	if len(missing) != 1 {
		t.Errorf("Get(): unexpected number of missing ranges: %d, want 1", len(missing))
	}
	if missing[0].Start != 0 || missing[0].End != 512 {
		t.Errorf("Get(): incorrect missing Entry: [%d, %d] want [0, 512]", missing[0].Start, missing[0].End)
	}
	for i := int(missing[0].End); len(buf) > i; i++ {
		if buf[i] != byte(i%256) {
			t.Errorf("Put()+Get() returned incorrect data at buf offset %d", i)
			break
		}
	}

	if err := c.Delete([]byte("test.txt"), 768, 64); err != nil {
		t.Fatalf("Put() failed: %v", err)
	}

	c = nil
	runtime.GC() // Ensure os.File's get closed.

	c, err = Open(tmp, nil)
	if err != nil {
		t.Fatalf("Failed to open cache: %v", err)
	}
	buf = make([]byte, 1024)
	missing, err = c.Get([]byte("test.txt"), 0, buf)
	if err != nil {
		t.Errorf("Failed to Get() on empty cache: %v", err)
	}
	if len(missing) != 2 {
		t.Errorf("Get(): unexpected number of missing ranges: %d, want 2", len(missing))
	}
	if missing[0].Start != 0 || missing[0].End != 512 {
		t.Errorf("Get(): incorrect missing Entry: [%d, %d] want [0, 512]", missing[0].Start, missing[0].End)
	}
	if missing[1].Start != 768 || missing[1].End != 768+64 {
		t.Errorf("Get(): incorrect missing Entry: [%d, %d] want [768, 832]", missing[1].Start, missing[1].End)
	}
	for i := int(missing[0].End); len(buf) > i; i++ {
		if i == int(missing[1].Start) {
			i = int(missing[1].End)
		}
		if buf[i] != byte(i%256) {
			t.Errorf("Put()+Get() returned incorrect data at buf offset %d", i)
			break
		}
	}
}

func TestChaos(t *testing.T) {
	bug = t.Errorf
	files := [][]byte{
		make([]byte, 1024*1024),
		make([]byte, 2*1024*1024),
	}
	fileNames := []string{
		"file1.txt",
		"filetwo.txt",
	}
	tmp := t.TempDir()
	cfg := &Config{
		MaxGarbageRatio: 0.95,
		LogFileSize:     4 * 1024 * 1024,
	}
	c, err := Open(tmp, cfg)
	if err != nil {
		t.Fatalf("Failed to initialize empty cache: %v", err)
	}
	for i := 0; 10000 > i; i++ {
		if i%500 == 0 {
			c = nil
			runtime.GC() // Ensure os.File's get closed.
			c, err = Open(tmp, cfg)
			if err != nil {
				t.Fatalf("Failed to open cache: %v", err)
			}
		}
		f := rand.Intn(len(fileNames))
		fn := fileNames[f]
		fd := files[f]
		switch rand.Intn(3) {
		case 0:
			o := rand.Intn(len(fd))
			s := rand.Intn((len(fd) - o) / 4)
			buf := make([]byte, s)
			rand.Read(buf)
			if err := c.Put([]byte(fn), int64(o), buf); err != nil {
				t.Fatalf("Put() failed: %v", err)
			}
			copy(fd[o:], buf)
		case 1:
			o := rand.Intn(len(fd))
			s := rand.Intn((len(fd) - o) / 4)
			if err := c.Delete([]byte(fn), int64(o), s); err != nil {
				t.Fatalf("Delete() failed: %v", err)
			}
			copy(fd[o:], make([]byte, s))
		case 2:
			o := rand.Intn(len(fd))
			s := rand.Intn(len(fd) - o)
			buf := make([]byte, s)
			_, err := c.Get([]byte(fn), int64(o), buf)
			if err != nil {
				t.Fatalf("Get() failed: %v", err)
			}
			if !bytes.Equal(buf, fd[o:o+s]) {
				for j := 0; len(buf) > j; j++ {
					if buf[j] != fd[o+j] {
						t.Fatalf("Get(%s, %d, %d) returned wrong data at offset %d: cache: %d; check: %d", fn, o, s, o+j, buf[j], fd[o+j])
					}
				}
				t.Fatalf("Get(%s, %d, %d) returned wrong data", fn, o, s)
			}
		}
	}
	cmd := exec.Command("find", tmp, "-ls")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Run()
}
