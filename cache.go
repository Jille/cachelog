// Package cachelog is a log structured cache. See the README for details.
package cachelog

import (
	"bufio"
	"bytes"
	"crypto/md5"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Jille/cachelog/ranges"
	"github.com/Jille/dfr"
	"golang.org/x/sync/errgroup"
)

// bug is overwritten by tests to cause a test failure.
var bug = log.Printf

var headerSize = binary.Size(Header{})

type Config struct {
	// Expiry controls how long a block should not be read/written before it is discarded during garbage collection. The expiry is a minimum, the block will remain available until it is actually garbage collected.
	// The default Expiry is 1 week.
	Expiry time.Duration
	// LogFileSize controls how large to make each log file. The default is 10MB. Files might be slightly larger.
	LogFileSize int
	// MaxGarbageRatio is a number between 0 and 1 that indicates the threshold at which to start garbage collection.
	// Don't set this too low to avoid excessive IOPS.
	// The default is 0.75, which starts garbage collection once 75% of the data is dead.
	MaxGarbageRatio float64
}

// Open a new cache, possibly reading in old data from disk.
// Passing a nil Config uses the default config.
func Open(dir string, cfg *Config) (*Cache, error) {
	dirFd, err := os.Open(dir)
	if err != nil {
		return nil, err
	}
	if cfg == nil {
		cfg = &Config{}
	}
	if cfg.Expiry <= 0 {
		cfg.Expiry = 7 * 24 * time.Hour
	}
	if cfg.LogFileSize == 0 {
		cfg.LogFileSize = 10 * 1024 * 1024
	}
	if cfg.MaxGarbageRatio <= 0 || cfg.MaxGarbageRatio >= 1 {
		cfg.MaxGarbageRatio = 0.75
	}
	c := &Cache{
		cfg:             *cfg,
		dir:             dir,
		dirFd:           dirFd,
		files:           map[string]*cachedFile{},
		nextWriteLogId:  1,
		nextPurgeLogId:  1,
		oldestOpenLogId: 1,
		openLogfiles:    map[int]*os.File{},
	}
	logs, purgeLogs, err := c.findLogFiles()
	if err != nil {
		return nil, err
	}
	if len(logs) > 0 {
		c.oldestOpenLogId = logs[0]
		c.nextWriteLogId = logs[len(logs)-1] + 1
	}
	if len(purgeLogs) > 0 {
		c.nextPurgeLogId = purgeLogs[len(purgeLogs)-1] + 1
	}
	purgeCh := make(chan purgeRequest, 5)
	purgeErr := make(chan error, 1)
	go func() {
		purgeErr <- c.purgeLogReader(purgeLogs, purgeCh)
	}()
	c.readLogs(logs, purgeCh)
	if err := <-purgeErr; err != nil {
		log.Printf("Error reading purge logs. No choice but to wipe the cache: %v", err)
		// Nuke it all and start clean.
		c.files = map[string]*cachedFile{}
		newId := max(c.nextWriteLogId, c.nextPurgeLogId) + 1
		c.nextWriteLogId = newId
		c.nextPurgeLogId = newId
		c.oldestOpenLogId = newId
		c.openLogfiles = map[int]*os.File{}
	}
	c.currentWriteLogId = c.nextWriteLogId
	c.garbageCollection()
	return c, nil
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

type Cache struct {
	cfg   Config
	dir   string
	dirFd *os.File

	// TODO(quis): Use fine grained locking to allow more concurrency.
	giantMtx sync.Mutex

	files map[string]*cachedFile

	writeLog          *os.File
	writeLogBuffered  *bufio.Writer
	writeLogOffset    int64
	currentWriteLogId int
	nextWriteLogId    int

	purgeLog       *os.File
	nextPurgeLogId int

	openLogfiles    map[int]*os.File
	oldestOpenLogId int
}

// findLogFiles looks at the files on disk and decides which ones to use.
// It removes useless files from disk.
// It returns two sorted slices: one with log indexes and one with purge log indexes.
func (c *Cache) findLogFiles() ([]int, []int, error) {
	var logs []int
	var purgeLogs []int
	entries, err := os.ReadDir(c.dir)
	if err != nil {
		return nil, nil, err
	}
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".log") {
			i, err := strconv.ParseInt(strings.TrimSuffix(e.Name(), ".log"), 10, 64)
			if err != nil {
				continue
			}
			logs = append(logs, int(i))
		} else if strings.HasSuffix(e.Name(), ".plog") {
			i, err := strconv.ParseInt(strings.TrimSuffix(e.Name(), ".plog"), 10, 64)
			if err != nil {
				continue
			}
			purgeLogs = append(purgeLogs, int(i))
		}
	}
	if len(logs) == 0 {
		for _, i := range purgeLogs {
			_ = os.Remove(filepath.Join(c.dir, fmt.Sprintf("%d.plog", i)))
		}
		return nil, nil, nil
	}
	sort.Ints(logs)
	sort.Ints(purgeLogs)
	n := logs[0]
	for _, l := range logs[1:] {
		if l != n+1 {
			_ = os.Remove(filepath.Join(c.dir, fmt.Sprintf("%d.log", n)))
			logs = logs[1:]
		}
		n = l
	}
	for i, n := range purgeLogs {
		if n >= logs[0] {
			purgeLogs = purgeLogs[i:]
			break
		}
		_ = os.Remove(filepath.Join(c.dir, fmt.Sprintf("%d.plog", n)))
	}
	// XXX if there is a purgeLog gap, remove all older write logs
	return logs, purgeLogs, nil
}

type cachedFile struct {
	ranges ranges.Ranges
}

type cacheChunk struct {
	dataOffset int64
	dataSize   int

	f              *os.File
	headerPosition int64
	startOfData    int

	lastAccess time.Time
}

// Slice is called when this cacheChunk is shrunk. If data in the middle of this chunk is overwritten, Slice will be called twice: once for the remaining first part and once for the remaining second part.
func (cc cacheChunk) Slice(start, end int64) ranges.Data {
	return cacheChunk{
		dataOffset:     start,
		dataSize:       int(end - start),
		f:              cc.f,
		headerPosition: cc.headerPosition,
		startOfData:    cc.startOfData + int(start-cc.dataOffset),
		lastAccess:     cc.lastAccess,
	}
}

// readLogs reads all the logs from disk while executing purge logs at the intended offsets.
// It fills c.files[].ranges with cacheChunk's that tell you where to find data on disk.
func (c *Cache) readLogs(logs []int, purgeCh <-chan purgeRequest) {
	nextPurgeRequest := <-purgeCh
	for _, n := range logs {
		f, err := os.Open(filepath.Join(c.dir, fmt.Sprintf("%d.log", n)))
		if err != nil {
			log.Printf("cachelog: Warning when reading old logs: %v", err)
			continue
		}
		if err := c.readLog(n, f, purgeCh, &nextPurgeRequest); err != nil {
			log.Printf("cachelog: Warning when reading old logs: %v", err)
			continue
		}
	}
	// If there's any remaining purge logs, execute them now.
	if nextPurgeRequest.logNumber != 0 {
		c.executePurgeRequest(nextPurgeRequest)
	}
	for pr := range purgeCh {
		c.executePurgeRequest(pr)
	}
}

// readLog reads through a single log file and registers where the data for a specific file+offset can be found on disk.
// Logs are read sequentially, so next readLog() might overwrite it with newer data.
// If an error occurs, the rest of this log file is ignored.
func (c *Cache) readLog(logNumber int, f *os.File, purgeCh <-chan purgeRequest, nextPurgeRequest *purgeRequest) error {
	r := bufio.NewReader(f)
	offset := int64(0)
	for {
		for nextPurgeRequest.logNumber < logNumber || (nextPurgeRequest.logNumber == logNumber && nextPurgeRequest.logOffset <= offset) {
			if nextPurgeRequest.logNumber == 0 {
				// purgeCh is closed; there are no more purges in the log.
				break
			}
			c.executePurgeRequest(*nextPurgeRequest)
			*nextPurgeRequest = <-purgeCh
		}
		headerOffset := offset
		var h Header
		if err := binary.Read(r, binary.LittleEndian, &h); err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		offset += int64(headerSize)
		filename := make([]byte, h.FilenameLength)
		if _, err := io.ReadFull(r, filename); err != nil {
			return err
		}
		offset += int64(h.FilenameLength)
		hash, err := hashForWriting(h.HashedHeader, filename, io.LimitReader(r, int64(h.DataSize)))
		if err != nil {
			return err
		}
		offset += int64(h.DataSize)
		if !bytes.Equal(h.Hash[:], hash) {
			return errors.New("checksum failure")
		}
		c.openLogfiles[logNumber] = f
		c.recordDataPosition(f, headerOffset, filename, h)
	}
}

// recordDataPosition stores in which log data can be found on disk.
func (c *Cache) recordDataPosition(f *os.File, offset int64, filename []byte, h Header) {
	fn := string(filename)
	cf, ok := c.files[fn]
	if !ok {
		cf = &cachedFile{}
		c.files[fn] = cf
	}
	cf.ranges.Add(ranges.Entry{h.DataOffset, h.DataOffset + int64(h.DataSize), cacheChunk{
		dataOffset:     h.DataOffset,
		dataSize:       int(h.DataSize),
		f:              f,
		headerPosition: offset,
		startOfData:    headerSize + int(h.FilenameLength),
		lastAccess:     time.Unix(h.LastAccess, 0),
	}})
}

// Get reads into buf and fills it with data from filename@offset. For data not in the cache, those bytes in buf will be untouched and those ranges will be returned in $missing.
func (c *Cache) Get(filename []byte, offset int64, buf []byte) (missing []ranges.Entry, _ error) {
	c.giantMtx.Lock()
	defer c.giantMtx.Unlock()
	return c.get(filename, offset, buf, true)
}

func (c *Cache) get(filename []byte, offset int64, buf []byte, updateTimestamps bool) (missing []ranges.Entry, _ error) {
	fn := string(filename)
	f, ok := c.files[fn]
	if !ok {
		f = &cachedFile{}
		c.files[fn] = f
	}
	// If the data is spread over (multiple places in the) logs, those will be fetched in parallel and written to their place in buf.
	now := time.Now()
	recentEnough := now.Add(-c.cfg.Expiry / 100)
	nowb := timeInBytes(now)
	chunks, missing := f.ranges.Get(offset, offset+int64(len(buf)))
	var g errgroup.Group
	for _, e := range chunks {
		ch := e.Data.(cacheChunk)
		g.Go(func() error {
			bufOffset := int(ch.dataOffset - offset)
			_, err := ch.f.ReadAt(buf[bufOffset:bufOffset+ch.dataSize], ch.headerPosition+int64(ch.startOfData))
			return err
		})
		if updateTimestamps {
			// Only update the timestamp if we didn't do so recently. (specially: 1% of c.cfg.Expiry has passed. With the default 1 week expiry, that's once every 1.6 hour.)
			if ch.lastAccess.Before(recentEnough) {
				ch.lastAccess = now
				g.Go(func() error {
					_, _ = ch.f.WriteAt(nowb, ch.headerPosition)
					return nil
				})
			}
		}
	}
	return missing, g.Wait()
}

func timeInBytes(t time.Time) []byte {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(t.Unix()))
	return buf
}

// Header is the on-disk header in logs.
type Header struct {
	LastAccess int64
	Hash       [md5.Size]byte
	HashedHeader
}

// HashedHeader is the hashed part of the Header, containing all fields that don't change.
type HashedHeader struct {
	FilenameLength int32
	DataSize       int32
	DataOffset     int64
}

// hashForWriting hashes the header + filename + data.
func hashForWriting(hh HashedHeader, filename []byte, data io.Reader) ([]byte, error) {
	mh := md5.New()
	if err := binary.Write(mh, binary.LittleEndian, hh); err != nil {
		return nil, err
	}
	if _, err := mh.Write(filename); err != nil {
		return nil, err
	}
	if _, err := io.Copy(mh, data); err != nil {
		return nil, err
	}
	return mh.Sum(nil), nil
}

// closeWriteLog stops writing to the current writeLog. It doesn't actually close the *os.File, because we still need that to keep serving reads.
func (c *Cache) closeWriteLog() {
	_ = c.writeLogBuffered.Flush()
	c.writeLog = nil
	c.writeLogBuffered = nil
	if c.purgeLog != nil && c.nextWriteLogId == c.nextPurgeLogId {
		c.purgeLog.Close()
		c.purgeLog = nil
	}
}

// Put writes data to the log.
// We guarantee that future Get()s will only get this new data if there is no machine/disk crash.
// If the machine/disk does crash, old data might be served. To avoid this, see Delete().
func (c *Cache) Put(filename []byte, offset int64, buf []byte) error {
	c.giantMtx.Lock()
	defer c.giantMtx.Unlock()
	return c.put(filename, offset, buf, time.Now())
}

func (c *Cache) put(filename []byte, offset int64, buf []byte, lastAccess time.Time) error {
	if c.writeLog == nil {
		c.currentWriteLogId = c.nextWriteLogId
		c.nextWriteLogId++
		fh, err := os.OpenFile(filepath.Join(c.dir, fmt.Sprintf("%d.log", c.currentWriteLogId)), os.O_RDWR|os.O_CREATE|os.O_TRUNC|os.O_EXCL, 0666)
		if err != nil {
			return err
		}
		c.writeLog = fh
		c.writeLogBuffered = bufio.NewWriterSize(c.writeLog, 16*1024)
		c.writeLogOffset = 0
		c.openLogfiles[c.currentWriteLogId] = fh
	}
	h := Header{
		LastAccess: lastAccess.Unix(),
		HashedHeader: HashedHeader{
			FilenameLength: int32(len(filename)),
			DataSize:       int32(len(buf)),
			DataOffset:     offset,
		},
	}
	hash, err := hashForWriting(h.HashedHeader, filename, bytes.NewReader(buf))
	if err != nil {
		return err
	}
	copy(h.Hash[:], hash)

	headerOffset := c.writeLogOffset
	if err := binary.Write(c.writeLogBuffered, binary.LittleEndian, h); err != nil {
		c.closeWriteLog()
		return err
	}
	c.writeLogOffset += int64(headerSize)
	if _, err := c.writeLogBuffered.Write(filename); err != nil {
		c.closeWriteLog()
		return err
	}
	c.writeLogOffset += int64(len(filename))
	if _, err := c.writeLogBuffered.Write(buf); err != nil {
		c.closeWriteLog()
		return err
	}
	c.writeLogOffset += int64(len(buf))
	// We need to flush the buffered writer so future reads can get the data.
	if err := c.writeLogBuffered.Flush(); err != nil {
		c.closeWriteLog()
		return err
	}
	c.recordDataPosition(c.writeLog, headerOffset, filename, h)
	if c.writeLogOffset >= int64(c.cfg.LogFileSize) {
		c.closeWriteLog()
		c.garbageCollection()
	}
	return nil
}

// Delete data from the cache. The deletion marker is synced to a separate log file, which guarantees the old data is never served again - even in the face of machine crashes.
func (c *Cache) Delete(filename []byte, offset int64, size int) error {
	c.giantMtx.Lock()
	defer c.giantMtx.Unlock()
	if c.purgeLog == nil {
		if c.currentWriteLogId > c.nextPurgeLogId {
			c.nextPurgeLogId = c.currentWriteLogId
		}
		fh, err := os.OpenFile(filepath.Join(c.dir, fmt.Sprintf("%d.plog", c.nextPurgeLogId)), os.O_RDWR|os.O_CREATE|os.O_TRUNC|os.O_EXCL, 0666)
		c.nextPurgeLogId++
		if err != nil {
			return err
		}
		if err := c.dirFd.Sync(); err != nil {
			return err
		}
		c.purgeLog = fh
	}
	h := PurgeHeader{
		HashedPurgeHeader: HashedPurgeHeader{
			LogNumber:      int32(c.currentWriteLogId),
			LogOffset:      c.writeLogOffset,
			FilenameLength: int32(len(filename)),
			DataOffset:     offset,
			DataSize:       int32(size),
		},
	}
	hash, err := hashForPurging(h.HashedPurgeHeader, filename)
	if err != nil {
		return err
	}
	copy(h.Hash[:], hash)

	c.executePurgeRequest(createPurgeRequest(h, filename))

	// XXX Close log file on failure.
	if err := binary.Write(c.purgeLog, binary.LittleEndian, h); err != nil {
		return err
	}
	if _, err := c.purgeLog.Write(filename); err != nil {
		return err
	}
	if err := c.purgeLog.Sync(); err != nil {
		return err
	}
	return nil
}

// PurgeHeader is the on-disk header for purge logs (those containing deletes).
type PurgeHeader struct {
	Hash [md5.Size]byte
	HashedPurgeHeader
}

// HashedPurgeHeader is the hashed part of PurgeHeader.
type HashedPurgeHeader struct {
	LogOffset      int64
	LogNumber      int32
	FilenameLength int32
	DataOffset     int64
	DataSize       int32
}

func hashForPurging(hh HashedPurgeHeader, filename []byte) ([]byte, error) {
	mh := md5.New()
	if err := binary.Write(mh, binary.LittleEndian, hh); err != nil {
		return nil, err
	}
	if _, err := mh.Write(filename); err != nil {
		return nil, err
	}
	return mh.Sum(nil), nil
}

// GarbageCollection runs the garbage collection if needed. If the MaxGarbageRatio isn't violated, GarbageCollection does nothing.
// The garbage collection process involves copying the still relevant data from the oldest log files to the active log file, this then allows us to remove the old log file and all the stale data with it.
// Multiple files might be collected in one run, but the active write log file is never considered even if it contains dead data.
func (c *Cache) GarbageCollection() {
	c.giantMtx.Lock()
	defer c.giantMtx.Unlock()
	c.garbageCollection()
}

func (c *Cache) garbageCollection() {
	fileSizes := map[int]int64{}
	var stored, alive int64
	for n, fh := range c.openLogfiles {
		st, err := fh.Stat()
		if err != nil {
			// Can fstat() on a valid fd even fail?
			continue
		}
		fileSizes[n] = st.Size()
		stored += st.Size()
	}
	for _, cf := range c.files {
		alive += cf.ranges.Count()
	}
	for i := c.oldestOpenLogId; stored > 0 && 1.0-float64(alive)/float64(stored) > c.cfg.MaxGarbageRatio && c.currentWriteLogId > i; i++ {
		fh, open := c.openLogfiles[i]
		if open {
			// TODO(quis): Figure out how many files we need to collect and call c.collect() only once.
			if err := c.collect(map[*os.File]struct{}{fh: struct{}{}}); err != nil {
				log.Printf("cachelog: garbage collection: %v", err)
				break
			}
			_ = fh.Close()
			delete(c.openLogfiles, i)
		}
		_ = os.Remove(filepath.Join(c.dir, fmt.Sprintf("%d.log", i)))
		_ = os.Remove(filepath.Join(c.dir, fmt.Sprintf("%d.plog", i)))
		c.oldestOpenLogId++
		stored -= fileSizes[i]
		// TODO(quis): Get the new size of c.writeLog and increase stored accordingly.
	}
}

func (c *Cache) collect(collectFiles map[*os.File]struct{}) error {
	buf := make([]byte, 1024*1024)
	cutoff := time.Now().Add(-c.cfg.Expiry)
	for fn, cf := range c.files {
		got, _ := cf.ranges.Get(0, math.MaxInt64)
		inThisLog := ranges.Ranges{}
		for _, e := range got {
			ch := e.Data.(cacheChunk)
			if _, found := collectFiles[ch.f]; !found {
				continue
			}
			if ch.lastAccess.Before(cutoff) {
				cf.ranges.Remove(e.Start, e.End)
				continue
			}
			inThisLog.Add(ranges.Entry{ch.dataOffset, ch.dataOffset + int64(ch.dataSize), dateAsData(ch.lastAccess)})
		}
		alive := 0
		toCopy, _ := inThisLog.Get(0, math.MaxInt64)
		toCopy = ranges.MergeAdjacent(toCopy, func(a, b ranges.Entry) (ranges.Data, bool) {
			if b.End-a.Start > int64(len(buf)) {
				return nil, false
			}
			return dateAsData(maxDate(time.Time(a.Data.(dateAsData)), time.Time(b.Data.(dateAsData)))), true
		})
		for _, e := range toCopy {
			s := e.End - e.Start
			var sb []byte
			if s <= int64(len(buf)) {
				sb = buf[:e.End-e.Start]
			} else {
				sb = make([]byte, s)
			}
			if _, err := c.get([]byte(fn), e.Start, sb, false); err != nil {
				log.Printf("cachelog: garbage collection: Failed to read from old log: %v", err)
				cf.ranges.Remove(e.Start, e.End)
				continue
			}
			if err := c.put([]byte(fn), e.Start, sb, time.Time(e.Data.(dateAsData))); err != nil {
				return err
			}
			alive += len(buf)
		}
	}
	return nil
}

type dateAsData time.Time

func (dad dateAsData) Slice(start, end int64) ranges.Data {
	return dad
}

func maxDate(a, b time.Time) time.Time {
	if a.Before(b) {
		return b
	}
	return a
}

type purgeRequest struct {
	logNumber  int
	logOffset  int64
	filename   []byte
	dataOffset int64
	dataSize   int32
}

func (c *Cache) executePurgeRequest(pr purgeRequest) {
	fn := string(pr.filename)
	cf, ok := c.files[fn]
	if !ok {
		return
	}
	cf.ranges.Remove(pr.dataOffset, pr.dataOffset+int64(pr.dataSize))
}

func createPurgeRequest(h PurgeHeader, filename []byte) purgeRequest {
	return purgeRequest{
		logNumber:  int(h.LogNumber),
		logOffset:  h.LogOffset,
		filename:   filename,
		dataOffset: h.DataOffset,
		dataSize:   h.DataSize,
	}
}

// purgeLogReader reads the given purge logs and sends the entries back over $out.
func (c *Cache) purgeLogReader(purgeLogs []int, out chan<- purgeRequest) (retErr error) {
	d := dfr.D{}
	defer d.Run(&retErr)
	var lastLogNumber int32
	var lastLogOffset int64
	for _, n := range purgeLogs {
		cnt := 0
		f, err := os.Open(filepath.Join(c.dir, fmt.Sprintf("%d.plog", n)))
		if err != nil {
			return err
		}
		closer := d.AddErr(f.Close)
		r := bufio.NewReader(f)
		for {
			var h PurgeHeader
			if err := binary.Read(r, binary.LittleEndian, &h); err != nil {
				if err == io.EOF {
					break
				}
				return err
			}
			filename := make([]byte, h.FilenameLength)
			if _, err := io.ReadFull(r, filename); err != nil {
				return err
			}
			hash, err := hashForPurging(h.HashedPurgeHeader, filename)
			if err != nil {
				return err
			}
			if !bytes.Equal(h.Hash[:], hash) {
				return errors.New("purgelog checksum failure")
			}
			if lastLogNumber > h.LogNumber {
				bug("cachelog: Purge log went backwards (%d.plog -> %d.plog)", lastLogNumber, h.LogNumber)
			} else if lastLogNumber < h.LogNumber {
				lastLogNumber = h.LogNumber
				lastLogOffset = 0
			}
			if lastLogOffset > h.LogOffset {
				bug("cachelog: Purge log went backwards (%d.plog:%d -> %d.plog:%d)", lastLogNumber, lastLogOffset, h.LogNumber, h.LogOffset)
			} else if lastLogOffset < h.LogOffset {
				lastLogOffset = h.LogOffset
			}
			out <- createPurgeRequest(h, filename)
			cnt++
		}
		closer(true)
	}
	close(out)
	return nil
}
