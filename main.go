package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sync"
)

const ReadBlockSize = 1024 * 16

func main() {

	// keep all the files in an array to minimize copying
	files := []FileWithHasher{}

	filepath.Walk(".", func(path string, info os.FileInfo, err error) error {
		if !info.IsDir() && info.Size() > 0 {
			files = append(files, FileWithHasher{
				hasher:   sha256.New(),
				path:     path,
				id:       len(files),
				filesize: info.Size(),
				offset:   0,
			})
		}
		return nil
	})

	// fill initial cache
	file_cache := make(map[SizeDigestPair][]*FileWithHasher)
	for i := 0; i < len(files); i++ {
		fh := &files[i]
		// initially the has is all zeros and that's ok. This way we first differentiate by size of files only, not
		// their contents
		key := SizeDigestPair{
			filesize: fh.filesize,
		}
		file_cache[key] = append(file_cache[key], fh)
	}

	jobs := make(chan []*FileWithHasher, len(files))
	results := make(chan []*FileWithHasher, len(files))

	wg := &sync.WaitGroup{}

	for i := 0; i <= runtime.GOMAXPROCS(-1); i++ {
		go worker(wg, jobs, results)
	}

	wg.Add(len(file_cache))
	for _, v := range file_cache {
		jobs <- v
	}

	go func() {
		for arr := range results {
			key := arr[0].get_key()
			for _, fh := range arr {
				fmt.Println(hex.EncodeToString(key.digest[:4]), fh.path)
			}
		}
	}()

	wg.Wait()
}

func worker(wg *sync.WaitGroup, buckets chan []*FileWithHasher, results chan<- []*FileWithHasher) {
	for bucket := range buckets {

		new_file_buckets := make(map[SizeDigestPair][]*FileWithHasher)
		final_file_buckets := make(map[SizeDigestPair][]*FileWithHasher)

		for _, fh := range bucket {
			if key, active := fh.iterate(); active {
				new_file_buckets[key] = append(new_file_buckets[key], fh)
			} else {
				final_file_buckets[fh.get_key()] = append(final_file_buckets[fh.get_key()], fh)
			}
		}

		for _, v := range new_file_buckets {
			if len(v) > 1 {
				buckets <- v
				wg.Add(1)
			}
		}
		for _, v := range final_file_buckets {
			if len(v) > 1 {
				results <- v
			}
		}

		wg.Done()
	}
}

type SizeDigestPair struct {
	filesize int64
	digest   [sha256.Size]byte
}

type FileWithHasher struct {
	id       int
	filesize int64
	path     string
	hasher   hash.Hash
	offset   int64
	digest   [sha256.Size]byte
}

func (fh *FileWithHasher) get_key() SizeDigestPair {
	return SizeDigestPair{
		filesize: fh.filesize,
		digest:   fh.digest,
	}
}

func (fh *FileWithHasher) iterate() (SizeDigestPair, bool) {
	f, err := os.Open(fh.path)
	die(err)
	defer f.Close()
	f.Seek(fh.offset, io.SeekStart)
	buf := make([]byte, ReadBlockSize)
	n, err := f.Read(buf)
	fh.offset += int64(n)
	if n == 0 || err == io.EOF {
		return SizeDigestPair{}, false
	} else {
		die(err)
		fh.hasher.Write(buf)
		digestSlice := fh.hasher.Sum(nil)
		copy(fh.digest[0:sha256.Size], digestSlice[0:sha256.Size])
		return fh.get_key(), true
	}
}

func die(err error) {
	if err != nil {
		panic(err)
	}
}
