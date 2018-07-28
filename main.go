package main

import (
	"path/filepath"
	"os"
	"hash"
	"crypto/sha256"
	"fmt"
	"encoding/hex"
	"io"
)

const ReadBlockSize = 1024 * 16

func main() {

	// keep all the files in an array to minimize copying
	files := []FileWithHasher{}

	filepath.Walk(".", func(path string, info os.FileInfo, err error) error {
		if !info.IsDir() {
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

	//fmt.Println(len(files))
	//for i := 0; i < len(files); i++ {
	//	fmt.Println(files[i])
	//}

	// fill initial cache
	file_cache := make(map[SizeDigestPair][]*FileWithHasher)
	for i := 0; i < len(files); i++ {
		fh := &files[i]
		if key, active := fh.iterate(); active {
			file_cache[key] = append(file_cache[key], fh)
		}
	}

	final_file_buckets := make(map[SizeDigestPair][]*FileWithHasher)

	for len(file_cache) > 0 {
		new_file_cache := make(map[SizeDigestPair][]*FileWithHasher)
		for old_key, arr := range file_cache {
			if len(arr) == 1 {
				// file is unique
				continue
			} else {
				for _, fh := range arr {
					if key, active := fh.iterate(); active {
						new_file_cache[key] = append(new_file_cache[key], fh)
					} else {
						final_file_buckets[old_key] = append(final_file_buckets[old_key], fh)
					}
				}
			}
		}
		file_cache = new_file_cache
		//fmt.Println(len(file_cache))
	}

	for key, arr := range final_file_buckets {
		for _, fh := range arr {
			fmt.Println(hex.EncodeToString(key.digest[:8]), fh.path)
		}
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
}

func (fh *FileWithHasher) get_key() SizeDigestPair {
	digestSlice := fh.hasher.Sum(nil)
	pair := SizeDigestPair{
		filesize: fh.filesize,
	}
	copy(pair.digest[0:sha256.Size], digestSlice[0:sha256.Size])
	return pair
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
		return fh.get_key(), true
	}
}

func die(err error) {
	if err != nil {
		panic(err)
	}
}
