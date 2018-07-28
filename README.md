# duplicate-file-finder
finds duplicate files as efficiently as possible, by reading files from the beginning and hashing as it goes, and keeping track of files as soon as they diverge.

## Usage:
How to use: `go run /path/to/repo/main.go`

Options: none

Example output:
```
<truncated hash> path/to/duplicated/file
```
Files that are not duplicates are not printed.
