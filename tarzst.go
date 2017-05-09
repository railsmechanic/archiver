package archiver

import (
	"archive/tar"
	"fmt"
	"os"
	"strings"

	"github.com/Datadog/zstd"
)

// TarZst is for TarZst format
var TarZst tarZstFormat

func init() {
	RegisterFormat("TarZst", TarZst)
}

type tarZstFormat struct{}

func (tarZstFormat) Match(filename string) bool {
	return strings.HasSuffix(strings.ToLower(filename), ".tar.zst") || strings.HasSuffix(strings.ToLower(filename), ".tzst") || isTarZst(filename)
}

// isTarZst checks the file has the Zst compressed Tar format header by
// reading its beginning block.
func isTarZst(tarzstPath string) bool {
	f, err := os.Open(tarzstPath)
	if err != nil {
		return false
	}
	defer f.Close()

	zstr := zstd.NewReader(f)
	buf := make([]byte, tarBlockSize)
	n, err := zstr.Read(buf)
	if err != nil || n < tarBlockSize {
		return false
	}

	return hasTarHeader(buf)
}

// Make creates a .tar.zst file at tarzstPath containing
// the contents of files listed in filePaths. File paths
// can be those of regular files or directories. Regular
// files are stored at the 'root' of the archive, and
// directories are recursively added.
func (tarZstFormat) Make(tarzstPath string, filePaths []string) error {
	out, err := os.Create(tarzstPath)
	if err != nil {
		return fmt.Errorf("error creating %s: %v", tarzstPath, err)
	}
	defer out.Close()

	zstWriter := zstd.NewWriter(out)
	defer zstWriter.Close()

	tarWriter := tar.NewWriter(zstWriter)
	defer tarWriter.Close()

	return tarball(filePaths, tarWriter, tarzstPath)
}

// Open untars source and decompresses the contents into destination.
func (tarZstFormat) Open(source, destination string) error {
	f, err := os.Open(source)
	if err != nil {
		return fmt.Errorf("%s: failed to open archive: %v", source, err)
	}
	defer f.Close()

	zstr := zstd.NewReader(f)
	return untar(tar.NewReader(zstr), destination)
}
