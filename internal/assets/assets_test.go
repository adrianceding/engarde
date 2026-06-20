package assets

import (
	"errors"
	"io/fs"
	"strings"
	"testing"
)

func TestGetWebFS(t *testing.T) {
	webFS, err := GetWebFS()
	if err != nil {
		t.Fatalf("GetWebFS returned error: %v", err)
	}
	if _, err := fs.Stat(webFS, "README.md"); err != nil {
		t.Fatalf("embedded browser placeholder unavailable: %v", err)
	}
}

func TestGetWebFSError(t *testing.T) {
	originalSubFS := subFS
	subFS = func(fsys fs.FS, dir string) (fs.FS, error) { return nil, errors.New("sub") }
	t.Cleanup(func() { subFS = originalSubFS })

	_, err := GetWebFS()
	if err == nil || !strings.Contains(err.Error(), "embedded web assets unavailable") {
		t.Fatalf("GetWebFS error = %v, want embedded assets error", err)
	}
}
