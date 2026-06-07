package assets

import (
	"embed"
	"fmt"
	"io/fs"
)

//go:embed browser
var WebAssets embed.FS

var subFS = fs.Sub

func GetWebFS() (fs.FS, error) {
	webFS, err := subFS(WebAssets, "browser")
	if err != nil {
		return nil, fmt.Errorf("embedded web assets unavailable: %w", err)
	}
	return webFS, nil
}
