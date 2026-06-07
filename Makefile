.PHONY: all web-assets linux windows darwin linux-i386 linux-amd64 linux-arm linux-arm64 windows-i386 windows-amd64 windows-arm64 darwin-amd64 darwin-arm64

all: web-assets linux windows darwin

web-assets:
	cd webmanager && npm ci && npm run build-prod
	rm -rf internal/assets/browser
	cp -r webmanager/dist/webmanager/browser internal/assets/

linux-i386: web-assets
	CGO_ENABLED=0 GOOS=linux GOARCH=386 ./build-scripts/build.sh
linux-amd64: web-assets
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 ./build-scripts/build.sh
linux-arm: web-assets
	CGO_ENABLED=0 GOOS=linux GOARCH=arm ./build-scripts/build.sh
linux-arm64: web-assets
	CGO_ENABLED=0 GOOS=linux GOARCH=arm64 ./build-scripts/build.sh
windows-i386: web-assets
	GOOS=windows GOARCH=386 ./build-scripts/build.sh
windows-amd64: web-assets
	GOOS=windows GOARCH=amd64 ./build-scripts/build.sh
windows-arm64: web-assets
	GOOS=windows GOARCH=arm64 ./build-scripts/build.sh
darwin-amd64: web-assets
	GOOS=darwin GOARCH=amd64 ./build-scripts/build.sh
darwin-arm64: web-assets
	GOOS=darwin GOARCH=arm64 ./build-scripts/build.sh

linux: linux-i386 linux-amd64 linux-arm linux-arm64
windows: windows-i386 windows-amd64 windows-arm64
darwin: darwin-amd64 darwin-arm64
