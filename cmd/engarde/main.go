package main

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/signal"
	"syscall"

	"github.com/adrianceding/engarde/internal/assets"
	"github.com/adrianceding/engarde/internal/clientrole"
	"github.com/adrianceding/engarde/internal/config"
	pathmgr "github.com/adrianceding/engarde/internal/path"
	"github.com/adrianceding/engarde/internal/serverrole"
	"github.com/adrianceding/engarde/internal/version"
	log "github.com/sirupsen/logrus"
)

var exit = os.Exit
var getWebFS = assets.GetWebFS
var listInterfaces = pathmgr.ListInterfaces
var startClient = defaultStartClient
var startServer = defaultStartServer

func defaultStartClient(ctx context.Context, cfg config.Client, version string, webFS fs.FS) error {
	return clientrole.New(cfg, version, webFS).Run(ctx)
}

func defaultStartServer(ctx context.Context, cfg config.Server, version string, webFS fs.FS) error {
	return serverrole.New(cfg, version, webFS).Run(ctx)
}

func main() {
	exit(runExitCode(os.Args[1:], os.Stdout))
}

func runExitCode(args []string, out io.Writer) int {
	if err := run(args, out); err != nil {
		log.Error(err)
		return 1
	}
	return 0
}

func run(args []string, out io.Writer) error {
	configName := "engarde.yml"
	if len(args) > 0 {
		configName = args[0]
	}

	printVersion(out)
	if configName == "-v" {
		return nil
	}
	if configName == "list-interfaces" {
		return listInterfaces(out)
	}

	cfg, role, err := config.Load(configName)
	if err != nil {
		return err
	}
	webFS, err := getWebFS()
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return runRole(ctx, cfg, role, webFS)
}

func runRole(ctx context.Context, cfg *config.Config, role config.Role, webFS fs.FS) error {
	switch role {
	case config.RoleClient:
		log.Info("Starting as client")
		return startClient(ctx, cfg.Client, version.Version, webFS)
	case config.RoleServer:
		log.Info("Starting as server")
		return startServer(ctx, cfg.Server, version.Version, webFS)
	}
	return fmt.Errorf("unknown role %q", role)
}

func printVersion(out io.Writer) {
	if version.Version != "" {
		fmt.Fprint(out, "engarde ver. "+version.Version+"\r\n")
	}
}
