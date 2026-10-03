package main

import (
	"flag"
	"fmt"
	"go/ast"
	"os"
	"path/filepath"

	"golang.org/x/tools/go/packages"
)

func nope(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}

func main() {
	var err error
	var dirName string
	var pkgName string
	var out string

	dirName, err = os.Getwd()
	pkgName = filepath.Base(dirName)

	if err != nil {
		nope(err)
	}

	flag.StringVar(&dirName, "dir", dirName, "directory of generated code")
	flag.StringVar(&pkgName, "pkg", pkgName, "package name of generated code (defaults to based on directory name)")
	flag.StringVar(&out, "out", out, "write to file instead of stdout")

	flag.Parse()
	args := flag.Args()

	if len(args) < 1 {
		fmt.Fprintln(os.Stderr, "usage: rpcgen [options] iface package-pattern")
		os.Exit(2)
	}

	ifaceName := args[0]
	dirName, err = filepath.Abs(dirName)

	cfg := &packages.Config{Mode: packages.LoadSyntax, Tests: true}
	pattern := append([]string{"github.com/zugcat/zrpc"}, args[1:]...)
	pkgs, err := packages.Load(cfg, pattern...)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if packages.PrintErrors(pkgs) > 0 {
		os.Exit(1)
	}

	var pkg *packages.Package
	var ts *ast.TypeSpec
	var file *ast.File

	for _, pkg = range pkgs {
		ts, file, err = locate(ifaceName, pkg)
		if err != nil {
			nope(err)
		}
		if ts != nil {
			break
		}
	}

	if ts == nil {
		fmt.Fprintf(os.Stderr, "interface %s not found\n", ifaceName)
		os.Exit(1)
	}

	gen := newGen(dirName, pkgName, file, pkg, ts)

	success := false
	w := os.Stdout
	if out != "" {
		fd, err := os.Create(out)
		if err != nil {
			nope(err)
		}
		defer func() {
			fd.Close()
			if !success {
				os.Remove(out)
			}
		}()
		w = fd
	}

	if err := gen.write(w, pkgName); err != nil {
		nope(err)
	}
	success = true
}
