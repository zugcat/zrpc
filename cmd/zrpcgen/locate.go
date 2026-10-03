package main

import (
	"errors"
	"fmt"
	"go/ast"
	"go/token"

	"golang.org/x/tools/go/packages"
)

var errNotInterface = errors.New("not a direct interface type")

func locate(name string, pkg *packages.Package) (*ast.TypeSpec, *ast.File, error) {
	for _, file := range pkg.Syntax {
		if spec, err := locateFile(name, file); spec != nil || err != nil {
			return spec, file, err
		}
	}

	return nil, nil, nil
}

func locateFile(name string, file *ast.File) (*ast.TypeSpec, error) {
	for _, decl := range file.Decls {
		if genDecl, ok := decl.(*ast.GenDecl); ok {
			if genDecl.Tok == token.TYPE {
				for _, spec := range genDecl.Specs {
					spec := spec.(*ast.TypeSpec)
					spec.Doc = genDecl.Doc // kludge
					if spec.Name.Name == name {
						if _, ok := spec.Type.(*ast.InterfaceType); ok {
							return spec, nil
						}
						return nil, fmt.Errorf("%w: %s", errNotInterface, name)
					}
				}
			}
		}
	}

	return nil, nil
}
