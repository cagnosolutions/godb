#!/usr/bin/env bash

go test -v -race -cpu=8 -parallel=8 -bench=. -benchmem -cover
#go test -v -race -cpu=4 -parallel=4 -bench=. -benchmem -cover
