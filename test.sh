#!/usr/bin/env bash

go test -v -race -cpu=1 -parallel=0 -bench=. -benchmem -cover
#go test -v -race -cpu=4 -parallel=4 -bench=. -benchmem -cover
