#!/usr/bin/env bash

go test -v -race -cpu=4 -parallel=4 -bench=. -benchmem -cover
