#!/usr/bin/env bash

go test -bench=. -benchmem -cover -cpu 1
