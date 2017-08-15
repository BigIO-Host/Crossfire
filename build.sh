#!/bin/bash

export GOPATH=`pwd`/go
go get apiserver
go install utils
