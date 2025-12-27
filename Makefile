###########
# Remote development requires VSCode with Dev Containers, Remote SSH, Remote Explorer
# https://code.visualstudio.com/docs/remote/containers
###########

.PHONY: all SHELL
all:

# Makefile global config
# Use config.mak to override any of the following variables.
# Do not make changes here.

.DEFAULT_GOAL := help
.EXPORT_ALL_VARIABLES:
.ONESHELL:
.SILENT:
MAKEFLAGS += "-j$(NUM_CORES) -l$(NUM_CORES)"
MAKEFLAGS += --silent
SHELL:= /bin/bash
.SHELLFLAGS = -eu -o pipefail -c

DOCKER_CLI ?= docker
IMAGE_NAME ?= rustfs:v1.0.0
CONTAINER_NAME ?= rustfs-dev

# Docker build configurations
DOCKERFILE_PRODUCTION = Dockerfile
DOCKERFILE_SOURCE = Dockerfile.source

BUILD_OS ?= rockylinux9.3

# Makefile colors config
bold := $(shell tput bold)
normal := $(shell tput sgr0)
errorTitle := $(shell tput setab 1 && tput bold && echo '\n')
recommendation := $(shell tput setab 4)
underline := $(shell tput smul)
reset := $(shell tput -Txterm sgr0)
black := $(shell tput setaf 0)
red := $(shell tput setaf 1)
green := $(shell tput setaf 2)
yellow := $(shell tput setaf 3)
blue := $(shell tput setaf 4)
magenta := $(shell tput setaf 5)
cyan := $(shell tput setaf 6)
white := $(shell tput setaf 7)

define HEADER
How to use me:
	# To get help for each target
	${bold}make help${reset}

	# To run and execute a target
	${bold}make ${cyan}<target>${reset}

endef
export HEADER

-include $(addsuffix /*.mak, $(shell find .config/make -type d))

