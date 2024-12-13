# FlatBuffers Generated Code

This folder contains the generated [flatbuffer](https://flatbuffers.dev/) code. Flat buffer schemas are in the root `schemas` folder.

## How to Generate FlatBuffers Go Code

To generate `.go` files from `.fbs` files:

1. Install [flatc](https://github.com/google/flatbuffers).
2. Run `make flatbuf` from the project root. (This is automatically run when `make build` is run)

Step 2 will generate `manifest_generated.go` file under the directory `internal/flatbuf` with package name `flatbuf`

_NOTE: You can install `flatc` with `brew install flatbuffers` if you're using Homebrew._