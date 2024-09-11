# FlatBuffers Generated Code

This folder contains the generated [flatbuffer](https://flatbuffers.dev/) code. Flat buffer schemas are in the root `schemas` folder.

## How to Generate FlatBuffers Go Code

To generate `.go` files from `.fbs` files:

1. Install [flatc](https://github.com/google/flatbuffers).
2. Run `flatc -o gen/ --go --gen-all --gen-onefile --go-namespace flatbuf schemas/manifest.fbs` from the project root.
_NOTE: You can install it with `brew install flatbuffers` if you're using Homebrew._