# Official toolchain for ScyllaDB

While we aim to build out-of-the-box on recent distributions, this isn't
always possible and not everyone runs a recent distribution. For that reason
a version-controlled toolchain it provided as a docker image.

## Obtaining the current toolchain

The toolchain is stored in a file called `tools/toolchain/image`. To access
the toolchain, pull that image:

    docker pull $(<tools/toolchain/image)

A helper script `dbuild` allows you to run command in that toolchain with
the working directory mounted:

    ./tools/toolchain/dbuild ./configure.py
    ./tools/toolchain/dbuild ninja

## Building the toolchain

Run the command

    docker build -f tools/toolchain/Dockerfile .

and use the resulting image.

## Publishing an image

If you're a maintainer, you can tag the image and push it
using `docker push`. Tags follow the format
`scylladb/scylla-toolchain:fedora-29-[branch-3.0-]20181128`. After the
image is pushed, update `tools/toolchain/image` so new
builds can use it automatically.

For master toolchains, the branch designation is omitted. In a branch, if
there is a need to update a toolchain, the branch designation is added to
the tag to avoid ambiguity.
