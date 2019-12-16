#!/bin/bash
set -e
set -x
flags="$(pkg-config --cflags --libs oggz protobuf)"
g++ -o oggpb foo.pb.pb.cc oggpb.cpp $flags
./oggpb
echo "INFO"
oggz info -a oggpb.ogp
echo "VALIDATE"
oggz validate oggpb.ogp 
