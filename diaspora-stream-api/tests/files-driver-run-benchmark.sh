#!/usr/bin/env bash

HERE=$(dirname $0)

$HERE/files-driver-pre-test.sh

if ! which diaspora-ctl ; then
    echo "==> Detected that we are in the build tree, adding $HERE/../bin to PATH"
    export PATH=$PATH:$HERE/../bin
fi

if ! which diaspora-producer-benchmark ; then
    echo "==> Detected that we are in the build tree, adding $HERE/../benchmarks to PATH"
    export PATH=$PATH:$HERE/../benchmarks
fi

echo "==> Generating configuration for backend"
echo "{}" > benchmark_config.json

echo "==> Creating topic"
diaspora-ctl topic create --driver files --name my_topic --topic.partitions 1
r="$?"
if [ "$r" -ne "0" ]; then
    $HERE/files-driver-post-test.sh $r
    exit 1
fi

echo "==> Running producer benchmark"
diaspora-producer-benchmark -d files \
                            -c benchmark_config.json \
                            -t my_topic \
                            -n 100 \
                            -m 16 \
                            -s 128 \
                            -b 8 \
                            -f 10 \
                            -p 1
r="$?"
if [ "$r" -ne "0" ]; then
    $HERE/files-driver-post-test.sh $r
    exit 1
fi

echo "==> Running consumer benchmark"
diaspora-consumer-benchmark -d files \
                            -c benchmark_config.json \
                            -t my_topic \
                            -n 100 \
                            -s 0.5 \
                            -i 0.8 \
                            -p 1
r="$?"
if [ "$r" -ne "0" ]; then
    $HERE/files-driver-post-test.sh 0
    exit 1
fi

$HERE/files-driver-post-test.sh 0
