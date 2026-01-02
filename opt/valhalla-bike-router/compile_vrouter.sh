#!/bin/bash
# Compile vrouter on N9 or with MADDE SDK
# Requires: gcc, zlib-dev

cd /opt/valhalla-bike-router

if [ -f /usr/bin/gcc ]; then
    echo "Compiling with system gcc..."
    gcc -O3 -std=c99 -lz -lm -o vrouter vrouter.c
elif which arm-none-linux-gnueabi-gcc >/dev/null 2>&1; then
    echo "Compiling with MADDE SDK..."
    arm-none-linux-gnueabi-gcc -O3 -std=c99 -march=armv7-a -mtune=cortex-a8 \
        --sysroot=$SYSROOT -lz -lm -o vrouter vrouter.c
else
    echo "No compiler found. Install gcc or use MADDE SDK."
    exit 1
fi

if [ -f vrouter ]; then
    echo "Success! vrouter compiled."
    chmod +x vrouter
else
    echo "Compilation failed."
    exit 1
fi
