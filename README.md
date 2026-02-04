1. Set up environment (adjust <USERNAME> to your Linux username):

   export MADDE_ROOT=/home/<USERNAME>QtSDK/Madde
   export TOOLCHAIN=$MADDE_ROOT/toolchains/arm-2009q3-67-arm-none-linux-gnueabi-x86_64-unknown-linux-gnu/arm-2009q3-67
   export SYSROOT=$MADDE_ROOT/sysroots/harmattan_sysroot_10.2011.34-1_slim
   export PATH=$TOOLCHAIN/bin:$PATH
2. Compile:
    arm-none-linux-gnueabi-gcc -O2 -std=c99 -mcpu=cortex-a8 \
      -funsafe-math-optimizations -ffast-math \
      --sysroot=$SYSROOT -o vrouter vrouter_neon2.c -lz -lm3. Strip (optional, reduces binary size):
 arm-none-linux-gnueabi-strip vrouter
