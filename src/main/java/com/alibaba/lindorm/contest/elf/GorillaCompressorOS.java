package com.alibaba.lindorm.contest.elf;


import gorilla.CompressorOS;

public class GorillaCompressorOS implements ICompressor {
    private final CompressorOS gorilla;
    public GorillaCompressorOS(int bufferSize) {
        this.gorilla = new CompressorOS(bufferSize);
    }

    @Override public void addValue(double v) {
        this.gorilla.addValue(v);
    }

    @Override public int getSize() {
        return this.gorilla.getSize();
    }

    @Override public byte[] getBytes() {
        return this.gorilla.getOutputStream().getBuffer();
    }

    @Override public void close() {
        this.gorilla.close();
    }
}
