package com.liveramp.hank.storage.cueball;

import com.liveramp.hank.compression.cueball.NoCueballCompressionCodec;
import com.liveramp.hank.test.BaseTestCase;
import org.junit.Test;

import java.io.FileOutputStream;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestCueballStreamBuffer extends BaseTestCase {
  private static final byte[] CONTIG_DATA = new byte[]{
      0x00, 1,
      0x40, 2,
      (byte) 0x80, 3,
      (byte) 0xc0, 4,
      0, 0, 0, 0, 0, 0, 0, 0,
      2, 0, 0, 0, 0, 0, 0, 0,
      4, 0, 0, 0, 0, 0, 0, 0,
      6, 0, 0, 0, 0, 0, 0, 0,
      2, 0, 0, 0,
      2, 0, 0, 0,
  };
  private final String CONTIG_PATH = localTmpDir + "/contiguous_file.cueball";

  private static final byte[] DISCONTIG_DATA_INTERNAL_HOLE = new byte[]{
      0x00, 1,
      (byte) 0x80, 3,
      (byte) 0xc0, 4,
      0, 0, 0, 0, 0, 0, 0, 0,
      -1, -1, -1, -1, -1, -1, -1, -1,
      2, 0, 0, 0, 0, 0, 0, 0,
      4, 0, 0, 0, 0, 0, 0, 0,
      2, 0, 0, 0,
      2, 0, 0, 0,
  };
  private final String DISCONTIG_INTERNAL_HOLE_PATH = localTmpDir + "/discontiguous_internal_file.cueball";

  private static final byte[] DISCONTIG_DATA_END = new byte[]{
      0x00, 1,
      0x40, 2,
      0, 0, 0, 0, 0, 0, 0, 0,
      2, 0, 0, 0, 0, 0, 0, 0,
      -1, -1, -1, -1, -1, -1, -1, -1,
      -1, -1, -1, -1, -1, -1, -1, -1,
      2, 0, 0, 0,
      2, 0, 0, 0,
  };
  private final String DISCONTIG_END_PATH = localTmpDir + "/discontiguous_end_file.cueball";

  @Test
  public void testContiguousBlocks() throws Exception {
    final FileOutputStream stream = new FileOutputStream(CONTIG_PATH);
    stream.write(CONTIG_DATA);
    stream.flush();
    stream.close();

    final CueballStreamBuffer sb = new CueballStreamBuffer(CONTIG_PATH, 0, 1, 1, 2, new NoCueballCompressionCodec());
    assertTrue(sb.anyRemaining());
    assertEquals(0, sb.getCurrentOffset());
    assertEquals(ByteBuffer.wrap(CONTIG_DATA, 0, 2), ByteBuffer.wrap(sb.getBuffer(), 0, 2));
    sb.consume();

    assertTrue(sb.anyRemaining());
    assertEquals(0, sb.getCurrentOffset());
    assertEquals(ByteBuffer.wrap(CONTIG_DATA, 2, 2), ByteBuffer.wrap(sb.getBuffer(), 0, 2));
    sb.consume();

    assertTrue(sb.anyRemaining());
    assertEquals(0, sb.getCurrentOffset());
    assertEquals(ByteBuffer.wrap(CONTIG_DATA, 4, 2), ByteBuffer.wrap(sb.getBuffer(), 0, 2));
    sb.consume();

    assertTrue(sb.anyRemaining());
    assertEquals(0, sb.getCurrentOffset());
    assertEquals(ByteBuffer.wrap(CONTIG_DATA, 6, 2), ByteBuffer.wrap(sb.getBuffer(), 0, 2));
    sb.consume();

    assertFalse(sb.anyRemaining());
  }

  @Test
  public void testDiscontiguousInternalBlocks() throws Exception {
    final FileOutputStream stream = new FileOutputStream(DISCONTIG_INTERNAL_HOLE_PATH);
    stream.write(DISCONTIG_DATA_INTERNAL_HOLE);
    stream.flush();
    stream.close();

    final CueballStreamBuffer sb = new CueballStreamBuffer(DISCONTIG_INTERNAL_HOLE_PATH, 0, 1, 1, 2, new NoCueballCompressionCodec());
    assertTrue(sb.anyRemaining());
    assertEquals(0, sb.getCurrentOffset());
    assertEquals(ByteBuffer.wrap(DISCONTIG_DATA_INTERNAL_HOLE, 0, 2), ByteBuffer.wrap(sb.getBuffer(), 0, 2));
    sb.consume();

    assertTrue(sb.anyRemaining());
    assertEquals(0, sb.getCurrentOffset());
    assertEquals(ByteBuffer.wrap(DISCONTIG_DATA_INTERNAL_HOLE, 2, 2), ByteBuffer.wrap(sb.getBuffer(), 0, 2));
    sb.consume();

    assertTrue(sb.anyRemaining());
    assertEquals(0, sb.getCurrentOffset());
    assertEquals(ByteBuffer.wrap(DISCONTIG_DATA_INTERNAL_HOLE, 4, 2), ByteBuffer.wrap(sb.getBuffer(), 0, 2));
    sb.consume();

    assertFalse(sb.anyRemaining());
  }

  @Test
  public void testDiscontiguousEndBlocks() throws Exception {
    final FileOutputStream stream = new FileOutputStream(DISCONTIG_END_PATH);
    stream.write(DISCONTIG_DATA_END);
    stream.flush();
    stream.close();

    final CueballStreamBuffer sb = new CueballStreamBuffer(DISCONTIG_END_PATH, 0, 1, 1, 2, new NoCueballCompressionCodec());
    assertTrue(sb.anyRemaining());
    assertEquals(0, sb.getCurrentOffset());
    assertEquals(ByteBuffer.wrap(DISCONTIG_DATA_END, 0, 2), ByteBuffer.wrap(sb.getBuffer(), 0, 2));
    sb.consume();

    assertTrue(sb.anyRemaining());
    assertEquals(0, sb.getCurrentOffset());
    assertEquals(ByteBuffer.wrap(DISCONTIG_DATA_END, 2, 2), ByteBuffer.wrap(sb.getBuffer(), 0, 2));
    sb.consume();

    assertFalse(sb.anyRemaining());
  }
}
