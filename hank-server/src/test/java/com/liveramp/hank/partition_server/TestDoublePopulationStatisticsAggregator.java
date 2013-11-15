package com.liveramp.hank.partition_server;

import com.liveramp.hank.test.BaseTestCase;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestDoublePopulationStatisticsAggregator extends BaseTestCase {

  @Test
  public void testDecileIndexSingleton() {
    assertEquals(0.0, DoublePopulationStatisticsAggregator.getDecileIndex(1, 1), .0001);
    assertEquals(0.0, DoublePopulationStatisticsAggregator.getDecileIndex(1, 2), .0001);
    assertEquals(0.0, DoublePopulationStatisticsAggregator.getDecileIndex(1, 3), .0001);
    assertEquals(0.0, DoublePopulationStatisticsAggregator.getDecileIndex(1, 4), .0001);
    assertEquals(0.0, DoublePopulationStatisticsAggregator.getDecileIndex(1, 5), .0001);
    assertEquals(0.0, DoublePopulationStatisticsAggregator.getDecileIndex(1, 6), .0001);
    assertEquals(0.0, DoublePopulationStatisticsAggregator.getDecileIndex(1, 7), .0001);
    assertEquals(0.0, DoublePopulationStatisticsAggregator.getDecileIndex(1, 8), .0001);
    assertEquals(0.0, DoublePopulationStatisticsAggregator.getDecileIndex(1, 9), .0001);
  }

  @Test
  public void testDecileIndex() {
    assertEquals(0.1, DoublePopulationStatisticsAggregator.getDecileIndex(2, 1), .0001);
    assertEquals(0.2, DoublePopulationStatisticsAggregator.getDecileIndex(2, 2), .0001);
    assertEquals((1.0 / 10.0) * 3.0, DoublePopulationStatisticsAggregator.getDecileIndex(2, 3), .0001);
    assertEquals(0.9, DoublePopulationStatisticsAggregator.getDecileIndex(2, 9), .0001);
  }

  @Test
  public void testDecile() {
    double[] p = new double[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11};
    assertEquals(2.0, DoublePopulationStatisticsAggregator.getSortedPopulationDecile(p, 1), .0001);
    assertEquals(3.0, DoublePopulationStatisticsAggregator.getSortedPopulationDecile(p, 2), .0001);
    assertEquals(4.0, DoublePopulationStatisticsAggregator.getSortedPopulationDecile(p, 3), .0001);
    assertEquals(5.0, DoublePopulationStatisticsAggregator.getSortedPopulationDecile(p, 4), .0001);
    assertEquals(6.0, DoublePopulationStatisticsAggregator.getSortedPopulationDecile(p, 5), .0001);
    assertEquals(7.0, DoublePopulationStatisticsAggregator.getSortedPopulationDecile(p, 6), .0001);
    assertEquals(8.0, DoublePopulationStatisticsAggregator.getSortedPopulationDecile(p, 7), .0001);
    assertEquals(9.0, DoublePopulationStatisticsAggregator.getSortedPopulationDecile(p, 8), .0001);
    assertEquals(10.0, DoublePopulationStatisticsAggregator.getSortedPopulationDecile(p, 9), .0001);
  }

  @Test
  public void testDecileSingleton() {
    double[] p = new double[]{4};
    assertEquals(4.0, DoublePopulationStatisticsAggregator.getSortedPopulationDecile(p, 1), .0001);
    assertEquals(4.0, DoublePopulationStatisticsAggregator.getSortedPopulationDecile(p, 2), .0001);
    assertEquals(4.0, DoublePopulationStatisticsAggregator.getSortedPopulationDecile(p, 3), .0001);
    assertEquals(4.0, DoublePopulationStatisticsAggregator.getSortedPopulationDecile(p, 4), .0001);
    assertEquals(4.0, DoublePopulationStatisticsAggregator.getSortedPopulationDecile(p, 5), .0001);
    assertEquals(4.0, DoublePopulationStatisticsAggregator.getSortedPopulationDecile(p, 6), .0001);
    assertEquals(4.0, DoublePopulationStatisticsAggregator.getSortedPopulationDecile(p, 7), .0001);
    assertEquals(4.0, DoublePopulationStatisticsAggregator.getSortedPopulationDecile(p, 8), .0001);
    assertEquals(4.0, DoublePopulationStatisticsAggregator.getSortedPopulationDecile(p, 9), .0001);
  }

  @Test
  public void testDecileDuet() {
    double[] p = new double[]{4, 6};
    assertEquals(4.2, DoublePopulationStatisticsAggregator.getSortedPopulationDecile(p, 1), .0001);
    assertEquals(4.4, DoublePopulationStatisticsAggregator.getSortedPopulationDecile(p, 2), .0001);
    assertEquals(4.6, DoublePopulationStatisticsAggregator.getSortedPopulationDecile(p, 3), .0001);
    assertEquals(4.8, DoublePopulationStatisticsAggregator.getSortedPopulationDecile(p, 4), .0001);
    assertEquals(5.0, DoublePopulationStatisticsAggregator.getSortedPopulationDecile(p, 5), .0001);
    assertEquals(5.2, DoublePopulationStatisticsAggregator.getSortedPopulationDecile(p, 6), .0001);
    assertEquals(5.4, DoublePopulationStatisticsAggregator.getSortedPopulationDecile(p, 7), .0001);
    assertEquals(5.6, DoublePopulationStatisticsAggregator.getSortedPopulationDecile(p, 8), .0001);
    assertEquals(5.8, DoublePopulationStatisticsAggregator.getSortedPopulationDecile(p, 9), .0001);
  }

  @Test
  public void testInterpolatedDecile() {
    double[] p = new double[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    assertEquals(1.0, DoublePopulationStatisticsAggregator.getInterpolatedValueAtIndex(p, 0.0), .0001);
    assertEquals(1.1, DoublePopulationStatisticsAggregator.getInterpolatedValueAtIndex(p, 0.1), .0001);
    assertEquals(3.9, DoublePopulationStatisticsAggregator.getInterpolatedValueAtIndex(p, 2.9), .0001);
  }
}
