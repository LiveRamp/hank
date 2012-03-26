package com.rapleaf.hank.partition_server;

import com.rapleaf.hank.BaseTestCase;

public class TestDoublePopulationStatisticsAggregator extends BaseTestCase {

  public void testAggregate() {
    DoublePopulationStatisticsAggregator p1 = new DoublePopulationStatisticsAggregator(1, 10, 10, 55, new double[]{1, 2, 3, 4, 5, 6, 7, 8, 9});

    DoublePopulationStatisticsAggregator a = new DoublePopulationStatisticsAggregator();
    a.aggregate(p1);

    assertEquals(1.0, a.getMinimum());
    assertEquals(10.0, a.getMaximum());
    assertEquals(5.5, a.getMean());

    DoublePopulationStatisticsAggregator p2 = new DoublePopulationStatisticsAggregator(11, 20, 10, 155, new double[]{11, 12, 13, 14, 15, 16, 17, 18, 19});
    a.aggregate(p2);

    assertEquals(1.0, a.getMinimum());
    assertEquals(20.0, a.getMaximum());
    assertEquals(10.5, a.getMean());

    double[] deciles = a.getDeciles();
    for (int i = 0; i < 9; ++i) {
      System.out.println(i + ": " + deciles[i]);
    }
    assertEquals(2.0, deciles[0]);
    assertEquals(4.0, deciles[1]);
    assertEquals(6.0, deciles[2]);
    assertEquals(8.0, deciles[3]);
    assertEquals(10.0, deciles[4]);
    assertEquals(12.0, deciles[5]);
    assertEquals(14.0, deciles[6]);
    assertEquals(16.0, deciles[7]);
    assertEquals(18.0, deciles[8]);
  }

  public void testDecile() {
    double[] p = new double[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    assertEquals(1.0, DoublePopulationStatisticsAggregator.getSortedPopulationDecile(p, 1));
    assertEquals(2.0, DoublePopulationStatisticsAggregator.getSortedPopulationDecile(p, 2));
    assertEquals(3.0, DoublePopulationStatisticsAggregator.getSortedPopulationDecile(p, 3));
    assertEquals(4.0, DoublePopulationStatisticsAggregator.getSortedPopulationDecile(p, 4));
    assertEquals(5.0, DoublePopulationStatisticsAggregator.getSortedPopulationDecile(p, 5));
    assertEquals(6.0, DoublePopulationStatisticsAggregator.getSortedPopulationDecile(p, 6));
    assertEquals(7.0, DoublePopulationStatisticsAggregator.getSortedPopulationDecile(p, 7));
    assertEquals(8.0, DoublePopulationStatisticsAggregator.getSortedPopulationDecile(p, 8));
    assertEquals(9.0, DoublePopulationStatisticsAggregator.getSortedPopulationDecile(p, 9));
  }

  public void testInterpolateWithinSegment() {
    double v = 2.0;
    DoublePopulationStatisticsAggregator.ValueAndCount vac = new DoublePopulationStatisticsAggregator.ValueAndCount(4.0, 10);
    assertEquals(2.2, DoublePopulationStatisticsAggregator.interpolateValueWithinValueAndCounts(v, vac, 0));
    assertEquals(2.4, DoublePopulationStatisticsAggregator.interpolateValueWithinValueAndCounts(v, vac, 1));
    assertEquals(2.6, DoublePopulationStatisticsAggregator.interpolateValueWithinValueAndCounts(v, vac, 2));
    assertEquals(4.0, DoublePopulationStatisticsAggregator.interpolateValueWithinValueAndCounts(v, vac, 9));
  }

  public void testInterpolatedDecile() {
    double[] p = new double[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    assertEquals(1.0, DoublePopulationStatisticsAggregator.getInterpolatedValueAtIndex(p, 0.0));
    assertEquals(1.1, DoublePopulationStatisticsAggregator.getInterpolatedValueAtIndex(p, 0.1));
    assertEquals(3.9, DoublePopulationStatisticsAggregator.getInterpolatedValueAtIndex(p, 2.9));
  }
}
