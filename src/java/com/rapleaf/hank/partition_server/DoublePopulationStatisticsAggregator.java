/**
 *  Copyright 2011 Rapleaf
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.rapleaf.hank.partition_server;

import java.text.DecimalFormat;
import java.util.Arrays;

public class DoublePopulationStatisticsAggregator {

  private static DecimalFormat format3 = new DecimalFormat("#.###");
  private static DecimalFormat format1 = new DecimalFormat("#.#");
  private static DecimalFormat format0 = new DecimalFormat("#");

  private double minimum;
  private double maximum;
  private long numValues;
  private double total;
  private double[] deciles;

  public DoublePopulationStatisticsAggregator() {
    minimum = Double.MAX_VALUE;
    maximum = Double.MIN_VALUE;
    numValues = 0;
    total = 0.0;
    deciles = new double[]{0, 0, 0, 0, 0, 0, 0, 0, 0};
  }

  public DoublePopulationStatisticsAggregator(double minimum,
                                              double maximum,
                                              long numValues,
                                              double total,
                                              double[] deciles) {
    if (deciles.length != 9) {
      throw new RuntimeException("Invalid population statistics: num deciles =" + deciles.length);
    }
    this.minimum = minimum;
    this.maximum = maximum;
    this.numValues = numValues;
    this.total = total;
    this.deciles = deciles;
  }

  public void clear() {
    minimum = Double.MAX_VALUE;
    maximum = Double.MIN_VALUE;
    numValues = 0;
    total = 0.0;
    Arrays.fill(deciles, 0);
  }

  public void aggregate(double minimum, double maximum, long numValues, double total, double[] deciles) {
    if (numValues < 10 || deciles.length != 9) {
      throw new RuntimeException("Invalid population statistics to aggregate. numValues="
          + numValues + ", deciles.length=" + deciles.length);
    }
    if (this.numValues == 0) {
      // Copy deciles directly
      System.arraycopy(deciles, 0, this.deciles, 0, 9);
    } else if (numValues == 0) {
      // Do nothing
    } else {
      aggregateDeciles(this.deciles, this.numValues, this.getMaximum(),
          deciles, numValues, maximum,
          this.deciles);
    }
    if (maximum > this.maximum) {
      this.maximum = maximum;
    }
    if (minimum < this.minimum) {
      this.minimum = minimum;
    }
    this.numValues += numValues;
    this.total += total;
  }

  public void aggregate(DoublePopulationStatisticsAggregator other) {
    if (numValues == 0) {
      // Copy deciles directly
      System.arraycopy(other.deciles, 0, deciles, 0, 9);
    } else if (other.numValues == 0) {
      // Keep this deciles unchanged
    } else {
      // Aggregate both deciles
      aggregateDeciles(this.deciles, this.numValues, this.getMaximum(),
          other.deciles, other.numValues, other.getMaximum(),
          this.deciles);
    }
    if (other.maximum > this.maximum) {
      this.maximum = other.maximum;
    }
    if (other.minimum < this.minimum) {
      this.minimum = other.minimum;
    }
    this.numValues += other.numValues;
    this.total += other.total;
  }

  private static void aggregateDeciles(double[] decilesA, long numValuesA, double maximumA,
                                       double[] decilesB, long numValuesB, double maximumB,
                                       double[] outputDeciles) {
    // Compute all deciles
    ValueAndCount[] allDeciles = new ValueAndCount[2 * 10];
    // Load all decile values and counts into one array
    for (int i = 0; i < 9; ++i) {
      allDeciles[i] = new ValueAndCount(decilesA[i], numValuesA);
      allDeciles[i + 9] = new ValueAndCount(decilesB[i], numValuesB);
    }

    // Fake deciles to fill in voids
    allDeciles[18] = new ValueAndCount(maximumA, numValuesA);
    allDeciles[19] = new ValueAndCount(maximumB, numValuesB);

    // Sort all deciles
    Arrays.sort(allDeciles);

    // Update deciles
    final long numValuesCombined = 10 * (numValuesA + numValuesB);
    long previousRank = 0;
    int j = 0;
    for (int i = 0; i < 9; ++i) {
      final long decileRank = Math.round(getDecileRank(numValuesCombined, i + 1));
      while (previousRank + allDeciles[j].count < decileRank) {
        previousRank += allDeciles[j].count;
        j += 1;
      }

      // j point to the current segment where the correct decile rank lies
      if (j == 0) {
        // For the first decile, consider all value to be equal (no interpolation)
        outputDeciles[i] = allDeciles[j].value;
      } else {
        // Interpolate value for other deciles
        outputDeciles[i] =
            interpolateValueWithinValueAndCounts(allDeciles[j - 1].value,
                allDeciles[j],
                decileRank - previousRank);
      }
    }
  }

  public static double interpolateValueWithinValueAndCounts(double value, ValueAndCount segment, long indexInSegment) {
    double remainder = (double) (indexInSegment + 1) / (double) segment.count;
    return value + remainder * (segment.value - value);
  }

  public Double getMaximum() {
    if (maximum == Double.MIN_VALUE) {
      return null;
    }
    return maximum;
  }

  public Double getMinimum() {
    if (minimum == Double.MAX_VALUE) {
      return null;
    }
    return minimum;
  }

  public double getMean() {
    if (numValues != 0) {
      return total / numValues;
    } else {
      return 0;
    }
  }

  public double[] getDeciles() {
    return deciles;
  }

  public static String toString(DoublePopulationStatisticsAggregator populationStatistics) {
    StringBuilder result = new StringBuilder();
    result.append(populationStatistics.minimum);
    result.append(' ');
    result.append(populationStatistics.maximum);
    result.append(' ');
    result.append(populationStatistics.numValues);
    result.append(' ');
    result.append(populationStatistics.total);
    for (int i = 0; i < 9; ++i) {
      result.append(' ');
      result.append(populationStatistics.deciles[i]);
    }
    return result.toString();
  }

  public static String formatDouble(double value) {
    if (value < 1) {
      return format3.format(value);
    } else if (value < 100) {
      return format1.format(value);
    } else {
      return format0.format(value);
    }
  }

  public String format() {
    StringBuilder result = new StringBuilder();
    // Compute median on the fly
    double median = numValues == 0 ? 0 : deciles[4];
    // Compute 90% percentile on the fly
    double ninetiethPercentile = numValues == 0 ? 0 : deciles[8];
    result.append(formatDouble(getMean()));
    result.append(" / ");
    result.append(formatDouble(median));
    result.append(" / ");
    result.append(formatDouble(ninetiethPercentile));
    result.append(" ms");
    return result.toString();
  }

  public static class ValueAndCount implements Comparable<ValueAndCount> {

    public final double value;
    public final long count;

    public ValueAndCount(double value, long count) {
      this.value = value;
      this.count = count;
    }

    @Override
    public int compareTo(DoublePopulationStatisticsAggregator.ValueAndCount other) {
      return Double.compare(value, other.value);
    }

    @Override
    public String toString() {
      return value + "(" + count + ")";
    }
  }

  public static double getSortedPopulationDecile(double[] population, int decile) {
    return getInterpolatedValueAtIndex(population, getDecileRank(population.length, decile));
  }

  public static double getSortedPopulationDecile(long[] population, int decile) {
    return getInterpolatedValueAtIndex(population, getDecileRank(population.length, decile));
  }

  public static double getDecileRank(long size, int decile) {
    if (decile < 1 || decile > 9) {
      throw new RuntimeException("Invalid decile: " + decile);
    }
    if (size < 10) {
      throw new RuntimeException("Population is too small to compute deciles. Size: " + size);
    }
    return ((size / 10.0) * decile) - 1;
  }

  public static double getInterpolatedValueAtIndex(double[] population, double rank) {
    double rankFloored = Math.floor(rank);
    double remainder = rank - rankFloored;

    // Return interpolated value at index
    return population[(int) rankFloored] + (remainder * (population[(int) rankFloored + 1] - population[(int) rankFloored]));
  }

  public static double getInterpolatedValueAtIndex(long[] population, double rank) {
    double rankFloored = Math.floor(rank);
    double remainder = rank - rankFloored;

    // Return interpolated value at index
    return population[(int) rankFloored] + (remainder * (population[(int) rankFloored + 1] - population[(int) rankFloored]));
  }
}
