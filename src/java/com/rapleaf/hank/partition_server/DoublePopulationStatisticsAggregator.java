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
import java.util.Random;

public class DoublePopulationStatisticsAggregator {

  private static int POPULATION_RANDOM_SAMPLE_MAX_SIZE = 100;

  private static DecimalFormat format3 = new DecimalFormat("#.###");
  private static DecimalFormat format1 = new DecimalFormat("#.#");
  private static DecimalFormat format0 = new DecimalFormat("#");

  private double minimum;
  private double maximum;
  private long numValues;
  private double total;
  private final ReservoirSample reservoirSample = new ReservoirSample(POPULATION_RANDOM_SAMPLE_MAX_SIZE);
  private final Random random = new Random();

  public DoublePopulationStatisticsAggregator() {
    clear();
  }

  public DoublePopulationStatisticsAggregator(double minimum,
                                              double maximum,
                                              long numValues,
                                              double total,
                                              double[] randomSample) {
    this.minimum = minimum;
    this.maximum = maximum;
    this.numValues = numValues;
    this.total = total;
    this.reservoirSample.sample(randomSample, random);
  }

  public void clear() {
    minimum = Double.MAX_VALUE;
    maximum = Double.MIN_VALUE;
    numValues = 0;
    total = 0.0;
    reservoirSample.clear();
  }

  public void aggregate(double minimum, double maximum, long numValues, double total, double[] randomSample) {
    if (maximum > this.maximum) {
      this.maximum = maximum;
    }
    if (minimum < this.minimum) {
      this.minimum = minimum;
    }
    this.numValues += numValues;
    this.total += total;
    this.reservoirSample.sample(randomSample, random);
  }

  public void aggregate(DoublePopulationStatisticsAggregator other) {
    if (other.maximum > this.maximum) {
      this.maximum = other.maximum;
    }
    if (other.minimum < this.minimum) {
      this.minimum = other.minimum;
    }
    this.numValues += other.numValues;
    this.total += other.total;
    this.reservoirSample.sample(other.reservoirSample, random);
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

  public double[] computeDeciles() {
    double[] result = new double[9];
    Arrays.fill(result, 0.0);
    if (reservoirSample.getSize() > 0) {
      // Sort valid reservoir values first
      Arrays.sort(reservoirSample.getReservoir(), 0, reservoirSample.getSize());
      // Compute deciles
      for (int i = 0; i < 9; ++i) {
        result[i] = getSortedPopulationDecile(reservoirSample.getReservoir(), i + 1, reservoirSample.getSize());
      }
    }
    return result;
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
    for (int i = 0; i < populationStatistics.reservoirSample.getSize(); ++i) {
      result.append(' ');
      result.append(populationStatistics.reservoirSample.getReservoir()[i]);
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
    double[] deciles = computeDeciles();
    StringBuilder result = new StringBuilder();
    // Compute median
    double median = numValues == 0 ? 0 : deciles[4];
    // Compute 90% percentile
    double ninetiethPercentile = numValues == 0 ? 0 : deciles[8];
    result.append(formatDouble(getMean()));
    result.append(" / ");
    result.append(formatDouble(median));
    result.append(" / ");
    result.append(formatDouble(ninetiethPercentile));
    result.append(" ms");
    return result.toString();
  }

  public class ReservoirSample {

    private final double[] reservoir;
    private int size;
    private int count;

    public ReservoirSample(int reservoirMaxSize) {
      reservoir = new double[reservoirMaxSize];
      clear();
    }

    public void sample(double[] values, Random random) {
      for (double value : values) {
        sample(value, random);
      }
    }

    public void sample(double value, Random random) {
      if (count < reservoir.length) {
        reservoir[size++] = value;
      } else {
        if (random.nextInt(count) < reservoir.length) {
          reservoir[random.nextInt(reservoir.length)] = value;
        }
      }
      ++count;
    }

    public void sample(ReservoirSample other, Random random) {
      for (int i = 0; i < other.getSize(); ++i) {
        sample(other.getReservoir()[i], random);
      }
    }

    public double[] getReservoir() {
      return reservoir;
    }

    public int getSize() {
      return size;
    }

    public void clear() {
      size = 0;
      count = 0;
    }
  }

  public static double getSortedPopulationDecile(double[] population, int decile, int endIndex) {
    return getInterpolatedValueAtIndex(population, getDecileIndex(endIndex, decile));
  }

  public static double getSortedPopulationDecile(long[] population, int decile, int endIndex) {
    return getInterpolatedValueAtIndex(population, getDecileIndex(endIndex, decile));
  }

  public static double getSortedPopulationDecile(double[] population, int decile) {
    return getInterpolatedValueAtIndex(population, getDecileIndex(population.length, decile));
  }

  public static double getSortedPopulationDecile(long[] population, int decile) {
    return getInterpolatedValueAtIndex(population, getDecileIndex(population.length, decile));
  }

  public static double getDecileIndex(long size, int decile) {
    if (decile < 1 || decile > 9) {
      throw new RuntimeException("Invalid decile: " + decile);
    }
    return ((size - 1) / 10.0) * decile;
  }

  public static double getInterpolatedValueAtIndex(double[] population, double rank) {
    double rankFloored = Math.floor(rank);
    double remainder = rank - rankFloored;

    if (remainder == 0 || ((int) rankFloored) == population.length - 1) {
      return population[(int) rankFloored];
    }

    // Return interpolated value at index
    return population[(int) rankFloored] + (remainder * (population[(int) rankFloored + 1] - population[(int) rankFloored]));
  }

  public static double getInterpolatedValueAtIndex(long[] population, double rank) {
    double rankFloored = Math.floor(rank);
    double remainder = rank - rankFloored;

    if (remainder == 0 || ((int) rankFloored) == population.length - 1) {
      return population[(int) rankFloored];
    }

    // Return interpolated value at index
    return population[(int) rankFloored] + (remainder * (population[(int) rankFloored + 1] - population[(int) rankFloored]));
  }
}
