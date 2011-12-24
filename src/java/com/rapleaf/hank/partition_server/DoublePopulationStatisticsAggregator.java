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

  public void aggregate(double minimum, double maximum, double numValues, double total, double[] deciles) {
    if (maximum > this.maximum) {
      this.maximum = maximum;
    }
    if (minimum < this.minimum) {
      this.minimum = minimum;
    }
    this.numValues += numValues;
    this.total += total;
    for (int i = 0; i < 9; ++i) {
      // Note: do a weighted sum of deciles
      this.deciles[i] += numValues * deciles[i];
    }
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
    for (int i = 0; i < 9; ++i) {
      // Note: other deciles are already weighted sums, so just sum without multiplying
      this.deciles[i] += other.deciles[i];
    }
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
    double[] result = new double[]{0, 0, 0, 0, 0, 0, 0, 0, 0};
    if (numValues > 0) {
      for (int i = 0; i < 9; ++i) {
        result[i] = deciles[i] / numValues;
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
    double median = numValues == 0 ? 0 : deciles[4] / numValues;
    result.append(formatDouble(median));
    result.append(" / ");
    result.append(formatDouble(getMean()));
    result.append(" / ");
    result.append(formatDouble(maximum));
    return result.toString();
  }
}
