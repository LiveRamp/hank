package com.liveramp.hank.util;

import java.util.Comparator;

public class ReverseComparator<T extends Comparable<T>> implements Comparator<T> {
  @Override
  public int compare(T arg0, T arg1) {
    return -1 * arg0.compareTo(arg1);
  }
}
