package com.rapleaf.hank.coordinator;

public abstract class AbstractDomainGroupVersionDomainVersion implements DomainGroupVersionDomainVersion {

  @Override
  public int compareTo(DomainGroupVersionDomainVersion o) {
    return getDomain().getName().compareTo(o.getDomain().getName());
  }

  @Override
  public String toString() {
    return getDomain().getName() + "@v" + getVersionNumber();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    AbstractDomainGroupVersionDomainVersion that = (AbstractDomainGroupVersionDomainVersion) o;

    if (getDomain().getName() != null ? !getDomain().getName().equals(that.getDomain().getName()) : that.getDomain().getName() != null) {
      return false;
    }
    if (getVersionNumber() != that.getVersionNumber()) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = getDomain().getName() != null ? getDomain().getName().hashCode() : 0;
    result = 31 * result + getVersionNumber();
    return result;
  }
}
