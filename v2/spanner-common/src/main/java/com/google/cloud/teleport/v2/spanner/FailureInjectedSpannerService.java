package com.google.cloud.teleport.v2.spanner;

import com.google.cloud.ServiceFactory;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;

public class FailureInjectedSpannerService implements ServiceFactory<Spanner, SpannerOptions> {


  @Override
  public Spanner create(SpannerOptions spannerOptions) {
    return null;
  }
}
