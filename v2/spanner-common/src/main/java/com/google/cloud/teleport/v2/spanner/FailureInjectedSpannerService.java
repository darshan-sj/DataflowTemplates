package com.google.cloud.teleport.v2.spanner;

import com.google.cloud.ServiceFactory;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.spi.v1.SpannerInterceptorProvider;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall.SimpleForwardingClientCall;
import io.grpc.ForwardingClientCallListener.SimpleForwardingClientCallListener;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

public class FailureInjectedSpannerService implements ServiceFactory<Spanner, SpannerOptions> {

  /** Injects errors in streaming calls to simulate call restarts */
  private static class GrpcErrorInjector implements ClientInterceptor {

    private final double errorProbability;
    private final Random random = new Random();

    GrpcErrorInjector(double errorProbability) {
      this.errorProbability = errorProbability;
    }

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
        final MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
      // Only inject errors in the Cloud Spanner data API.
      if (!method.getFullMethodName().startsWith("google.spanner.v1.Spanner")) {
        return next.newCall(method, callOptions);
      }
      if (method.getFullMethodName().startsWith("google.spanner.v1.Spanner/BatchCreateSessions")) {
        return next.newCall(method, callOptions);
      }

      final AtomicBoolean errorInjected = new AtomicBoolean();
      final ClientCall<ReqT, RespT> clientCall = next.newCall(method, callOptions);

      return new SimpleForwardingClientCall<ReqT, RespT>(clientCall) {
        @Override
        public void start(Listener<RespT> responseListener, Metadata headers) {
          super.start(
              new SimpleForwardingClientCallListener<RespT>(responseListener) {
                @Override
                public void onMessage(RespT message) {
                  super.onMessage(message);
                  if (mayInjectError()) {
                    // Cancel the call after at least one response has been received.
                    // This will cause the call to terminate, then we can set UNAVAILABLE
                    // in the onClose() handler to cause a retry.
                    errorInjected.set(true);
                    clientCall.cancel("Cancelling call for injected error", null);
                  }
                }

                @Override
                public void onClose(Status status, Metadata metadata) {
                  if (errorInjected.get()) {
                    // UNAVAILABLE error will cause the call to retry.
                    status = Status.OUT_OF_RANGE.augmentDescription("INJECTED BY TEST");
                  }
                  super.onClose(status, metadata);
                }
              },
              headers);
        }
      };
    }

    private boolean mayInjectError() {
      return random.nextDouble() < errorProbability;
    }
  }

  @Override
  public Spanner create(SpannerOptions spannerOptions) {
    double errorProbability = 1.0;

    SpannerInterceptorProvider interceptorProvider =
        SpannerInterceptorProvider.createDefault().with(new GrpcErrorInjector(errorProbability));

    spannerOptions.toBuilder().setInterceptorProvider(interceptorProvider);

    return spannerOptions.getService();
  }
}
