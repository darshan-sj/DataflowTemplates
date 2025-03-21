/*
 * Copyright (C) 2025 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.v2.templates.spanner;

import com.google.cloud.spanner.BatchClient;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.TransactionRunner.TransactionCallable;
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
import org.junit.Before;
import org.junit.Test;

public class SpannerTest {

  /** Injects errors in streaming calls to simulate call restarts. */
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
      // if (method.getFullMethodName().startsWith("google.spanner.v1.Spanner/BatchCreateSessions"))
      // {
      //   return next.newCall(method, callOptions);
      // }

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

  DatabaseClient databaseClient;
  BatchClient batchClient;
  DatabaseAdminClient databaseAdminClient;

  @Before
  public void setup() {
    String projectId = "span-cloud-testing";
    String instanceId = "djagaluru-dms-test";
    String databaseId = "temp-djagaluru";
    double errorProbability = 0.3;
    SpannerOptions.Builder builder =
        SpannerOptions.newBuilder()
            .setAutoThrottleAdministrativeRequests()
            .setTrackTransactionStarter();

    SpannerInterceptorProvider interceptorProvider =
        SpannerInterceptorProvider.createDefault().with(new GrpcErrorInjector(errorProbability));

    builder.setInterceptorProvider(interceptorProvider);
    // DirectPath tests need to set a custom endpoint to the ChannelProvider
    // InstantiatingGrpcChannelProvider.Builder customChannelProviderBuilder =
    //     InstantiatingGrpcChannelProvider.newBuilder();
    // if (attemptDirectPath) {
    //   customChannelProviderBuilder
    //       .setEndpoint(DIRECT_PATH_ENDPOINT)
    //       .setAttemptDirectPath(true)
    //       .setAttemptDirectPathXds()
    //       .setInterceptorProvider(interceptorProvider);
    //   builder.setChannelProvider(customChannelProviderBuilder.build());
    // }
    builder.setProjectId(projectId);
    SpannerOptions options = builder.build();
    Spanner spanner = options.getService();

    databaseClient = spanner.getDatabaseClient(DatabaseId.of(projectId, instanceId, databaseId));
    batchClient = spanner.getBatchClient(DatabaseId.of(projectId, instanceId, databaseId));
    databaseAdminClient = spanner.getDatabaseAdminClient();
  }

  @Test
  public void testInterleaveInsertChildBeforeParent() {

    Mutation mutation =
        Mutation.newInsertBuilder("Books")
            .set("id")
            .to(4)
            .set("author_id")
            .to(100)
            .set("title")
            .to("Child")
            .set("synth_id")
            .to("bcd")
            .build();
    try {
      databaseClient
          .readWriteTransaction()
          .run(
              (TransactionCallable<Void>)
                  transaction -> {
                    transaction.buffer(mutation);
                    return null;
                  });
    } catch (SpannerException e) {
      throw e;
    }
  }
}
