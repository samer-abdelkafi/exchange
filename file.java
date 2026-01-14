package org.springframework.integration.aws.dsl;

import software.amazon.awssdk.services.s3.S3Client;

import org.springframework.integration.aws.inbound.S3InboundFileSynchronizer;
import org.springframework.integration.aws.inbound.S3InboundFileSynchronizingMessageSource;
import org.springframework.integration.aws.outbound.S3MessageHandler;
import org.springframework.integration.dsl.MessageSourceSpec;
import org.springframework.integration.dsl.MessageHandlerSpec;
import org.springframework.util.Assert;

/**
 * Factory for AWS S3 components in the Spring Integration DSL.
 */
public final class S3 {

    private S3() {
    }

    /**
     * Create an inbound channel adapter spec for S3.
     *
     * @param s3Client the S3 client
     * @param bucket   the S3 bucket
     * @return the MessageSourceSpec
     */
    public static MessageSourceSpec<S3InboundFileSynchronizingMessageSource, ?> inboundAdapter(
            S3Client s3Client, String bucket) {

        Assert.notNull(s3Client, "'s3Client' must not be null");
        Assert.hasText(bucket, "'bucket' must not be empty");

        S3InboundFileSynchronizer synchronizer =
                new S3InboundFileSynchronizer(s3Client, bucket);

        S3InboundFileSynchronizingMessageSource messageSource =
                new S3InboundFileSynchronizingMessageSource(synchronizer);

        return new S3InboundMessageSourceSpec(messageSource);
    }

    /**
     * Create an outbound message handler spec for S3.
     *
     * @param s3Client the S3 client
     * @param bucket   the S3 bucket
     * @return the MessageHandlerSpec
     */
    public static MessageHandlerSpec<S3MessageHandler, ?> outboundAdapter(
            S3Client s3Client, String bucket) {

        Assert.notNull(s3Client, "'s3Client' must not be null");
        Assert.hasText(bucket, "'bucket' must not be empty");

        return new S3MessageHandlerSpec(new S3MessageHandler(s3Client, bucket));
    }

}
