package org.springframework.integration.aws.dsl;

import org.springframework.integration.aws.inbound.S3InboundFileSynchronizingMessageSource;
import org.springframework.integration.dsl.MessageSourceSpec;

public class S3InboundMessageSourceSpec
        extends MessageSourceSpec<S3InboundMessageSourceSpec, S3InboundFileSynchronizingMessageSource> {

    S3InboundMessageSourceSpec(S3InboundFileSynchronizingMessageSource source) {
        super(source);
    }

    public S3InboundMessageSourceSpec remoteDirectory(String prefix) {
        this.target.getSynchronizer().setRemoteDirectory(prefix);
        return this;
    }

    public S3InboundMessageSourceSpec prefix(String prefix) {
        this.target.getSynchronizer().setRemoteDirectory(prefix);
        return this;
    }

    public S3InboundMessageSourceSpec deleteSourceFiles(boolean delete) {
        this.target.getSynchronizer().setDeleteRemoteFiles(delete);
        return this;
    }

    public S3InboundMessageSourceSpec preventDuplicates(boolean preventDuplicates) {
        this.target.setAutoCreateLocalDirectory(true);
        this.target.getSynchronizer().setPreserveTimestamp(preventDuplicates);
        return this;
    }
}
