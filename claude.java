import com.amazonaws.services.s3.AmazonS3;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.aws.inbound.S3InboundFileSynchronizer;
import org.springframework.integration.aws.inbound.S3InboundFileSynchronizingMessageSource;
import org.springframework.integration.aws.outbound.S3MessageHandler;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.dsl.Pollers;
import org.springframework.integration.file.filters.AcceptOnceFileListFilter;
import org.springframework.integration.file.filters.CompositeFileListFilter;
import org.springframework.integration.file.filters.RegexPatternFileListFilter;

import java.io.File;

@Configuration
public class S3IntegrationConfig {

    @Bean
    public S3InboundFileSynchronizer s3InboundFileSynchronizer(AmazonS3 amazonS3) {
        S3InboundFileSynchronizer synchronizer = new S3InboundFileSynchronizer(amazonS3);
        synchronizer.setRemoteDirectory("input-bucket");
        
        // Filtre pour les fichiers CSV
        synchronizer.setFilter(new RegexPatternFileListFilter(".*\\.csv"));
        
        return synchronizer;
    }

    @Bean
    public S3InboundFileSynchronizingMessageSource s3MessageSource(
            S3InboundFileSynchronizer s3InboundFileSynchronizer) {
        
        S3InboundFileSynchronizingMessageSource messageSource = 
            new S3InboundFileSynchronizingMessageSource(s3InboundFileSynchronizer);
        
        // Répertoire local temporaire
        messageSource.setLocalDirectory(new File("temp/s3-downloads"));
        messageSource.setAutoCreateLocalDirectory(true);
        
        // Filtre pour éviter de retraiter les mêmes fichiers
        CompositeFileListFilter<File> compositeFilter = new CompositeFileListFilter<>();
        compositeFilter.addFilter(new AcceptOnceFileListFilter<>());
        compositeFilter.addFilter(new RegexPatternFileListFilter(".*\\.csv"));
        messageSource.setLocalFilter(compositeFilter);
        
        return messageSource;
    }

    @Bean
    public S3MessageHandler s3MessageHandler(AmazonS3 amazonS3) {
        S3MessageHandler messageHandler = new S3MessageHandler(amazonS3, "output-bucket");
        messageHandler.setObjectKeyExpression(
            new org.springframework.expression.common.LiteralExpression("processed-files/")
        );
        return messageHandler;
    }

    @Bean
    public IntegrationFlow s3Flow(S3InboundFileSynchronizingMessageSource s3MessageSource,
                                   S3MessageHandler s3MessageHandler) {
        return IntegrationFlows
            .from(s3MessageSource, c -> c.poller(Pollers.fixedDelay(5000)))
            .handle(msg -> {
                File file = (File) msg.getPayload();
                System.out.println("Processing file: " + file.getName());
                
                // Votre logique de traitement CSV ici
                // Par exemple avec OpenCSV ou autre
                
            })
            .handle(s3MessageHandler)
            .get();
    }

    @Bean
    public AmazonS3 amazonS3() {
        // Configuration de votre client S3
        return AmazonS3ClientBuilder.standard()
            .withRegion("eu-west-1") // Votre région
            .build();
    }
}
