package football;

import football.configs.MainConfig;
import football.services.DatasetCreator;
import football.services.EnrichmentService;
import football.services.ValidationService;
import org.apache.spark.sql.Dataset;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import static football.constants.ConstProfiles.PROD;

public class Main {

    public static void main(String[] args) {

        System.setProperty("spring.profiles.active", PROD);
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(MainConfig.class);

        //JavaSparkContext sc = context.getBean(JavaSparkContext.class);
        DatasetCreator ds = context.getBean(DatasetCreator.class);
        ValidationService validationService = context.getBean(ValidationService.class);
        EnrichmentService enrichmentService = context.getBean(EnrichmentService.class);

        System.out.println("Raw file was successfully loaded.");
        Dataset dataset = ds.createDataset();

        dataset = validationService.validate(dataset);
        System.out.println("Validation successfully finished.");

        dataset = enrichmentService.enrich(dataset);
        System.out.println("Enrichment successfully finished.");

        System.out.println("\n=============================Final dataset=============================\n");
        dataset.show();

        context.close();

    }

}
