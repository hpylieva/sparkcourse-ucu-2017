package football;

import football.configs.MainConfig;
import football.services.DatasetCreator;
import football.services.EnrichmentService;
import football.services.ValidationService;
import org.apache.spark.sql.Dataset;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import static football.configs.spark_configs.ConstProfiles.PROD;

public class Main {

    public static void main(String[] args) {
       // System.setProperty("hadoop.home.dir", "C:\\util\\hadoop-common-2.2.0-bin-master\\");
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

        context.close();

    }

}
