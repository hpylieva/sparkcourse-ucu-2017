package football;

import football.configs.MainConfig;
import football.services.DatasetCreator;
import football.services.EnrichmentService;
import football.services.ValidationService;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SQLContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import static org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK;

public class Main {

    public static void main(String[] args) {
       // System.setProperty("hadoop.home.dir", "C:\\util\\hadoop-common-2.2.0-bin-master\\");
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(MainConfig.class);

        JavaSparkContext sc = context.getBean(JavaSparkContext.class);
        DatasetCreator ds = context.getBean(DatasetCreator.class);
        ValidationService validationService = context.getBean(ValidationService.class);
        EnrichmentService enrichmentService = context.getBean(EnrichmentService.class);

        System.out.println("Raw file was loaded...");
        Dataset dataset = ds.createDataset();
        dataset.show();

        System.out.println("Validation finished ...");
        dataset = validationService.validate(dataset);

        System.out.println("Enrichment finished ...");
        dataset = enrichmentService.enrich(dataset);

    }

}
