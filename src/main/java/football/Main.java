package football;

import football.configs.MainConfig;
import football.services.DatasetCreator;
import football.services.EnrichmentService;
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
        EnrichmentService enrichmentService = context.getBean(EnrichmentService.class);

        ds.createDataset().show();
        enrichmentService.enrich();

    }

}
