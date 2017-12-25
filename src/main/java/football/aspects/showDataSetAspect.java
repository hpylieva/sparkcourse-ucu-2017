package football.aspects;

import org.apache.spark.sql.Dataset;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.AfterReturning;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import static football.constants.ConstProfiles.DEV;


@Component
@Aspect
@Profile(DEV)
public class showDataSetAspect {

    @AfterReturning(value = "@annotation(ShowDataSetInTheEnd)", returning = "dataset")
    public void showDataSetInTheEnd(JoinPoint jp, Dataset dataset){

        System.out.println("ShowDataSetInTheEnd aspect is working...");
        printToConsole(jp, dataset);
        System.out.println("ShowDataSetInTheEnd aspect ended printing...");
    }

    private void printToConsole(JoinPoint jp, Dataset dataset) {
        String className = jp.getClass().getSimpleName();
        String methodName = jp.getSignature().getName();
        System.out.println("\n===========BEGIN============= class:  "+className+"  method:  "+methodName+ "   =============="  );
        dataset.show();
        System.out.println("\n===========END============= class:  "+className+"  method:  "+methodName+ "   =============="  );
    }
}
