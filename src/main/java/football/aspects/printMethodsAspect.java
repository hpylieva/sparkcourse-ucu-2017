package football.aspects;


import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.After;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

/* Really wanted to make the second Aspect work, but I gain nonSerializable exception for all methods
which perform dataset.show(). Due to time expiration didn't found the solution.
 */


@Component
@Aspect
@Profile("UndefinedYet")
public class printMethodsAspect {

    @Before("execution(* football.services.validators.*.validate(..))")
    public void beforeValidationMethods(JoinPoint jp) {
        System.out.println("Performing  "+jp.getTarget().getClass().getSimpleName() + " validation...");
    }

    @After("execution(* football.services.validators.*.validate(..))")
    public void afterValidationMethods(JoinPoint jp) {
        System.out.println("Validation  "+jp.getTarget().getClass().getSimpleName() + " has finished.");
    }

    @Before("execution(* football.services.enrichers.*.*(..))")
    public void beforeEnrichmentMethods(JoinPoint jp) {
        System.out.println("Performing  "+jp.getTarget().getClass().getSimpleName() + " enrichment...");
    }

    @After("execution(* football.services.enrichers.*.*(..))")
    public void afterEnrichmentMethods(JoinPoint jp) {
        System.out.println("Enrichment  "+jp.getTarget().getClass().getSimpleName() + " was successfully done.");
    }
}
