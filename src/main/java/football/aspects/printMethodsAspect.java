package football.aspects;


import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.After;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import static football.constants.ConstProfiles.DEV;

@Component
@Aspect
@Profile(DEV)

public class printMethodsAspect {

    @Before("execution(* services.validators.*.*(..))")
    public void beforeValidationMethods(JoinPoint jp) {
        System.out.println("Performing  "+jp.getTarget().getClass().getSimpleName() + " validation...");
    }

    @Before("execution(* services.*.*(..))")
    public void beforeAnyMethods(JoinPoint jp) {
        System.out.println("Performing  "+jp.getTarget().getClass().getSimpleName() + " validation...");
    }

    @After("execution(* services.validators.*.*(..))")
    public void afterValidationMethods(JoinPoint jp) {
        System.out.println("Validation  "+jp.getTarget().getClass().getSimpleName() + " has finished.");
    }

    @Before("execution(* services.enrichers.*.*(..))")
    public void beforeEnrichmentMethods(JoinPoint jp) {
        System.out.println("Performing  "+jp.getTarget().getClass().getSimpleName() + " enrichment...");
    }

    @After("execution(* services.enrichers.*.*(..))")
    public void afterEnrichmentMethods(JoinPoint jp) {
        System.out.println("Enrichment  "+jp.getTarget().getClass().getSimpleName() + " was successfully done.");
    }
}
