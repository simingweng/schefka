package com.siming.schefka.annotation;

import org.springframework.context.annotation.Import;

import java.lang.annotation.*;

@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Import(SchefkaConfiguration.class)
public @interface EnableSchefka {
}
