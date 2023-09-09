plugins {
    java
    id("org.springframework.boot").apply(false) version "3.1.2"
    id("io.spring.dependency-management").apply(false) version "1.1.2"
}

group = "com.petro"
version = "0.0.1-SNAPSHOT"

subprojects {
    apply(plugin = "java")
    apply(plugin = "org.springframework.boot")
    apply(plugin = "io.spring.dependency-management")

    java {
        sourceCompatibility = JavaVersion.VERSION_17
    }

    configurations {
        compileOnly {
            extendsFrom(configurations.annotationProcessor.get())
        }
    }

    repositories {
        mavenCentral()
    }

    dependencies {
        implementation("org.springframework.boot:spring-boot-starter-web")
        implementation("org.apache.kafka:kafka-streams")
        implementation("org.springframework.kafka:spring-kafka")

        compileOnly("org.projectlombok:lombok")
        annotationProcessor("org.projectlombok:lombok")

        testImplementation("org.springframework.boot:spring-boot-starter-test")
        testImplementation("org.springframework.kafka:spring-kafka-test")
    }

    tasks.withType<Test> {
        useJUnitPlatform()
    }
}
