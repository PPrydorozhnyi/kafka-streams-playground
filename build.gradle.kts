import com.diffplug.spotless.LineEnding

plugins {
    java
    id("org.springframework.boot").apply(false) version "3.2.0"
    id("io.spring.dependency-management").apply(false) version "1.1.4"
    id("com.diffplug.spotless") version "6.23.3"
}

group = "com.petro"
version = "0.0.2-SNAPSHOT"

subprojects {
    apply(plugin = "java")
    apply(plugin = "org.springframework.boot")
    apply(plugin = "io.spring.dependency-management")
    apply(plugin = "com.diffplug.spotless")

    java {
        sourceCompatibility = JavaVersion.VERSION_21
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

    spotless {
        lineEndings = LineEnding.PLATFORM_NATIVE
        java {
            target("src/*/java/**/*.java")

            palantirJavaFormat()
            removeUnusedImports()
            importOrder()
            formatAnnotations()
            indentWithSpaces()
            trimTrailingWhitespace()
        }
        kotlin {
            target("src/*/kotlin/**/*.kt")

            //more rules https://pinterest.github.io/ktlint/0.49.1/rules/standard/
            ktlint()
                    .setEditorConfigPath("${project.rootDir}/spotless/.editorconfig")
        }
        kotlinGradle {
            target("*.gradle.kts")
            ktlint()
                    .setEditorConfigPath("${project.rootDir}/spotless/.editorconfig")
        }
    }

    tasks.withType<Test> {
        useJUnitPlatform()
    }
}
