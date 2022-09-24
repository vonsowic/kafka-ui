plugins {
    id("org.jetbrains.kotlin.jvm") version "1.6.21"
    id("org.jetbrains.kotlin.kapt") version "1.6.21"
    id("org.jetbrains.kotlin.plugin.allopen") version "1.6.21"
    id("com.github.johnrengelman.shadow") version "7.1.2"
    id("io.micronaut.application") version "3.6.0"
}

version = "0.1"
group = "io.vonsowic"

val kotlinVersion=project.properties.get("kotlinVersion")
repositories {
    mavenCentral()
    maven("https://packages.confluent.io/maven")
}

dependencies {
    kapt("io.micronaut:micronaut-http-validation")
    implementation("io.micronaut:micronaut-http-client")
    implementation("io.micronaut:micronaut-jackson-databind")
    implementation("io.micronaut.kotlin:micronaut-kotlin-runtime")
    implementation("io.micronaut.r2dbc:micronaut-r2dbc-core")
    implementation("io.micronaut.reactor:micronaut-reactor")
    implementation("io.micronaut.reactor:micronaut-reactor-http-client")
    implementation("jakarta.annotation:jakarta.annotation-api")
    implementation("org.apache.logging.log4j:log4j-core:2.19.0")
    implementation("org.jetbrains.kotlin:kotlin-reflect:${kotlinVersion}")
    implementation("org.jetbrains.kotlin:kotlin-stdlib-jdk8:${kotlinVersion}")
    runtimeOnly("io.r2dbc:r2dbc-h2")
    runtimeOnly("org.apache.logging.log4j:log4j-api:2.19.0")
    runtimeOnly("org.apache.logging.log4j:log4j-slf4j-impl:2.17.2")
    testImplementation("io.micronaut.test:micronaut-test-rest-assured")
    compileOnly("org.graalvm.nativeimage:svm")

    implementation("io.projectreactor.kafka:reactor-kafka:1.3.12")

    implementation("org.apache.avro:avro:1.11.1")
    implementation("io.confluent:kafka-avro-serializer:7.2.1")

    implementation("io.micronaut:micronaut-validation")

    runtimeOnly("com.fasterxml.jackson.module:jackson-module-kotlin")

}


application {
    mainClass.set("io.vonsowic.ApplicationKt")
}
java {
    sourceCompatibility = JavaVersion.toVersion("11")
}

tasks {
    compileKotlin {
        kotlinOptions {
            jvmTarget = "11"
        }
    }
    compileTestKotlin {
        kotlinOptions {
            jvmTarget = "11"
        }
    }
}
graalvmNative.toolchainDetection.set(false)
micronaut {
    runtime("netty")
    testRuntime("kotest")
    processing {
        incremental(true)
        annotations("io.vonsowic.*")
    }
}



