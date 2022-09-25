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
    implementation("org.jetbrains.kotlin:kotlin-reflect:${kotlinVersion}")
    implementation("org.jetbrains.kotlin:kotlin-stdlib-jdk8:${kotlinVersion}")
    runtimeOnly("io.r2dbc:r2dbc-h2")
    implementation("io.r2dbc:r2dbc-pool")
    runtimeOnly("ch.qos.logback:logback-classic")
    testImplementation("io.micronaut.test:micronaut-test-rest-assured")
    compileOnly("org.graalvm.nativeimage:svm")

    implementation("io.projectreactor.kafka:reactor-kafka:1.3.12")
    implementation("org.apache.kafka:kafka-clients:3.2.3")

    implementation("org.apache.avro:avro:1.11.1")
    implementation("io.confluent:kafka-avro-serializer:7.2.1")

    implementation("io.micronaut:micronaut-validation")

    runtimeOnly("com.fasterxml.jackson.module:jackson-module-kotlin")

    testImplementation("net.datafaker:datafaker:1.5.0")
    testImplementation("org.assertj:assertj-core:3.23.1")
    testImplementation("org.testcontainers:kafka:1.17.3")
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
    testRuntime("junit")
    processing {
        incremental(true)
        annotations("io.vonsowic.*")
    }
}



