plugins {
    java
}

dependencies {
    testImplementation("com.redis:testcontainers-redis")
    testImplementation("org.springframework.kafka:spring-kafka")
    testImplementation("org.testcontainers:kafka")
    testImplementation("org.testcontainers:mysql")
}

tasks.test {
    dependsOn(":apps:commerce-api:bootJar", ":apps:commerce-streamer:bootJar")
    systemProperty("e2e.root.dir", rootProject.projectDir.absolutePath)
}
