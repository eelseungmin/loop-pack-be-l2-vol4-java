rootProject.name = "loopers-java-spring-template"

include(
    ":apps:commerce-api",
    ":apps:commerce-streamer",
    ":apps:commerce-batch",
    ":apps:pg-simulator",
    ":tests:commerce-e2e",
    ":modules:jpa",
    ":modules:redis",
    ":modules:kafka",
    ":modules:event-contract",
    ":modules:ranking-contract",
    ":supports:jackson",
    ":supports:logging",
    ":supports:monitoring",
)

// configurations
pluginManagement {
    val springBootVersion: String by settings
    val springDependencyManagementVersion: String by settings

    repositories {
        maven { url = uri("https://repo.spring.io/milestone") }
        maven { url = uri("https://repo.spring.io/snapshot") }
        gradlePluginPortal()
    }

    resolutionStrategy {
        eachPlugin {
            when (requested.id.id) {
                "org.springframework.boot" -> useVersion(springBootVersion)
                "io.spring.dependency-management" -> useVersion(springDependencyManagementVersion)
            }
        }
    }
}
