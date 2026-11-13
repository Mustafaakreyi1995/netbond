plugins {
    id("com.android.application")
    id("org.jetbrains.kotlin.android")
}

android {
    namespace = "app.netbond"
    compileSdk = 34

    defaultConfig {
        applicationId = "app.netbond"
        minSdk = 26
        targetSdk = 34
        versionCode = (System.getenv("VERSION_CODE") ?: "1").toInt()
        versionName = System.getenv("VERSION_NAME") ?: "1.0"
    }

    signingConfigs {
        create("shared") {
            // A fixed key kept in the repo so every build can update the
            // previous one in place. Fine for a personal sideloaded app.
            storeFile = file("../netbond.keystore")
            storePassword = "netbond"
            keyAlias = "netbond"
            keyPassword = "netbond"
        }
    }

    buildTypes {
        release {
            isMinifyEnabled = false
            signingConfig = signingConfigs.getByName("shared")
        }
        debug {
            signingConfig = signingConfigs.getByName("shared")
        }
    }

    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_17
        targetCompatibility = JavaVersion.VERSION_17
    }
    kotlinOptions {
        jvmTarget = "17"
    }
    lint {
        checkReleaseBuilds = false
        abortOnError = false
    }
}

dependencies {
    // Built by `gomobile bind` from ../core (see .github/workflows/build.yml)
    implementation(files("libs/bondcore.aar"))
}
