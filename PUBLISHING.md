# Publishing Maestro to Maven Central

## Publishing Snapshots

Snapshots are automatically published when code is pushed to the `main` branch.

Nebula automatically determines version from git commits (e.g., `1.0.0-dev.5+a1b2c3d`).

## Publishing Releases

### Via Git Tag (Recommended)

1. Create and push a git tag:
   ```bash
   git tag -a v1.0.0 -m "Release 1.0.0"
   git push origin v1.0.0
   ```
2. GitHub Actions will automatically:
   - Detect the tag
   - Run `./gradlew final` to create release version
   - Build, sign, and publish to Sonatype staging
3. Manual step: Log in to https://s01.oss.sonatype.org/ and release

**Note:** Nebula automatically determines the version from the git tag. No need to manually update version in gradle.properties.

### Post-Release Steps

1. Log in to https://s01.oss.sonatype.org/
2. Find the staging repository (com.netflix.maestro-XXXX)
3. Click "Close" to validate artifacts
4. Click "Release" to publish to Maven Central
5. Artifacts available in ~15-30 minutes

## Using Published Artifacts

### In Netflix Internal Maestro (Gradle)
```gradle
dependencies {
    implementation 'com.netflix.maestro:maestro-flow:1.0.0'
    implementation 'com.netflix.maestro:maestro-engine:1.0.0'
}
```

### Maven
```xml
<dependency>
    <groupId>com.netflix.maestro</groupId>
    <artifactId>maestro-flow</artifactId>
    <version>1.0.0</version>
</dependency>
```

## Published Modules

All 12 library modules are published (maestro-server is excluded as it's an application):
- netflix-sel
- maestro-common
- maestro-dsl
- maestro-engine
- maestro-database
- maestro-flow
- maestro-queue
- maestro-timetrigger
- maestro-signal
- maestro-aws
- maestro-kubernetes
- maestro-http

## Prerequisites

Before you can publish releases, you need to:

1. **Create Sonatype OSSRH Account:**
   - Register at https://issues.sonatype.org/
   - Create JIRA ticket to claim `com.netflix.maestro` group ID
   - Wait for approval (1-2 business days)

2. **Generate GPG Key:**
   ```bash
   # Generate key pair
   gpg --gen-key

   # Export public key to key servers
   gpg --keyserver keyserver.ubuntu.com --send-keys <KEY_ID>
   gpg --keyserver keys.openpgp.org --send-keys <KEY_ID>

   # Export secret key for CI/CD (base64 encoded)
   gpg --export-secret-keys <KEY_ID> | base64
   ```

3. **Configure GitHub Secrets:**
   Add these secrets to the repository:
   - `OSSRH_USERNAME` - Sonatype JIRA username
   - `OSSRH_PASSWORD` - Sonatype JIRA password/token
   - `SIGNING_KEY` - Base64 encoded GPG secret key
   - `SIGNING_PASSWORD` - GPG key passphrase

## Local Testing

```bash
# Build all modules
./gradlew build

# Test local publishing (without credentials)
./gradlew publishToMavenLocal

# Verify artifacts in ~/.m2/repository/com/netflix/maestro/

# Check what version Nebula will generate
./gradlew properties | grep version
```
