# Spark Java sample application
This application aggregates all late responded calls by department or agency

### Build
 - Build fat jar
   ```
   gradlew clean shadowJar
   ```
- Build without dependencies
   ```
   gradlew clean build
   ```
- Collect dependencies
   ```
   gradlew copyDependencies
   ```
