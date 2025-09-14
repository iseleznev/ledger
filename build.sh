# Сборка по порядку - от domain к presentation
mvn clean install -pl shared
mvn clean install -pl domain
mvn clean install -pl application
mvn clean install -pl infrastructure
mvn clean install -pl presentation

# Или полная сборка всего проекта:
mvn clean install -DskipTests

# Для сборки только gRPC с генерацией protobuf:
mvn clean compile -pl presentation/grpc