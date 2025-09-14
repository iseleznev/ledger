## 1. Перейдите в корневой каталог проекта
#cd /path/to/your/ledger/project

# 2. Установите родительские модули (domain, shared и т.д.)
mvn clean install -pl shared,domain -am

# 3. Установите application модули
mvn clean install -pl application -am

# 4. Сгенерируйте protobuf классы для gRPC модуля
mvn clean compile -pl presentation/grpc

# 5. Если protobuf генерация прошла успешно, соберите весь проект
mvn clean install