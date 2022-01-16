rm -rf src/main/resources
cp -rf ../flink-examples/target/generated-sources/archetype/src/main/resources src/main/
rm -rf ./src/main/resources/archetype-resources/.idea
rm -rf ./src/main/resources/archetype-resources/dependency-reduced-pom.xml
rm -rf ./src/main/resources/archetype-resources/*.iml
rm -rf ./src/main/resources/archetype-resources/*.ipr
rm -rf ./src/main/resources/archetype-resources/*.iws