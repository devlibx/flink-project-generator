# How to build flink project

This sample command is creating a project with package "com.myapp" and in folder "my-app". Change it as required 
```shell
rm -rf my-app 
mvn -DinteractiveMode=false archetype:generate -DarchetypeGroupId=io.github.devlibx.tools.java.maven \
        -DarchetypeArtifactId=flink-project-generator -DarchetypeVersion=0.0.52 -DgroupId=com.myapp \
        -DartifactId=my-app

OR
        
mvn -DinteractiveMode=false archetype:generate -DarchetypeGroupId=io.github.devlibx.tools.java.maven \
        -DarchetypeArtifactId=flink-project-generator -DarchetypeVersion=0.0.52-SNAPSHOT -DgroupId=com.myapp \
        -DartifactId=my-app                        
```

#### Running code
This will include flink jar in the fat jar.
```shell
mvn clean install -P idea
java -cp target/my-app-1.0-SNAPSHOT.jar:.  com.myapp.Job
```

##### Production build
This will include flink jar in the fat jar
```shell
mvn clean install
# Launch with Flink
```
git p
### How to build form source

1. Checkout the project
Checkout project "git@github.com:harishbohara/flink-examples.git"
   
2. Run command 
```shell
sh copy.sh
mvn clean deploy 
```