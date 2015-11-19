to run

```
mvn package
mvn dependency:copy-dependencies -DoutputDirectory=libs/
java -cp target/fenzo-layerx-brain-1.0-SNAPSHOT.jar:libs/* com.layerx.stupidbrain.Brain
```
