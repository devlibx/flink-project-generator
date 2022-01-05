mvn -B clean install -Dtag=2.0.3 -Dmaven.test.skip=true release:prepare release:perform -P release
