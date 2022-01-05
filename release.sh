mvn -B clean install -Dtag=2.0.3 -Dmaven.test.skip=true release:prepare release:perform -P release


# Rollback
# mvn release:rollback
# git tag -d 2.0.2
# git push --delete origin 2.0.2