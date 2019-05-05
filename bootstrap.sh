echo bootstrap.sh called ...
echo export VERSION=0.1 > version.properties;
source ./version.properties;
export RULE_ENGINE_PACKAGE_NAME=rule-engine-bundled-${VERSION}.jar
echo "now deploying Rule Engine App"
jobmanager.sh start
/app/wait-for-it.sh localhost:8081 -t 300 -- echo "flink jobmanager started. Now starting application."
/app/local-deploy.sh 
CONFIG=$(/app/local-deploy.sh 2>/dev/null)
echo "Configuration: $CONFIG"
#sed -i 's/taskmanager.numberOfTaskSlots: [0-9]*/taskmanager.numberOfTaskSlots: 17/g' /opt/flink/conf/flink-conf.yaml
taskmanager.sh start
sleep 5
flink run -c org.oisp.RuleEngineBuild target/rule-engine-bundled-0.1.jar --runner=FlinkRunner --streaming=true --JSONConfig="$CONFIG"
echo "started jobmanager and one taskmanager -- but should not reach this point"

