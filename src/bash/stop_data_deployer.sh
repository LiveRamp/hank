for PID in `jps -l | grep com.rapleaf.hank.data_deployer.DataDeployer | cut -d ' ' -f 1`; do
  echo "Killing $PID"
  kill $PID
done