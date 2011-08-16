for PID in `jps -l | grep com.rapleaf.hank.part_daemon.PartDaemonServer | cut -d ' ' -f 1`; do
  echo "Killing $PID"
  kill $PID
done