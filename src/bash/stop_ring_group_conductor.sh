for PID in `jps -l | grep com.rapleaf.hank.ring_group_conductor.RingGroupConductor | cut -d ' ' -f 1`; do
  echo "Killing $PID"
  kill -9 $PID
done