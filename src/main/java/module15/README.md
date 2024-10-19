- Start Flink local cluster
- Run from terminal: nc -l 9999, enter words starting by N and others
- Run from another terminal 
- `flink run -c module15.WordCountDataStream target/reduce.jar
  `

Open Flink UI 
You should see the job running
In TaskManager, Stdout tab you can see the words starting by N

