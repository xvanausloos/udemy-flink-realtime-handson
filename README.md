# Oct 24 LDI Udemy Flink Training

Resources:
https://www.udemy.com/course/apache-flink-a-real-time-hands-on-course-on-flink

Run:
- start local Flink cluster (see doc) 
- package JAR: `mvn clean package`
- it creates a JAR in `target` folder
- Module 12 : 
```flink run -c module12.JoinExample target/reduce.jar --input1  /Users/xaviervanausloos/temp/person --input2  /Users/xaviervanausloos/temp/location --output /Users/xaviervanausloos/temp/module12output.csv```
  