Project hank
========

Hank is an open-source system for running distributed, highly-scalable, low-latency, batch-writable-only, key-value datastores. You can find a much more detailed description of the project [here](https://docs.google.com/document/d/1tam5b83GE2NnDti5o7giU-vBb-fpqu0ZJWgokrOSJo0/edit).

Some tips helpful when configuring Curly and Cueball domains can be found [here](https://docs.google.com/spreadsheet/ccc?key=0AvnnKDkRGJGodHM5TVk5eXdHMFIzcEJ4cDJWZTJadEE).

Download
====
You can either build Hank from source as described below, or pull the latest jar from the Liveramp Maven repository:

```xml
<repository>
  <id>repository.liveramp.com</id>
  <name>liveramp-repositories</name>
  <url>http://repository.liveramp.com/artifactory/liveramp-repositories</url>
</repository>
```

The 1.0-SNAPSHOT build can be retrieved there:

```xml
<dependency>
    <groupId>com.liveramp</groupId>
    <artifactId>hank</artifactId>
    <version>1.0-SNAPSHOT</version>
</dependency>
```

Building
====

To build hank from source and generate the jar in target/:

```bash
> mvn package
```

To run the test suite locally:

```bash
> mvn test
```

![architecture diagram](docs/hank-architecture.png)
![caching diagram](docs/hank-caching.png
![fixed length values diagram](docs/hank-fixed-length-values.png)
![variable length values diagram](docs/hank-variable-length-values.png)

![ring groups screenshot](docs/hank-screenshot-ring-groups.jpg)
![ring group screenshot](docs/hank-screenshot-ring-group-1.jpg)
![ring group screenshot](docs/hank-screenshot-ring-group-2.jpg)
![ring screenshot](docs/hank-screenshot-ring.jpg)
![partitions screenshot](docs/hank-screenshot-partitions.jpg)
![admin screenshot](docs/hank-screenshot-admin.jpg)
![domain group screenshot](docs/hank-screenshot-domain-group.jpg)
