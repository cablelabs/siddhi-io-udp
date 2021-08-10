Siddhi IO UDP
======================================

The **siddhi-io-udp extension** is an extension to <a target="_blank" href="https://siddhi.io">Siddhi</a> that receives
UDP Packets and passes the packet payload to a mapper to extract the data.

For information on <a target="_blank" href="https://siddhi.io/">Siddhi</a> and it's features refer <a target="_blank" href="https://siddhi.io/redirect/docs.html">Siddhi Documentation</a>. 

## Obtain bundle via Maven

Other Siddhi I/O extensions can include this project into their build by adding the following repository and dependency
to the component's pom.xml file.

```xml
    <repositories>
        ...
        <repository>
            <id>jitpack.io</id>
            <url>https://jitpack.io</url>
        </repository>
        ...
    </repositories>
```

```xml
    <dependencies>
        ...
        <dependency>
            <groupId>com.github.cablelabs</groupId>
            <artifactId>siddhi-io-udp</artifactId>
            <version>master-SNAPSHOT</version>
        </dependency>
        ...
    </dependencies>
```

## Latest API Docs

## Features

* udp (Source) - A Siddhi application can be configured to receive events vial the UDP transport by adding the
@Sink(type = 'udp') annotation at the top of an event stream definition
    
## Dependencies 

This I/O extension extends its functionality from the siddhi-io-tcp as it is basically doing the same thing except with
UDP packets rather than TCP.
```xml
    <dependencies>
        ...
        <dependency>
            <groupId>io.siddhi.extension.io.tcp</groupId>
            <artifactId>siddhi-io-tcp</artifactId>
            <version>3.0.5</version>
        </dependency>
        ...
    </dependencies>
```
   
## Installation
   
For installing this extension on various Siddhi execution environments refer Siddhi documentation section on <a target="_blank" href="https://siddhi.io/redirect/add-extensions.html">adding extensions</a>.
   
## Support and Contribution
   
* We encourage users to ask questions and get support via <a target="_blank" href="https://stackoverflow.com/questions/tagged/siddhi">StackOverflow</a>, make sure to add the `siddhi` tag to the issue for better response.

* If you find any issues related to the extension please report them on <a target="_blank" href="https://github.com/siddhi-io/siddhi-io-udp/issues">the issue tracker</a>.

* For production support and other contribution related information refer <a target="_blank" href="https://siddhi.io/community/">Siddhi Community</a> documentation.