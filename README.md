### Project Presentation Video available in the below link

 https://drive.google.com/file/d/1Sc8vuF2NV9ItzR_hJ9H_E3W6Iam5gQ1I/view?usp=sharing
  
### Requirements

 #### Setting Up PySpark
    sudo apt install openjdk-11-jdk 
 Download latest spark hadoop from http://spark.apache.org/downloads.html and exract it 
 
    $ tar -xzf spark-3.0.0-bin-hadoop2.7.tgz
    $ pip3 install pyspark
    $ sudo gedit .bashrc
write folowing and save it 

    export SPARK_HOME=<"path to extracted spark">
    export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.8.1-src.zip:$PYTHONPATH
    export PATH=$SPARK_HOME/bin:$SPARK_HOME/python:$PATH
    export PYSPARK_PYTHON=python3
    export PYSPARK_DRIVER_PYTHON=python3
now open terminal and type

    pyspark 
it will run.


 #### Setting Up Kafka
    Go to official Appache kafka and install kafka in your pc.
   
 ### Setting Up Elasticsearch and reactivesearch
 
    wget https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-7.7.0-linux-x86_64.tar.gz (gettting elasticsearch )
    tar -xzf elasticsearch-7.7.0-linux-x86_64.tar.gz 
    sudo apt install npm 
    npm install @appbseio/reactivesearch 
    npm install -g create-react-app 
    create-react-app  video-streaming
    
### spark_dependencies

Download following dependencies

    spark-sql-kafka-0-10_2.12-3.0.0-preview.jar
    kafka-clients-2.5.0.jar
    spark-streaming-kafka-0-10-assembly_2.12-3.0.0-preview2.jar
    commons-pool2-2.8.0.jar,elasticsearch-hadoop-7.7.0.jar
    (make sure all dependecies are consistent with the versions of spark and kafka)

### cloning yolov5

    !git clone https://github.com/ultralytics/yolov5 
    !pip install -qr yolov5/requirements.txt  
Check the yolo installation by this command

    cd yolov5
    !python detect.py --source '0' --output './results.avi'
    
    
###### changes to be made in Elasticsearch configuration file 
    • Go to elasticsearch directory (where it was extracted) → config → elasticsearch.yml
    Things to specify in the config file
    • <cluster.name: my_cluster_name>
    • <node.name: node_name>
    • For Master node: <node.master: true>
    • For Data nodes: <node.data: true>
    • <network.host: host_ip_address>
    • <http.port: 9200>
    
## Running
To start kafka server
Inside kafka directory and type 
 
    bin/zookeeper-server-start.sh config/zookeeper.properties
    bin/kafka-server-start.sh config/server.properties
    
runnung elasticsearch and reactivesearch UI

    cd elasticsearch_directory
    bin/elasticsearch 
    cd  name_of_your_app 
    npm start
Run producer.py to publish camera feeds then run kafka_spark.py to consume feeds from kafka, run yolov5 and send results to elasticsearch .(Make sure all spark dependencies are on the same directory where producer.py and kafka_spark.py exists. )

Finally UI will list all the results
