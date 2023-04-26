/opt/spark/bin/spark-submit \
    --master local[*] \
    --deploy-mode client \
    --class "com.cognira.processing.Main" \
    --driver-java-options "-Dlog4j.configuration=file:/app/src/main/resources/log4j.properties" \
    target/scala-2.12/processing_2.12-0.1.jar

