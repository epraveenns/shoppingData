FROM       openjdk:8-jre-alpine

WORKDIR    /app
COPY       MainClass.class /app/
COPY	   7210_1.csv /app/
COPY	   jars /app/jars
CMD	   java -cp ".:./jars/*" MainClass
