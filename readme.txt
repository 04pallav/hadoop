TO run the jars

For Question 1
**Type the following commands in the console


hdfs namenode -format

start-dfs.sh


**if running the jar second time, the commands below will delete the existing input and output folders
hdfs dfs -rm -r -f /input

hdfs dfs -rm -r -f /output

** make a new directory for input 

hdfs dfs -mkdir /input


** Copy the input file to hdfs, the first argument should be the location of the input file on the unix/linux file system, the second shoud be the destination on HDFS 
hdfs dfs -put /home/user/q1input/users.dat /input


**run the jar, first argument is input file in hdfs, second output folder in hdfs, third zipcode that you want to search users for

hadoop jar searchzipcode.jar searchzipcode /input/users.dat /output 80302

** list all the files generated in the output folder


hdfs dfs -ls /output


** View the contents of the output file
hdfs dfs -cat /output/part-r-00000

For Question 2

**Type the following commands in the console


hdfs namenode -format

start-dfs.sh


**if running the jar second time, the commands below will delete the existing input and output folders
hdfs dfs -rm -r -f /input

hdfs dfs -rm -r -f /output1
hdfs dfs -rm -r -f /output2


** make a new directory for input 

hdfs dfs -mkdir /input


** Copy the input file to hdfs, the first argument should be the location of the input file on the unix/linux file system, the second shoud be the destination on HDFS 
hdfs dfs -put /home/user/q2input/ratings.dat /input


**run the jar, first argument is input file in hdfs, second intermediate output1 folder in hdfs, third final output folder i.e. folder 2

hadoop jar descmovierating.jar DescMovieRating /input/ratings.dat /output1 /output2 



**** list all the files generated in the output folder


hdfs dfs -ls /output2

** View the contents of the output file

hdfs dfs -cat /output2/part-r-00000

