There are 3 datafiles :: movies.dat, ratings.dat, users.dat

Please read the “README_Important” file to know about the data organization and to know about the Attribute of the data. All are very well explained in that README_Important file. In class there will be brief demo/ discussion about that. Please read the questions carefully and use only the data file that you need. Some question may need only users.dat, or some question may need only ratings.dat


After being familiar with the data - you are required to write efficient Hadoop Map-Reduce programs in Java to find the following information ::


Q1. Given a input zipcode, find all the user-ids that belongs to that zipcode. You must take the input zipcode in command line.

[For example, if the input zipcode is 75252 then you need to find all users who lives in 75252]

[You only need users.dat file to get the answer.]



Q2. Find top 10 average rated movies with descending order of rating (Use of chaining of multiple map-reduce job is a must here )

[Clue : From the dataset we know that, each user can rate mulitple movies and each movie can be rated by multiple users and this information is found in ratings.dat file . So, first we have to find the average rating of a movie and second, we need to find the top 10 average rated movies.
